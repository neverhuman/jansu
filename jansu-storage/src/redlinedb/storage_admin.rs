// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

fn is_known_topic_config(
    known_configs: &BTreeMap<String, DescribeConfigsResourceResult>,
    name: &str,
) -> bool {
    known_configs.contains_key(name)
        || matches!(
            name,
            "jansu.virtual" | "jansu.schema.validation" | "jansu.lake.sink"
        )
}

impl Delegate {
    pub(super) async fn delegate_register_broker(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        let start = SystemTime::now();

        debug!(?broker_registration);

        let mut pc = self.connection().await?;

        let s = sql("register_broker.sql").map_err(Error::from)?;
        let _ = pc
            .execute(&s, (broker_registration.cluster_id.as_str(),))
            .map_err(Error::from)?;

        Ok(()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "register_broker")],
            )
        })
    }

    pub(super) async fn delegate_brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster);

        let broker_id = self.node;
        let host = match self.advertised_listener.host_str() {
            Some(host) => host.into(),
            None => {
                tracing::warn!(listener = %self.advertised_listener, "missing broker host; using wildcard");
                "0.0.0.0".into()
            }
        };
        let port = match self.advertised_listener.port() {
            Some(port) => port.into(),
            None => {
                tracing::warn!(listener = %self.advertised_listener, "missing broker port; using kafka default");
                9092
            }
        };
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "brokers")],
            )
        })
    }

    pub(super) async fn delegate_create_topic(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut pc = self.connection().await?;
        pc.begin(BeginMode::Immediate).map_err(Error::from)?;

        if let Some(configs) = topic.configs.as_ref() {
            let known_configs = crate::service::topic_config_defaults::build_default_configs();
            if configs
                .iter()
                .any(|config| !is_known_topic_config(&known_configs, config.name.as_str()))
            {
                return Err(Error::Api(ErrorCode::InvalidRequest));
            }
        }

        let uuid = {
            let uuid = Uuid::new_v4();

            let parameters = (
                self.cluster.as_str(),
                topic.name.as_str(),
                uuid.to_string(),
                topic.num_partitions,
                (topic.replication_factor as i32),
            );

            let s = sql("topic_insert.sql").map_err(Error::from)?;
            pc.execute(&s, parameters.clone())
                .inspect_err(|err| {
                    if is_unique_constraint(err) {
                        debug!(?err);
                    } else {
                        error!(?err)
                    }
                })
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
                .inspect(|_| debug!(?parameters))
                .map(|_| uuid)
        }
        .inspect(|uuid| debug!(?uuid))
        .inspect_err(|err| error!(?err))?;

        for partition in 0..topic.num_partitions {
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            let s = sql("topition_insert.sql").map_err(Error::from)?;
            let _ = pc
                .execute(&s, params)
                .inspect(|topition| debug!(?topition))
                .map_err(Error::from)?;

            let s = sql("watermark_insert.sql").map_err(Error::from)?;
            let _ = pc
                .execute(&s, params)
                .inspect(|watermark| debug!(?watermark))
                .map_err(Error::from)?;
        }

        if let Some(configs) = topic.configs.as_ref() {
            for config in configs {
                debug!(?config);

                let params = (
                    self.cluster.as_str(),
                    topic.name.as_str(),
                    config.name.as_str(),
                    config.value.as_deref(),
                );

                let s = sql("topic_configuration_upsert.sql").map_err(Error::from)?;
                let _ = pc
                    .execute(&s, params)
                    .inspect_err(|err| error!(?err, ?config))
                    .inspect(|id| debug!(?id, ?config))
                    .map_err(Error::from)?;
            }
        }

        let _ = pc.commit().map_err(Error::from)?;

        for partition in 0..topic.num_partitions {
            let s = sql("leader_epoch_history_insert.sql").map_err(Error::from)?;
            let _ = pc
                .execute(
                    &s,
                    (self.cluster.as_str(), topic.name.as_str(), partition, 0, 0),
                )
                .inspect_err(|err| error!(?err, ?topic, ?partition))
                .map_err(Error::from)?;
        }

        Ok(uuid).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "create_topic")],
            )
        })
    }

    pub(super) async fn delegate_delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let start = SystemTime::now();
        debug!(cluster = self.cluster, ?topics);

        let mut pc = self.connection().await?;
        pc.begin(BeginMode::Immediate).map_err(Error::from)?;

        let mut results = vec![];

        for topic in topics {
            let mut partitions = vec![];

            let partitions_ref = match topic.partitions.as_ref() {
                Some(partitions) => partitions.as_slice(),
                None => &[],
            };

            for partition in partitions_ref {
                let partition_index = partition.partition_index;
                let offset = partition.offset;

                let topic_name = topic.name.as_str();

                let topition_id = {
                    let s = sql("topition_select_id.sql").map_err(Error::from)?;
                    let mut rows = pc
                        .query(&s, (self.cluster.as_str(), topic_name, partition_index))
                        .map_err(Error::from)?;
                    match rows.step().map_err(Error::from)? {
                        Step::Row(row) => Some(row.get::<i64>(0)?),
                        Step::Done => None,
                    }
                };

                let topition_id = match topition_id {
                    Some(id) => id,
                    None => {
                        partitions.push(
                            DeleteRecordsPartitionResult::default()
                                .partition_index(partition_index)
                                .low_watermark(-1)
                                .error_code(i16::from(ErrorCode::UnknownTopicOrPartition)),
                        );
                        continue;
                    }
                };

                let s = sql("record_delete_by_offset.sql").map_err(Error::from)?;
                let _ = pc
                    .execute(
                        &s,
                        (self.cluster.as_str(), topic_name, partition_index, offset),
                    )
                    .inspect_err(|err| error!(?err))
                    .map_err(Error::from)?;

                let s = sql("redlinedb/watermark_update_low_by_topition_id.sql")
                    .map_err(Error::from)?;
                let _ = pc
                    .execute(&s, (topition_id, offset))
                    .inspect_err(|err| error!(?err))
                    .map_err(Error::from)?;

                // Read back the updated low watermark (or the current one if $offset was lower)
                let low_watermark = {
                    let s = sql("watermark_select_no_update.sql").map_err(Error::from)?;
                    let mut rows = pc
                        .query(&s, (self.cluster.as_str(), topic_name, partition_index))
                        .inspect_err(|err| error!(?err))
                        .map_err(Error::from)?;
                    match rows.step().map_err(Error::from)? {
                        Step::Row(row) => match row.get::<Option<i64>>(0) {
                            Ok(Some(low_watermark)) => low_watermark,
                            Ok(None) => 0,
                            Err(err) => {
                                error!(?err, "failed to read low watermark");
                                0
                            }
                        },
                        Step::Done => 0,
                    }
                };

                partitions.push(
                    DeleteRecordsPartitionResult::default()
                        .partition_index(partition_index)
                        .low_watermark(low_watermark)
                        .error_code(i16::from(ErrorCode::None)),
                );
            }

            results.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partitions)),
            );
        }

        let _ = pc.commit().map_err(Error::from)?;

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_records")],
            )
        })
    }

    pub(super) async fn delegate_delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let start = SystemTime::now();
        debug!(cluster = self.cluster, ?topic);

        let mut pc = self.connection().await?;
        pc.begin(BeginMode::Immediate).map_err(Error::from)?;

        let (topic_id, topic_name): (i64, String) = {
            let mut rows = match topic {
                TopicId::Id(id) => {
                    let s = sql("redlinedb/topic_select_id_by_uuid.sql").map_err(Error::from)?;
                    pc.query(&s, (self.cluster.as_str(), id.to_string().as_str()))
                        .map_err(Error::from)?
                }
                TopicId::Name(name) => {
                    let s = sql("redlinedb/topic_select_id_by_name.sql").map_err(Error::from)?;
                    pc.query(&s, (self.cluster.as_str(), name.as_str()))
                        .map_err(Error::from)?
                }
            };
            match rows.step().map_err(Error::from)? {
                Step::Row(row) => (
                    row.get::<i64>(0).map_err(Error::from)?,
                    row.get::<String>(1).map_err(Error::from)?,
                ),
                Step::Done => return Ok(ErrorCode::UnknownTopicOrPartition),
            }
        };

        debug!(?topic, topic_id, topic_name);

        let s = sql("redlinedb/topic_delete_id.sql").map_err(Error::from)?;
        let _ = pc.execute(&s, (topic_id,)).map_err(Error::from)?;

        let _ = pc.commit().map_err(Error::from)?;

        Ok(ErrorCode::None).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_topic")],
            )
        })
    }

    pub(super) async fn delegate_incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let start = SystemTime::now();
        debug!(?resource);

        let configs = match resource.configs.as_ref() {
            Some(configs) => configs.clone(),
            None => Vec::new(),
        };

        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Group => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::ClientMetric => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::BrokerLogger => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Broker => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Topic => {
                let mut error_code = ErrorCode::None;
                let known_configs = crate::service::topic_config_defaults::build_default_configs();

                for config in configs {
                    if !is_known_topic_config(&known_configs, config.name.as_str()) {
                        error_code = ErrorCode::InvalidRequest;
                        break;
                    }

                    match OpType::try_from(config.config_operation)? {
                        OpType::Set => {
                            let mut c = self.connection().await?;

                            let s = sql("topic_configuration_upsert.sql").map_err(Error::from)?;
                            if c.execute(
                                &s,
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                    config.value.as_deref(),
                                ),
                            )
                            .map_err(Error::from)
                            .inspect_err(|err| error!(?err))
                            .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Delete => {
                            let mut c = self.connection().await?;

                            let s = sql("topic_configuration_delete.sql").map_err(Error::from)?;
                            if c.execute(
                                &s,
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                ),
                            )
                            .map_err(Error::from)
                            .inspect_err(|err| error!(?err))
                            .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Append => {
                            let mut c = self.connection().await?;

                            let current_value = {
                                let s =
                                    sql("topic_configuration_select.sql").map_err(Error::from)?;
                                let mut rows = c
                                    .query(
                                        &s,
                                        (
                                            self.cluster.as_str(),
                                            resource.resource_name.as_str(),
                                            config.name.as_str(),
                                        ),
                                    )
                                    .map_err(Error::from)?;
                                match rows.step().map_err(Error::from)? {
                                    Step::Row(row) => row
                                        .get::<Value>(0)
                                        .map_err(Error::from)?
                                        .as_text()
                                        .ok()
                                        .map(|s| s.to_string()),
                                    Step::Done => None,
                                }
                            };

                            if let Some(new_val) = &config.value {
                                let mut list: Vec<&str> = match current_value.as_deref() {
                                    Some(s) => s
                                        .split(',')
                                        .map(str::trim)
                                        .filter(|s| !s.is_empty())
                                        .collect(),
                                    None => Vec::new(),
                                };
                                if !list.contains(&new_val.as_str()) {
                                    list.push(new_val.as_str());
                                }
                                let new_str = list.join(",");

                                let s =
                                    sql("topic_configuration_upsert.sql").map_err(Error::from)?;
                                if c.execute(
                                    &s,
                                    (
                                        self.cluster.as_str(),
                                        resource.resource_name.as_str(),
                                        config.name.as_str(),
                                        Some(new_str.as_str()),
                                    ),
                                )
                                .map_err(Error::from)
                                .inspect_err(|err| error!(?err))
                                .is_err()
                                {
                                    error_code = ErrorCode::UnknownServerError;
                                    break;
                                }
                            }
                        }
                        OpType::Subtract => {
                            let mut c = self.connection().await?;

                            let current_value = {
                                let s =
                                    sql("topic_configuration_select.sql").map_err(Error::from)?;
                                let mut rows = c
                                    .query(
                                        &s,
                                        (
                                            self.cluster.as_str(),
                                            resource.resource_name.as_str(),
                                            config.name.as_str(),
                                        ),
                                    )
                                    .map_err(Error::from)?;
                                match rows.step().map_err(Error::from)? {
                                    Step::Row(row) => row
                                        .get::<Value>(0)
                                        .map_err(Error::from)?
                                        .as_text()
                                        .ok()
                                        .map(|s| s.to_string()),
                                    Step::Done => None,
                                }
                            };

                            if let Some(del_val) = &config.value {
                                let list: Vec<&str> = match current_value.as_deref() {
                                    Some(s) => s
                                        .split(',')
                                        .map(str::trim)
                                        .filter(|&s| !s.is_empty() && s != del_val.as_str())
                                        .collect(),
                                    None => Vec::new(),
                                };

                                if list.is_empty() {
                                    let s = sql("topic_configuration_delete.sql")
                                        .map_err(Error::from)?;
                                    if c.execute(
                                        &s,
                                        (
                                            self.cluster.as_str(),
                                            resource.resource_name.as_str(),
                                            config.name.as_str(),
                                        ),
                                    )
                                    .map_err(Error::from)
                                    .inspect_err(|err| error!(?err))
                                    .is_err()
                                    {
                                        error_code = ErrorCode::UnknownServerError;
                                        break;
                                    }
                                } else {
                                    let new_str = list.join(",");
                                    let s = sql("topic_configuration_upsert.sql")
                                        .map_err(Error::from)?;
                                    if c.execute(
                                        &s,
                                        (
                                            self.cluster.as_str(),
                                            resource.resource_name.as_str(),
                                            config.name.as_str(),
                                            Some(new_str.as_str()),
                                        ),
                                    )
                                    .map_err(Error::from)
                                    .inspect_err(|err| error!(?err))
                                    .is_err()
                                    {
                                        error_code = ErrorCode::UnknownServerError;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(error_code.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
                .inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "incremental_alter_resource")],
                    )
                })
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "incremental_alter_resource")],
                )
            }),
        }
    }
}
