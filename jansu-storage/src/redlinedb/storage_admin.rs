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

        let connection = self.connection().await?;

        connection
            .execute(
                "register_broker.sql",
                &[broker_registration.cluster_id.as_str()],
            )
            .await
            .map_err(Into::into)
            .and(Ok(()))
            .inspect(|_| {
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
                9092.into()
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

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

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

            pc.execute("topic_insert.sql", parameters.clone())
                .await
                .inspect_err(|err| {
                    if is_unique_constraint(err) {
                        debug!(?err);
                    } else {
                        error!(?err)
                    }
                })
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
                .inspect(|rows| debug!(?parameters, rows))
                .map(|_| uuid)
        }
        .inspect(|uuid| debug!(?uuid))
        .inspect_err(|err| error!(?err))?;

        for partition in 0..topic.num_partitions {
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            _ = pc
                .execute("topition_insert.sql", params)
                .await
                .inspect(|topition| debug!(?topition))?;

            _ = pc
                .execute("watermark_insert.sql", params)
                .await
                .inspect(|watermark| debug!(?watermark))?;
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

                _ = pc
                    .execute("topic_configuration_upsert.sql", params)
                    .await
                    .inspect_err(|err| error!(?err, ?config))
                    .inspect(|id| debug!(?id, ?config))?;
            }
        }

        pc.commit(tx).await?;

        for partition in 0..topic.num_partitions {
            _ = pc
                .execute(
                    "leader_epoch_history_insert.sql",
                    (self.cluster.as_str(), topic.name.as_str(), partition, 0, 0),
                )
                .await
                .inspect_err(|err| error!(?err, ?topic, ?partition))?;
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

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

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

                let topition_id = if let Some(row) = pc
                    .query_opt(
                        "topition_select_id.sql",
                        (self.cluster.as_str(), topic_name, partition_index),
                    )
                    .await?
                {
                    row.get::<i64>(0)?
                } else {
                    partitions.push(
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition_index)
                            .low_watermark(-1)
                            .error_code(i16::from(ErrorCode::UnknownTopicOrPartition)),
                    );
                    continue;
                };

                _ = pc
                    .execute(
                        "record_delete_by_offset.sql",
                        (self.cluster.as_str(), topic_name, partition_index, offset),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = pc
                    .execute(
                        "redlinedb/watermark_update_low_by_topition_id.sql",
                        (topition_id, offset),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                // Read back the updated low watermark (or the current one if $offset was lower)
                let low_watermark = if let Some(row) = pc
                    .query_opt(
                        "watermark_select_no_update.sql",
                        (self.cluster.as_str(), topic_name, partition_index),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    match row.get::<Option<i64>>(0) {
                        Ok(Some(low_watermark)) => low_watermark,
                        Ok(None) => 0,
                        Err(err) => {
                            error!(?err, "failed to read low watermark");
                            0
                        }
                    }
                } else {
                    0
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

        pc.commit(tx).await?;

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

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let mut rows = match topic {
            TopicId::Id(id) => {
                pc.query(
                    "redlinedb/topic_select_id_by_uuid.sql",
                    (self.cluster.as_str(), id.to_string().as_str()),
                )
                .await?
            }

            TopicId::Name(name) => {
                pc.query(
                    "redlinedb/topic_select_id_by_name.sql",
                    (self.cluster.as_str(), name.as_str()),
                )
                .await?
            }
        };

        let Some(row) = rows.next().await? else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let topic_id = row.get::<i64>(0)?;
        let topic_name = row.get::<String>(1)?;

        let rows = pc
            .execute("redlinedb/topic_delete_id.sql", (topic_id,))
            .await?;

        debug!(?topic, rows, topic_id, topic_name);

        pc.commit(tx).await.and(Ok(ErrorCode::None)).inspect(|_| {
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
                            let c = self.connection().await?;

                            if c.query(
                                "topic_configuration_upsert.sql",
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                    config.value.as_deref(),
                                ),
                            )
                            .await
                            .inspect_err(|err| error!(?err))
                            .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Delete => {
                            let c = self.connection().await?;

                            if c.query(
                                "topic_configuration_delete.sql",
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                ),
                            )
                            .await
                            .inspect_err(|err| error!(?err))
                            .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Append => {
                            let c = self.connection().await?;
                            let mut rows = c
                                .query(
                                    "topic_configuration_select.sql",
                                    (
                                        self.cluster.as_str(),
                                        resource.resource_name.as_str(),
                                        config.name.as_str(),
                                    ),
                                )
                                .await?;

                            let current_value = if let Some(row) = rows.next().await? {
                                row.get_value(0)?.as_text().map(|s| s.to_string())
                            } else {
                                None
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

                                if c.query(
                                    "topic_configuration_upsert.sql",
                                    (
                                        self.cluster.as_str(),
                                        resource.resource_name.as_str(),
                                        config.name.as_str(),
                                        Some(new_str.as_str()),
                                    ),
                                )
                                .await
                                .inspect_err(|err| error!(?err))
                                .is_err()
                                {
                                    error_code = ErrorCode::UnknownServerError;
                                    break;
                                }
                            }
                        }
                        OpType::Subtract => {
                            let c = self.connection().await?;
                            let mut rows = c
                                .query(
                                    "topic_configuration_select.sql",
                                    (
                                        self.cluster.as_str(),
                                        resource.resource_name.as_str(),
                                        config.name.as_str(),
                                    ),
                                )
                                .await?;

                            let current_value = if let Some(row) = rows.next().await? {
                                row.get_value(0)?.as_text().map(|s| s.to_string())
                            } else {
                                None
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
                                    if c.query(
                                        "topic_configuration_delete.sql",
                                        (
                                            self.cluster.as_str(),
                                            resource.resource_name.as_str(),
                                            config.name.as_str(),
                                        ),
                                    )
                                    .await
                                    .inspect_err(|err| error!(?err))
                                    .is_err()
                                    {
                                        error_code = ErrorCode::UnknownServerError;
                                        break;
                                    }
                                } else {
                                    let new_str = list.join(",");
                                    if c.query(
                                        "topic_configuration_upsert.sql",
                                        (
                                            self.cluster.as_str(),
                                            resource.resource_name.as_str(),
                                            config.name.as_str(),
                                            Some(new_str.as_str()),
                                        ),
                                    )
                                    .await
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
