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

impl Delegate {
    pub(super) async fn delegate_register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
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
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
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

    pub(super) async fn delegate_create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topic, validate_only);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let uuid = {
            let uuid = Uuid::new_v4();

            let parameters = (
                self.cluster.as_str(),
                topic.name.as_str(),
                uuid.to_string(),
                topic.num_partitions,
                (topic.replication_factor as i32),
            );

            pc.query_one("topic_insert.sql", parameters.clone())
                .await
                .inspect_err(|err| {
                    if is_unique_constraint(err) {
                        debug!(?err);
                    } else {
                        error!(?err)
                    }
                })
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
                .inspect(|row| debug!(?parameters, ?row))
                .and_then(|row| {
                    row.get::<String>(0)
                        .inspect_err(|err| error!(?err))
                        .map_err(Into::into)
                })
                .and_then(|id| Uuid::parse_str(id.as_str()).map_err(Into::into))
        }
        .inspect(|uuid| debug!(?uuid))
        .inspect_err(|err| error!(?err))?;

        for partition in 0..topic.num_partitions {
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            _ = pc
                .query_opt("topition_insert.sql", params)
                .await
                .map(|row| row.map(|row| row.get_value(0)).transpose())
                .inspect(|topition| debug!(?topition))?;

            _ = pc
                .query_opt("watermark_insert.sql", params)
                .await
                .map(|row| row.map(|row| row.get_value(0)).transpose())
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
                    .query_one("topic_configuration_upsert.sql", params)
                    .await
                    .map(|row| row.get_value(0))
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

            for partition in topic.partitions.as_ref().unwrap_or(&vec![]) {
                let partition_index = partition.partition_index;
                let offset = partition.offset;

                let topic_name = topic.name.as_str();

                // Validate if topition exists
                if pc.query_opt(
                    "topition_select.sql",
                    (self.cluster.as_str(), topic_name, partition_index),
                ).await?.is_none() {
                    partitions.push(
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition_index)
                            .low_watermark(-1)
                            .error_code(i16::from(ErrorCode::UnknownTopicOrPartition)),
                    );
                    continue;
                }

                _ = pc.execute(
                    "record_delete_by_offset.sql",
                    (self.cluster.as_str(), topic_name, partition_index, offset),
                ).await.inspect_err(|err| error!(?err))?;

                _ = pc.execute(
                    "watermark_update_low.sql",
                    (self.cluster.as_str(), topic_name, partition_index, offset),
                ).await.inspect_err(|err| error!(?err))?;

                // Read back the updated low watermark (or the current one if $offset was lower)
                let low_watermark = if let Some(row) = pc.query_opt(
                    "watermark_select_no_update.sql",
                    (self.cluster.as_str(), topic_name, partition_index),
                ).await.inspect_err(|err| error!(?err))? {
                    row.get::<Option<i64>>(0).unwrap_or(Some(0)).unwrap_or(0)
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
                    "topic_select_uuid.sql",
                    (self.cluster.as_str(), id.to_string().as_str()),
                )
                .await?
            }

            TopicId::Name(name) => {
                pc.query(
                    "topic_select_name.sql",
                    (self.cluster.as_str(), name.as_str()),
                )
                .await?
            }
        };

        let Some(row) = rows.next().await? else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let topic_name = row.get_str(1)?;

        for sql in [
            "consumer_offset_delete_by_topic.sql",
            "topic_configuration_delete_by_topic.sql",
            "watermark_delete_by_topic.sql",
            "header_delete_by_topic.sql",
            "record_delete_by_topic.sql",
            "txn_offset_commit_tp_delete_by_topic.sql",
            "txn_produce_offset_delete_by_topic.sql",
            "txn_topition_delete_by_topic.sql",
            "producer_detail_delete_by_topic.sql",
            "topition_delete_by_topic.sql",
        ] {
            let rows = pc.execute(sql, (self.cluster.as_str(), topic_name)).await?;

            debug!(?topic, rows, sql)
        }

        _ = pc
            .execute("topic_delete_by.sql", (self.cluster.as_str(), topic_name))
            .await?;

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

                for config in resource.configs.unwrap_or_default() {
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
                                let mut list: Vec<&str> = current_value.as_deref().map(|s| s.split(',').map(str::trim).filter(|s| !s.is_empty()).collect()).unwrap_or_default();
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
                                let list: Vec<&str> = current_value.as_deref().map(|s| s.split(',').map(str::trim).filter(|s| !s.is_empty() && *s != del_val.as_str()).collect()).unwrap_or_default();
                                
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
