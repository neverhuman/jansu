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

pub(super) async fn register_broker(
    this: &Engine,
    broker_registration: BrokerRegistrationRequest,
) -> Result<()> {
    debug!(?broker_registration);

    let connection = this.connection().await?;

    this.prepare_execute(
        &connection,
        &sql_lookup("register_broker.sql")?,
        &[broker_registration.cluster_id],
    )
    .await
    .map_err(Into::into)
    .and(Ok(()))
}

pub(super) async fn brokers(this: &Engine) -> Result<Vec<DescribeClusterBroker>> {
    debug!(cluster = this.cluster);

    let broker_id = this.node;
    let host = this
        .advertised_listener
        .host_str()
        .unwrap_or("0.0.0.0")
        .into();
    let port = this.advertised_listener.port().unwrap_or(9092).into();
    let rack = None;

    Ok(vec![
        DescribeClusterBroker::default()
            .broker_id(broker_id)
            .host(host)
            .port(port)
            .rack(rack),
    ])
}

pub(super) async fn create_topic(
    this: &Engine,
    topic: CreatableTopic,
    validate_only: bool,
) -> Result<Uuid> {
    debug!(cluster = this.cluster, ?topic, validate_only);

    let mut connection = this.connection().await.inspect_err(|err| error!(?err))?;

    let tx = connection.transaction().await?;

    let uuid = {
        let uuid = Uuid::new_v4();

        let parameters = (
            this.cluster.as_str(),
            topic.name.as_str(),
            uuid.to_string(),
            topic.num_partitions,
            (topic.replication_factor as i32),
        );

        this.prepare_query_one(&tx, &sql_lookup("topic_insert.sql")?, parameters.clone())
            .await
            .inspect_err(|err| error!(?err))
            .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
            .inspect(|row| debug!(?parameters, ?row))
            .and_then(|row| {
                row.get_value(0)
                    .map(|value| value.as_text().cloned().unwrap())
                    .inspect_err(|err| error!(?err))
                    .map_err(Into::into)
            })
            .and_then(|id| Uuid::parse_str(id.as_str()).map_err(Into::into))
    }
    .inspect(|uuid| debug!(?uuid))
    .inspect_err(|err| error!(?err))?;

    for partition in 0..topic.num_partitions {
        let params = (this.cluster.as_str(), topic.name.as_str(), partition);

        _ = this
            .prepare_query_one(&tx, &sql_lookup("topition_insert.sql")?, params)
            .await
            .map(|row| row.get_value(0))
            .inspect(|topition| debug!(?topition))?;

        _ = this
            .prepare_query_one(&tx, &sql_lookup("watermark_insert.sql")?, params)
            .await
            .map(|row| row.get_value(0))
            .inspect(|watermark| debug!(?watermark))?;

        _ = this
            .prepare_execute(
                &tx,
                &sql_lookup("leader_epoch_history_insert.sql")?,
                (this.cluster.as_str(), topic.name.as_str(), partition, 0, 0),
            )
            .await
            .inspect_err(|err| error!(?err, ?topic, ?partition))?;
    }

    if let Some(configs) = topic.configs {
        for config in configs {
            debug!(?config);

            let params = (
                this.cluster.as_str(),
                topic.name.as_str(),
                config.name.as_str(),
                config.value.as_deref(),
            );

            _ = this
                .prepare_query_one(&tx, &sql_lookup("topic_configuration_upsert.sql")?, params)
                .await
                .map(|row| row.get_value(0))
                .inspect_err(|err| error!(?err, ?config))
                .inspect(|id| debug!(?id, ?config))?;
        }
    }

    tx.commit().await.map_err(Into::into).and(Ok(uuid))
}

pub(super) async fn delete_records(
    this: &Engine,
    topics: &[DeleteRecordsTopic],
) -> Result<Vec<DeleteRecordsTopicResult>> {
    debug!(cluster = this.cluster, ?topics);

    let mut connection = this.connection().await.inspect_err(|err| error!(?err))?;
    let tx = connection.transaction().await?;

    let mut results = vec![];

    for topic in topics {
        let mut partitions = vec![];

        for partition in topic.partitions.as_ref().unwrap_or(&vec![]) {
            let partition_index = partition.partition_index;
            let offset = partition.offset;
            let topic_name = topic.name.as_str();

            let params = (this.cluster.as_str(), topic_name, partition_index);

            // Validate if topition exists
            let mut rows = tx.query(
                &sql_lookup("topition_select.sql")?,
                params,
            ).await?;

            if rows.next().await?.is_none() {
                partitions.push(
                    DeleteRecordsPartitionResult::default()
                        .partition_index(partition_index)
                        .low_watermark(-1)
                        .error_code(i16::from(ErrorCode::UnknownTopicOrPartition)),
                );
                continue;
            }

            _ = this.prepare_execute(
                &tx,
                &sql_lookup("record_delete_by_offset.sql")?,
                (this.cluster.as_str(), topic_name, partition_index, offset),
            ).await.inspect_err(|err| error!(?err))?;

            _ = this.prepare_execute(
                &tx,
                &sql_lookup("watermark_update_low.sql")?,
                (this.cluster.as_str(), topic_name, partition_index, offset),
            ).await.inspect_err(|err| error!(?err))?;

            let mut rows = tx.query(
                &sql_lookup("watermark_select_no_update.sql")?,
                params,
            ).await?;

            let low_watermark = if let Some(row) = rows.next().await? {
                row.get_value(0)?.as_integer().copied().unwrap_or(0)
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

    tx.commit().await?;

    Ok(results)
}

pub(super) async fn delete_topic(this: &Engine, topic: &TopicId) -> Result<ErrorCode> {
    debug!(cluster = this.cluster, ?topic);

    let mut connection = this.connection().await?;
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;

    let mut rows = match topic {
        TopicId::Id(id) => {
            tx.query(
                &sql_lookup("topic_select_uuid.sql")?,
                (this.cluster.as_str(), id.to_string().as_str()),
            )
            .await?
        }

        TopicId::Name(name) => {
            tx.query(
                &sql_lookup("topic_select_name.sql")?,
                (this.cluster.as_str(), name.as_str()),
            )
            .await?
        }
    };

    let Some(row) = rows.next().await? else {
        return Ok(ErrorCode::UnknownTopicOrPartition);
    };

    let value = row.get_value(1)?;
    let topic_name = value
        .as_text()
        .map(|topic_name| topic_name.as_str())
        .ok_or(Error::UnexpectedValue(value.clone()))?;

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
        let rows = this
            .prepare_execute(&tx, &sql_lookup(sql)?, (this.cluster.as_str(), topic_name))
            .await?;

        debug!(?topic, rows, sql)
    }

    _ = this
        .prepare_execute(
            &tx,
            &sql_lookup("topic_delete_by.sql")?,
            (this.cluster.as_str(), topic_name),
        )
        .await?;

    tx.commit()
        .await
        .map_err(Into::into)
        .and(Ok(ErrorCode::None))
}

pub(super) async fn incremental_alter_resource(
    this: &Engine,
    resource: AlterConfigsResource,
) -> Result<AlterConfigsResourceResponse> {
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
                        let c = this.connection().await?;

                        if c.query(
                            &sql_lookup("topic_configuration_upsert.sql")?,
                            (
                                this.cluster.as_str(),
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
                        let c = this.connection().await?;

                        if c.query(
                            &sql_lookup("topic_configuration_delete.sql")?,
                            (
                                this.cluster.as_str(),
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
                        let c = this.connection().await?;
                        let mut rows = c
                            .query(
                                &sql_lookup("topic_configuration_select.sql")?,
                                (
                                    this.cluster.as_str(),
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
                                &sql_lookup("topic_configuration_upsert.sql")?,
                                (
                                    this.cluster.as_str(),
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
                        let c = this.connection().await?;
                        let mut rows = c
                            .query(
                                &sql_lookup("topic_configuration_select.sql")?,
                                (
                                    this.cluster.as_str(),
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
                                    &sql_lookup("topic_configuration_delete.sql")?,
                                    (
                                        this.cluster.as_str(),
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
                                    &sql_lookup("topic_configuration_upsert.sql")?,
                                    (
                                        this.cluster.as_str(),
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
        }
        ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
            .error_code(ErrorCode::None.into())
            .error_message(Some("".into()))
            .resource_type(resource.resource_type)
            .resource_name(resource.resource_name)),
    }
}
