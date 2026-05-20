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

//! Topic create/delete and record-deletion dispatch.
//!
//! Inherent `Postgres` methods backing the `Storage` trait implementation.

use super::*;

impl Postgres {
    pub(crate) async fn create_topic_dispatch(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let uuid = Uuid::new_v4();

        let topic_uuid = self
            .tx_prepare_query_one(
                &tx,
                "topic_insert.sql",
                &[
                    &self.cluster,
                    &topic.name,
                    &uuid,
                    &topic.num_partitions,
                    &(topic.replication_factor as i32),
                ],
            )
            .await
            .inspect_err(|err| debug!(?err, ?topic, ?validate_only))
            .map(|row| row.get(0))
            .map_err(|error| {
                if let Error::TokioPostgres(ref error) = error
                    && error
                        .code()
                        .is_some_and(|code| *code == SqlState::UNIQUE_VIOLATION)
                {
                    Error::Api(ErrorCode::TopicAlreadyExists)
                } else {
                    error
                }
            })?;

        debug!(?topic_uuid, cluster = self.cluster, ?topic);

        for partition in 0..topic.num_partitions {
            let cluster = Box::new(self.cluster.clone()) as Box<dyn ToSql + Sync + Send>;
            let name = Box::new(topic.name.clone()) as Box<dyn ToSql + Sync + Send>;
            let partition_value = Box::new(partition) as Box<dyn ToSql + Sync + Send>;
            _ = self
                .tx_prepare_query_raw(&tx, "topition_insert.sql", [cluster, name, partition_value])
                .await?;

            let cluster = Box::new(self.cluster.clone()) as Box<dyn ToSql + Sync + Send>;
            let name = Box::new(topic.name.clone()) as Box<dyn ToSql + Sync + Send>;
            let partition_value = Box::new(partition) as Box<dyn ToSql + Sync + Send>;
            _ = self
                .tx_prepare_query_raw(
                    &tx,
                    "watermark_insert.sql",
                    [cluster, name, partition_value],
                )
                .await?;

            _ = self
                .tx_prepare_execute(
                    &tx,
                    "leader_epoch_history_insert.sql",
                    &[&self.cluster, &topic.name, &partition, &0_i32, &0_i64],
                )
                .await?;
        }

        if let Some(configs) = topic.configs {
            for config in configs {
                debug!(?config);

                _ = self
                    .tx_prepare_execute(
                        &tx,
                        "topic_configuration_upsert.sql",
                        &[
                            &self.cluster,
                            &topic.name,
                            &config.name,
                            &config.value.as_deref(),
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err, ?config));
            }
        }

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(topic_uuid)
    }

    pub(crate) async fn delete_topic_dispatch(&self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(cluster = self.cluster, ?topic);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let row = match topic {
            TopicId::Id(id) => {
                self.tx_prepare_query_opt(&tx, "topic_select_uuid.sql", &[&self.cluster, &id])
                    .await?
            }

            TopicId::Name(name) => {
                self.tx_prepare_query_opt(&tx, "topic_select_name.sql", &[&self.cluster, name])
                    .await?
            }
        };

        let Some(row) = row else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let topic_name = row.try_get::<_, String>(1)?;

        for (description, sql) in [
            ("consumer_offsets", "consumer_offset_delete_by_topic.sql"),
            (
                "topic_configuration",
                "topic_configuration_delete_by_topic.sql",
            ),
            ("watermarks", "watermark_delete_by_topic.sql"),
            ("headers", "header_delete_by_topic.sql"),
            ("records", "record_delete_by_topic.sql"),
            (
                "txn_offset_commit_tp",
                "txn_offset_commit_tp_delete_by_topic.sql",
            ),
            (
                "txn_produce_offset_delete",
                "txn_produce_offset_delete_by_topic.sql",
            ),
            ("txn_topition", "txn_topition_delete_by_topic.sql"),
            ("producer_detail", "producer_detail_delete_by_topic.sql"),
            ("topitions", "topition_delete_by_topic.sql"),
        ] {
            let rows = self
                .tx_prepare_execute(&tx, sql, &[&self.cluster, &topic_name])
                .await
                .inspect_err(|err| {
                    debug!(?description, ?err);
                })?;

            debug!(?topic, ?rows, ?description);
        }

        _ = self
            .tx_prepare_execute(&tx, "topic_delete_by.sql", &[&self.cluster, &topic_name])
            .await?;

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(ErrorCode::None)
    }

    pub(crate) async fn delete_records_dispatch(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(cluster = self.cluster, ?topics);

        let c = self.connection().await?;

        let delete_records = c
            .prepare(concat!(
                "delete from record",
                " using topic, cluster",
                " where",
                " cluster.name=$1",
                " and topic.name = $2",
                " and record.partition = $3",
                " and record.id >= $4",
                " and topic.cluster = cluster.id",
                " and record.topic = topic.id",
            ))
            .await
            .inspect_err(|err| error!(?err, ?topics))?;

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                for partition in partitions {
                    _ = c
                        .execute(
                            &delete_records,
                            &[
                                &self.cluster,
                                &topic.name,
                                &partition.partition_index,
                                &partition.offset,
                            ],
                        )
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })?;

                    let prepared = c
                        .prepare(concat!(
                            "select",
                            " id as offset",
                            " from",
                            " record",
                            " join (",
                            " select",
                            " coalesce(min(record.id), (select last_value from record_id_seq)) as offset",
                            " from record, topic, cluster",
                            " where",
                            " topic.cluster = cluster.id",
                            " and cluster.name = $1",
                            " and topic.name = $2",
                            " and record.partition = $3",
                            " and record.topic = topic.id) as minimum",
                            " on record.id = minimum.offset",
                        ))
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })?;

                    let partition_result = c
                        .query_opt(
                            &prepared,
                            &[&self.cluster, &topic.name, &partition.partition_index],
                        )
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })
                        .map_or(
                            Ok(DeleteRecordsPartitionResult::default()
                                .partition_index(partition.partition_index)
                                .low_watermark(0)
                                .error_code(ErrorCode::UnknownServerError.into())),
                            |row| {
                                row.map_or(
                                    Ok(DeleteRecordsPartitionResult::default()
                                        .partition_index(partition.partition_index)
                                        .low_watermark(0)
                                        .error_code(ErrorCode::UnknownServerError.into())),
                                    |row| {
                                        row.try_get::<_, i64>(0).map(|low_watermark| {
                                            DeleteRecordsPartitionResult::default()
                                                .partition_index(partition.partition_index)
                                                .low_watermark(low_watermark)
                                                .error_code(ErrorCode::None.into())
                                        })
                                    },
                                )
                            },
                        )?;

                    partition_responses.push(partition_result);
                }
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }
        Ok(responses)
    }
}
