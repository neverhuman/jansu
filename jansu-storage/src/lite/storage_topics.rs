//! `Storage` topic lifecycle operations for the libSQL `Delegate`.

use super::*;

impl Delegate {
    pub(super) async fn create_topic_inner(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
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

    pub(super) async fn delete_topic_inner(&self, topic: &TopicId) -> Result<ErrorCode> {
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

    pub(super) async fn delete_records_inner(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);
        let pc = self.connection().await?;
        let mut responses = Vec::with_capacity(topics.len());

        for topic in topics {
            let topic_exists = pc
                .query_opt(
                    "topic_select_name.sql",
                    (self.cluster.as_str(), topic.name.as_str()),
                )
                .await?
                .is_some();

            let mut partition_responses = vec![];

            for partition in topic.partitions.as_deref().unwrap_or(&[]) {
                let (error_code, low_watermark) = if !topic_exists {
                    (ErrorCode::UnknownTopicOrPartition, 0)
                } else if pc
                    .query_opt(
                        "topition_select.sql",
                        (
                            self.cluster.as_str(),
                            topic.name.as_str(),
                            partition.partition_index,
                        ),
                    )
                    .await?
                    .is_none()
                {
                    (ErrorCode::UnknownTopicOrPartition, 0)
                } else {
                    let watermark = pc
                        .query_one(
                            "watermark_select.sql",
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                            ),
                        )
                        .await?;
                    let current_low = watermark.get::<Option<i64>>(0)?.unwrap_or(0);
                    let high = watermark.get::<Option<i64>>(1)?.unwrap_or(0);
                    let low = current_low.max(partition.offset.clamp(0, high));

                    _ = pc
                        .execute(
                            "record_delete_before_offset.sql",
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                                low,
                            ),
                        )
                        .await?;

                    _ = pc
                        .execute(
                            "watermark_update.sql",
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                                low,
                                high,
                            ),
                        )
                        .await?;

                    (ErrorCode::None, low)
                };

                partition_responses.push(
                    DeleteRecordsPartitionResult::default()
                        .partition_index(partition.partition_index)
                        .low_watermark(low_watermark)
                        .error_code(error_code.into()),
                );
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
