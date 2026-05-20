use super::sql::{sql_lookup, unique_constraint};
use super::*;

impl Engine {
    pub(super) async fn register_broker_impl(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        debug!(?broker_registration);

        let connection = self.connection().await?;

        self.prepare_execute(
            &connection,
            &sql_lookup("register_broker.sql")?,
            &[broker_registration.cluster_id],
        )
        .await
        .map_err(Into::into)
        .and(Ok(()))
    }

    pub(super) async fn brokers_impl(&self) -> Result<Vec<DescribeClusterBroker>> {
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
    }

    pub(super) async fn create_topic_impl(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut connection = self.connection().await.inspect_err(|err| error!(?err))?;

        let tx = connection.transaction().await?;

        let uuid = {
            let uuid = Uuid::new_v4();

            let parameters = (
                self.cluster.as_str(),
                topic.name.as_str(),
                uuid.to_string(),
                topic.num_partitions,
                (topic.replication_factor as i32),
            );

            self.prepare_query_one(&tx, &sql_lookup("topic_insert.sql")?, parameters.clone())
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
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            _ = self
                .prepare_query_one(&tx, &sql_lookup("topition_insert.sql")?, params)
                .await
                .map(|row| row.get_value(0))
                .inspect(|topition| debug!(?topition))?;

            _ = self
                .prepare_query_one(&tx, &sql_lookup("watermark_insert.sql")?, params)
                .await
                .map(|row| row.get_value(0))
                .inspect(|watermark| debug!(?watermark))?;

            _ = self
                .prepare_execute(
                    &tx,
                    &sql_lookup("leader_epoch_history_insert.sql")?,
                    (self.cluster.as_str(), topic.name.as_str(), partition, 0, 0),
                )
                .await
                .inspect_err(|err| error!(?err, ?topic, ?partition))?;
        }

        if let Some(configs) = topic.configs {
            for config in configs {
                debug!(?config);

                let params = (
                    self.cluster.as_str(),
                    topic.name.as_str(),
                    config.name.as_str(),
                    config.value.as_deref(),
                );

                _ = self
                    .prepare_query_one(&tx, &sql_lookup("topic_configuration_upsert.sql")?, params)
                    .await
                    .map(|row| row.get_value(0))
                    .inspect_err(|err| error!(?err, ?config))
                    .inspect(|id| debug!(?id, ?config))?;
            }
        }

        tx.commit().await.map_err(Into::into).and(Ok(uuid))
    }

    pub(super) async fn delete_records_impl(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);
        let c = self.connection().await?;
        let mut responses = Vec::with_capacity(topics.len());

        for topic in topics {
            let topic_exists = self
                .prepare_query_opt(
                    &c,
                    &sql_lookup("topic_select_name.sql")?,
                    (self.cluster.as_str(), topic.name.as_str()),
                )
                .await?
                .is_some();
            let mut partition_responses = vec![];

            let partitions = match topic.partitions.as_deref() {
                Some(partitions) => partitions,
                None => &[],
            };

            for partition in partitions {
                let (error_code, low_watermark) = if !topic_exists {
                    (ErrorCode::UnknownTopicOrPartition, 0)
                } else if self
                    .prepare_query_opt(
                        &c,
                        &sql_lookup("topition_select.sql")?,
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
                    let watermark = self
                        .prepare_query_one(
                            &c,
                            &sql_lookup("watermark_select.sql")?,
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                            ),
                        )
                        .await?;
                    let current_low = match watermark.get_value(0)?.as_integer().copied() {
                        Some(current_low) => current_low,
                        None => 0,
                    };
                    let high = match watermark.get_value(1)?.as_integer().copied() {
                        Some(high) => high,
                        None => 0,
                    };
                    let low = current_low.max(partition.offset.clamp(0, high));

                    _ = self
                        .prepare_execute(
                            &c,
                            &sql_lookup("record_delete_before_offset.sql")?,
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                                low,
                            ),
                        )
                        .await?;

                    _ = self
                        .prepare_execute(
                            &c,
                            &sql_lookup("watermark_update.sql")?,
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

    pub(super) async fn delete_topic_impl(&self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(cluster = self.cluster, ?topic);

        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let mut rows = match topic {
            TopicId::Id(id) => {
                tx.query(
                    &sql_lookup("topic_select_uuid.sql")?,
                    (self.cluster.as_str(), id.to_string().as_str()),
                )
                .await?
            }

            TopicId::Name(name) => {
                tx.query(
                    &sql_lookup("topic_select_name.sql")?,
                    (self.cluster.as_str(), name.as_str()),
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
            let rows = self
                .prepare_execute(&tx, &sql_lookup(sql)?, (self.cluster.as_str(), topic_name))
                .await?;

            debug!(?topic, rows, sql)
        }

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("topic_delete_by.sql")?,
                (self.cluster.as_str(), topic_name),
            )
            .await?;

        tx.commit()
            .await
            .map_err(Into::into)
            .and(Ok(ErrorCode::None))
    }
}
