//! `Storage` transaction and maintenance operations for the libSQL `Delegate`.

use super::*;

impl Delegate {
    pub(super) async fn txn_add_partitions_inner(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                topics,
            } => {
                debug!(?transaction_id, ?producer_id, ?producer_epoch, ?topics);

                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.unwrap_or(vec![]) {
                        _ = pc
                            .execute(
                                "txn_topition_insert.sql",
                                (
                                    self.cluster.as_str(),
                                    topic.name.as_str(),
                                    partition_index,
                                    transaction_id.as_str(),
                                    producer_id,
                                    producer_epoch,
                                ),
                            )
                            .await
                            .inspect_err(|err| {
                                error!(
                                    ?err,
                                    cluster = self.cluster,
                                    topic = topic.name,
                                    partition_index,
                                    transaction_id
                                )
                            })?;

                        results_by_partition.push(
                            AddPartitionsToTxnPartitionResult::default()
                                .partition_index(partition_index)
                                .partition_error_code(i16::from(ErrorCode::None)),
                        );
                    }

                    results.push(
                        AddPartitionsToTxnTopicResult::default()
                            .name(topic.name)
                            .results_by_partition(Some(results_by_partition)),
                    )
                }

                _ = pc
                    .execute(
                        "txn_detail_update_started_at.sql",
                        (
                            self.cluster.as_str(),
                            transaction_id.as_str(),
                            producer_id,
                            producer_epoch,
                        ),
                    )
                    .await
                    .inspect_err(|err| {
                        error!(
                            ?err,
                            cluster = self.cluster,
                            transaction_id,
                            producer_id,
                            producer_epoch,
                        )
                    })?;

                pc.commit(tx).await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results)).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "txn_add_partitions")],
                    )
                })
            }

            TxnAddPartitionsRequest::VersionFourPlus { transactions } => Ok(
                TxnAddPartitionsResponse::VersionFourPlus(
                    transactions
                        .into_iter()
                        .map(|transaction| {
                            let topics_in = match transaction.topics {
                                Some(topics) => topics,
                                None => Vec::new(),
                            };
                            AddPartitionsToTxnResult::default()
                                .transactional_id(transaction.transactional_id)
                                .topic_results(Some(
                                    topics_in
                                        .into_iter()
                                        .map(|topic| {
                                            let partitions_in = match topic.partitions {
                                                Some(partitions) => partitions,
                                                None => Vec::new(),
                                            };
                                            AddPartitionsToTxnTopicResult::default()
                                                .name(topic.name)
                                                .results_by_partition(Some(
                                                    partitions_in
                                                        .into_iter()
                                                        .map(|partition_index| {
                                                            AddPartitionsToTxnPartitionResult::default()
                                                                .partition_index(partition_index)
                                                                .partition_error_code(
                                                                    ErrorCode::UnsupportedVersion
                                                                        .into(),
                                                                )
                                                        })
                                                        .collect(),
                                                ))
                                        })
                                        .collect(),
                                ))
                        })
                        .collect(),
                ),
            ),
        }
    }

    pub(super) async fn txn_offset_commit_inner(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?offsets);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let (producer_id, producer_epoch) = if let Some(row) = pc
            .query_opt(
                "producer_epoch_for_current_txn.sql",
                (self.cluster.as_str(), offsets.transaction_id.as_str()),
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row
                .get::<i64>(0)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            let epoch = row
                .get::<i32>(1)
                .map(|epoch| epoch as i16)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            (producer_id, epoch)
        } else {
            (None, None)
        };

        _ = pc
            .execute(
                "consumer_group_insert.sql",
                (self.cluster.as_str(), offsets.group_id.as_str()),
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = pc
            .execute(
                "txn_offset_commit_insert.sql",
                (
                    self.cluster.as_str(),
                    offsets.transaction_id.as_str(),
                    offsets.group_id.as_str(),
                    offsets.producer_id,
                    offsets.producer_epoch,
                    offsets.generation_id,
                    offsets.member_id,
                ),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let mut topics = vec![];

        for topic in offsets.topics {
            let mut partitions = vec![];

            for partition in topic.partitions.unwrap_or(vec![]) {
                if producer_id.is_some_and(|producer_id| producer_id == offsets.producer_id) {
                    if producer_epoch
                        .is_some_and(|producer_epoch| producer_epoch == offsets.producer_epoch)
                    {
                        _ = pc
                            .execute(
                                "txn_offset_commit_tp_insert.sql",
                                (
                                    self.cluster.as_str(),
                                    offsets.transaction_id.as_str(),
                                    offsets.group_id.as_str(),
                                    offsets.producer_id,
                                    offsets.producer_epoch,
                                    topic.name.as_str(),
                                    partition.partition_index,
                                    partition.committed_offset,
                                    partition.committed_leader_epoch,
                                    partition.committed_metadata,
                                ),
                            )
                            .await
                            .inspect_err(|err| error!(?err))?;

                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::None)),
                        );
                    } else {
                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::InvalidProducerEpoch)),
                        );
                    }
                } else {
                    partitions.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(i16::from(ErrorCode::UnknownProducerId)),
                    );
                }
            }

            topics.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name)
                    .partitions(Some(partitions)),
            );
        }

        pc.commit(tx).await?;

        Ok(topics).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_offset_commit")],
            )
        })
    }

    pub(super) async fn txn_end_inner(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();

        debug!(cluster = ?self.cluster, transaction_id, producer_id, producer_epoch, committed);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let error_code = self
            .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &pc)
            .await?;

        pc.commit(tx).await.and(Ok(error_code)).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_end")],
            )
        })
    }

    pub(super) async fn maintain_inner(&self, now: SystemTime) -> Result<()> {
        self.vacuum_into().await?;

        let Ok(_permit) = self.maintenance.try_acquire() else {
            return Ok(());
        };

        let start = SystemTime::now();

        let deleted = self.policy_delete(now).await?;
        debug!(deleted);

        let connection = self.pool.get().await?;
        let expired = connection
            .execute(
                "consumer_offset_delete_expired.sql",
                (self.cluster.as_str(), LiteTimestamp::from(now)),
            )
            .await?;
        debug!(expired);

        let compacted = self.policy_compact().await?;
        debug!(compacted);

        {
            let mut rows = connection.query("maintain-vacuum.sql", ()).await?;

            if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
                debug!(
                    freelist_count = row.get_str(0)?,
                    page_size = row.get_str(1)?
                );
            }
        }

        Ok(()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "maintain")],
            )
        })
    }
}
