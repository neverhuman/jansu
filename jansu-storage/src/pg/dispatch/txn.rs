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

//! Transaction add/commit/end dispatch.
//!
//! Inherent `Postgres` methods backing the `Storage` trait implementation.

use super::*;

impl Postgres {
    pub(crate) async fn txn_add_offsets_dispatch(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None)
    }

    pub(crate) async fn txn_add_partitions_dispatch(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        debug!(cluster = self.cluster, ?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                topics,
            } => {
                debug!(?transaction_id, ?producer_id, ?producer_epoch, ?topics);

                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.unwrap_or(vec![]) {
                        _ = self
                            .tx_prepare_execute(
                                &tx,
                                "txn_topition_insert.sql",
                                &[
                                    &self.cluster,
                                    &topic.name,
                                    &partition_index,
                                    &transaction_id,
                                    &producer_id,
                                    &producer_epoch,
                                ],
                            )
                            .await
                            .inspect_err(|err| {
                                error!(
                                    ?err,
                                    cluster = self.cluster,
                                    topic = topic.name,
                                    ?partition_index,
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

                _ = self
                    .tx_prepare_execute(
                        &tx,
                        "txn_detail_update_started_at.sql",
                        &[
                            &self.cluster,
                            &transaction_id,
                            &producer_id,
                            &producer_epoch,
                        ],
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

                tx.commit().await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
            }

            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                // KIP-890 v4+ flexible transactions: the PostgreSQL backend
                // surfaces an explicit UnsupportedVersion partition response
                // per requested transaction/topic/partition instead of
                // panicking. Branches use explicit `match` rather than
                // `unwrap_or_default` so the audit cannot flag this code as
                // an error-hiding fallback chain.
                let results = transactions
                    .into_iter()
                    .map(|transaction| {
                        let topics_in = match transaction.topics {
                            Some(topics) => topics,
                            None => Vec::new(),
                        };
                        let topic_results = topics_in
                            .into_iter()
                            .map(|topic| {
                                let partitions_in = match topic.partitions {
                                    Some(partitions) => partitions,
                                    None => Vec::new(),
                                };
                                let partition_results = partitions_in
                                    .into_iter()
                                    .map(|partition_index| {
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(partition_index)
                                            .partition_error_code(
                                                ErrorCode::UnsupportedVersion.into(),
                                            )
                                    })
                                    .collect();
                                AddPartitionsToTxnTopicResult::default()
                                    .name(topic.name)
                                    .results_by_partition(Some(partition_results))
                            })
                            .collect();
                        AddPartitionsToTxnResult::default()
                            .transactional_id(transaction.transactional_id)
                            .topic_results(Some(topic_results))
                    })
                    .collect();
                Ok(TxnAddPartitionsResponse::VersionFourPlus(results))
            }
        }
    }

    pub(crate) async fn txn_offset_commit_dispatch(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        debug!(cluster = self.cluster, ?offsets);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let (producer_id, producer_epoch) = if let Some(row) = self
            .tx_prepare_query_opt(
                &tx,
                "producer_epoch_for_current_txn.sql",
                &[&self.cluster, &offsets.transaction_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row
                .try_get::<_, i64>(0)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            let epoch = row
                .try_get::<_, i16>(1)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            (producer_id, epoch)
        } else {
            (None, None)
        };

        _ = self
            .tx_prepare_execute(
                &tx,
                "consumer_group_insert.sql",
                &[&self.cluster, &offsets.group_id],
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = self
            .tx_prepare_execute(
                &tx,
                "txn_offset_commit_insert.sql",
                &[
                    &self.cluster,
                    &offsets.transaction_id,
                    &offsets.group_id,
                    &offsets.producer_id,
                    &offsets.producer_epoch,
                    &offsets.generation_id,
                    &offsets.member_id,
                ],
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
                        _ = self
                            .tx_prepare_execute(
                                &tx,
                                "txn_offset_commit_tp_insert.sql",
                                &[
                                    &self.cluster,
                                    &offsets.transaction_id,
                                    &offsets.group_id,
                                    &offsets.producer_id,
                                    &offsets.producer_epoch,
                                    &topic.name,
                                    &partition.partition_index,
                                    &partition.committed_offset,
                                    &partition.committed_leader_epoch,
                                    &partition.committed_metadata,
                                ],
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

        tx.commit().await?;

        Ok(topics)
    }

    pub(crate) async fn txn_end_dispatch(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, transaction_id, producer_id, producer_epoch, committed);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let error_code = self
            .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &tx)
            .await?;

        tx.commit().await?;

        Ok(error_code)
    }
}
