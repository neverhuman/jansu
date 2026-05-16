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
    pub(super) async fn delegate_txn_add_partitions(
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

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                todo!()
            }
        }
    }

    pub(super) async fn delegate_txn_offset_commit(
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

    pub(super) async fn delegate_txn_end(
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

}
