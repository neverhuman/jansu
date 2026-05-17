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
    fn mark_txn_started(
        &self,
        pc: &mut PoolConnection,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        let started_at = Value::from(SystemTime::now());

        let ids: Vec<i64> = {
            let s = sql("redlinedb/txn_detail_select_started_at_id.sql").map_err(Error::from)?;
            let mut rows = pc.query(
                &s,
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                ),
            ).map_err(Error::from)?;
            let mut ids = vec![];
            while let Step::Row(row) = rows.step().map_err(Error::from)? {
                ids.push(row.get::<i64>(0).map_err(Error::from)?);
            }
            ids
        };

        let ds = sql("redlinedb/txn_detail_update_started_at_by_id.sql").map_err(Error::from)?;
        for txn_detail_id in ids {
            let _ = pc.execute(&ds, (started_at.clone(), txn_detail_id)).map_err(Error::from)?;
        }

        Ok(())
    }

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

                let mut pc = self.connection().await?;
                pc.begin(BeginMode::Immediate).map_err(Error::from)?;

                let mut results = vec![];

                let s = sql("txn_topition_insert.sql").map_err(Error::from)?;
                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.unwrap_or(vec![]) {
                        let _ = pc.execute(
                            &s,
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition_index,
                                transaction_id.as_str(),
                                producer_id,
                                producer_epoch,
                            ),
                        )
                        .map_err(Error::from)
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

                self.mark_txn_started(&mut pc, transaction_id.as_str(), producer_id, producer_epoch)
                    .inspect_err(|err| {
                        error!(
                            ?err,
                            cluster = self.cluster,
                            transaction_id,
                            producer_id,
                            producer_epoch,
                        )
                    })?;

                let _ = pc.commit().map_err(Error::from)?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results)).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "txn_add_partitions")],
                    )
                })
            }

            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                debug!(?transactions);

                let mut pc = self.connection().await?;
                pc.begin(BeginMode::Immediate).map_err(Error::from)?;

                let mut results = vec![];

                for transaction in transactions {
                    let transaction_id = &transaction.transactional_id;
                    let producer_id = transaction.producer_id;
                    let producer_epoch = transaction.producer_epoch;
                    let verify_only = transaction.verify_only;

                    let mut topic_results = vec![];

                    for topic in transaction.topics.unwrap_or(vec![]) {
                        let mut results_by_partition = vec![];

                        for partition_index in topic.partitions.unwrap_or(vec![]) {
                            if verify_only {
                                let exists = {
                                    let s = sql("txn_topition_select.sql").map_err(Error::from)?;
                                    let mut rows = pc.query(
                                        &s,
                                        (
                                            self.cluster.as_str(),
                                            producer_id,
                                            producer_epoch,
                                            topic.name.as_str(),
                                            partition_index,
                                        ),
                                    ).map_err(Error::from)?;
                                    matches!(rows.step().map_err(Error::from)?, Step::Row(_))
                                };

                                if exists {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(partition_index)
                                            .partition_error_code(i16::from(ErrorCode::None)),
                                    );
                                } else {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(partition_index)
                                            .partition_error_code(i16::from(
                                                ErrorCode::InvalidTxnState,
                                            )),
                                    );
                                }
                            } else {
                                let s = sql("txn_topition_insert.sql").map_err(Error::from)?;
                                let _ = pc.execute(
                                    &s,
                                    (
                                        self.cluster.as_str(),
                                        topic.name.as_str(),
                                        partition_index,
                                        transaction_id.as_str(),
                                        producer_id,
                                        producer_epoch,
                                    ),
                                )
                                .map_err(Error::from)
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
                        }

                        topic_results.push(
                            AddPartitionsToTxnTopicResult::default()
                                .name(topic.name)
                                .results_by_partition(Some(results_by_partition)),
                        );
                    }

                    if !verify_only {
                        self.mark_txn_started(
                            &mut pc,
                            transaction_id.as_str(),
                            producer_id,
                            producer_epoch,
                        )
                        .inspect_err(|err| {
                            error!(
                                ?err,
                                cluster = self.cluster,
                                transaction_id,
                                producer_id,
                                producer_epoch,
                            )
                        })?;
                    }

                    results.push(
                        AddPartitionsToTxnResult::default()
                            .transactional_id(transaction_id.clone())
                            .topic_results(Some(topic_results)),
                    );
                }

                let _ = pc.commit().map_err(Error::from)?;

                Ok(TxnAddPartitionsResponse::VersionFourPlus(results)).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "txn_add_partitions")],
                    )
                })
            }
        }
    }

    pub(super) async fn delegate_txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?offsets);

        let mut pc = self.connection().await?;
        pc.begin(BeginMode::Immediate).map_err(Error::from)?;

        let (producer_id, producer_epoch) = {
            let s = sql("producer_epoch_for_current_txn.sql").map_err(Error::from)?;
            let mut rows = pc.query(
                &s,
                (self.cluster.as_str(), offsets.transaction_id.as_str()),
            ).map_err(Error::from).inspect_err(|err| error!(?err))?;

            match rows.step().map_err(Error::from)? {
                Step::Row(row) => {
                    let producer_id = row.get::<i64>(0).map(Some).inspect_err(|err| error!(?err))?;
                    let epoch_i32 = row.get::<i32>(1).inspect_err(|err| error!(?err))?;
                    let epoch = Some(i16::try_from(epoch_i32)?);
                    (producer_id, epoch)
                }
                Step::Done => (None, None),
            }
        };

        let s = sql("consumer_group_insert.sql").map_err(Error::from)?;
        let _ = pc.execute(&s, (self.cluster.as_str(), offsets.group_id.as_str())).map_err(Error::from)?;

        debug!(?producer_id, ?producer_epoch);

        let s = sql("redlinedb/txn_offset_commit_insert.sql").map_err(Error::from)?;
        let _ = pc.execute(
            &s,
            (
                offsets.generation_id,
                offsets.member_id,
                self.cluster.as_str(),
                offsets.transaction_id.as_str(),
                offsets.group_id.as_str(),
                offsets.producer_id,
                offsets.producer_epoch,
            ),
        )
        .map_err(Error::from)
        .inspect_err(|err| error!(?err))?;

        let mut topics = vec![];

        for topic in offsets.topics {
            let mut partitions = vec![];

            for partition in topic.partitions.unwrap_or(vec![]) {
                if producer_id.is_some_and(|producer_id| producer_id == offsets.producer_id) {
                    if producer_epoch
                        .is_some_and(|producer_epoch| producer_epoch == offsets.producer_epoch)
                    {
                        let s = sql("redlinedb/txn_offset_commit_tp_insert.sql").map_err(Error::from)?;
                        let _ = pc.execute(
                            &s,
                            (
                                partition.committed_offset,
                                partition.committed_leader_epoch,
                                partition.committed_metadata,
                                self.cluster.as_str(),
                                offsets.transaction_id.as_str(),
                                offsets.group_id.as_str(),
                                offsets.producer_id,
                                offsets.producer_epoch,
                                topic.name.as_str(),
                                partition.partition_index,
                            ),
                        )
                        .map_err(Error::from)
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

        let _ = pc.commit().map_err(Error::from)?;

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

        let mut pc = self.connection().await?;
        pc.begin(BeginMode::Immediate).map_err(Error::from)?;

        let error_code = self
            .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &mut pc)
            .await?;

        let _ = pc.commit().map_err(Error::from)?;

        Ok(error_code).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_end")],
            )
        })
    }
}
