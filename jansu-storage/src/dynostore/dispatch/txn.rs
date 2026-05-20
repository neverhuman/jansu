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

//! Transaction add-offsets/add-partitions dispatch.
//!
//! Inherent `DynoStore` methods backing the `Storage` trait implementation.

use super::*;

impl DynoStore {
    pub(crate) async fn txn_add_offsets_dispatch(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    pub(crate) async fn txn_add_partitions_dispatch(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                ref topics,
            } => {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        let Some(transaction) = meta.transactions.get_mut(&transaction_id) else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(
                                                ErrorCode::TransactionalIdNotFound.into(),
                                            ),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if transaction.producer != producer_id {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(
                                                ErrorCode::UnknownProducerId.into(),
                                            ),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        }

                        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(ErrorCode::ProducerFenced.into()),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if &producer_epoch != current_epoch.key() {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(ErrorCode::ProducerFenced.into()),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        }

                        let txn_detail = current_epoch.get_mut();

                        let mut results = vec![];

                        for topic in topics {
                            let mut results_by_partition = vec![];

                            for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                _ = txn_detail
                                    .produces
                                    .entry(topic.name.clone())
                                    .or_default()
                                    .entry(*partition_index)
                                    .or_default();

                                results_by_partition.push(
                                    AddPartitionsToTxnPartitionResult::default()
                                        .partition_index(*partition_index)
                                        .partition_error_code(i16::from(ErrorCode::None)),
                                );
                            }

                            results.push(
                                AddPartitionsToTxnTopicResult::default()
                                    .name(topic.name.clone())
                                    .results_by_partition(Some(results_by_partition)),
                            )
                        }

                        txn_detail.started_at = Some(SystemTime::now());
                        txn_detail.state = Some(TxnState::Begin);

                        Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
                    })
                    .await
            }

            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        let mut results = vec![];

                        for transaction in &transactions {
                            let topics = transaction.topics.as_deref().unwrap_or(&[]);
                            let mut topic_results = vec![];

                            let transaction_error =
                                match meta.transactions.get_mut(&transaction.transactional_id) {
                                    Some(txn) if txn.producer != transaction.producer_id => {
                                        ErrorCode::ProducerFenced
                                    }
                                    Some(txn) => {
                                        let txn_detail = txn
                                            .epochs
                                            .entry(transaction.producer_epoch)
                                            .or_insert_with(|| TxnDetail {
                                                transaction_timeout_ms: 0,
                                                ..Default::default()
                                            });

                                        for topic in topics {
                                            for partition_index in
                                                topic.partitions.as_deref().unwrap_or(&[])
                                            {
                                                if !transaction.verify_only {
                                                    _ = txn_detail
                                                        .produces
                                                        .entry(topic.name.clone())
                                                        .or_default()
                                                        .entry(*partition_index)
                                                        .or_default();
                                                }
                                            }
                                        }

                                        txn_detail.started_at = Some(SystemTime::now());
                                        txn_detail.state = Some(TxnState::Begin);
                                        ErrorCode::None
                                    }
                                    None => ErrorCode::TransactionalIdNotFound,
                                };

                            for topic in topics {
                                topic_results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(
                                            topic
                                                .partitions
                                                .as_deref()
                                                .unwrap_or(&[])
                                                .iter()
                                                .copied()
                                                .map(|partition_index| {
                                                    AddPartitionsToTxnPartitionResult::default()
                                                        .partition_index(partition_index)
                                                        .partition_error_code(i16::from(
                                                            transaction_error,
                                                        ))
                                                })
                                                .collect(),
                                        )),
                                );
                            }

                            results.push(
                                AddPartitionsToTxnResult::default()
                                    .transactional_id(transaction.transactional_id.clone())
                                    .topic_results(Some(topic_results)),
                            );
                        }

                        Ok(TxnAddPartitionsResponse::VersionFourPlus(results))
                    })
                    .await
            }
        }
    }
}
