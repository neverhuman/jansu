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

//! Transaction Storage impl helpers: txn_add_offsets, txn_add_partitions,
//! txn_offset_commit

use std::time::SystemTime;

use jansu_sans_io::{
    ErrorCode,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use tracing::debug;

use crate::{
    DEFAULT_OFFSET_RETENTION, Error, Result, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest,
};

use super::engine::Engine;
use super::types::Transactions;

impl Engine {
    pub(super) async fn impl_txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Err(Error::Api(ErrorCode::UnknownServerError))
    }

    pub(super) async fn impl_txn_add_partitions(
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
                let tx = self
                    .db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .inspect_err(|err| debug!(?err))?;

                // Helper to create error responses for all topics/partitions
                let make_error_response =
                    |error_code: ErrorCode| -> Vec<AddPartitionsToTxnTopicResult> {
                        topics
                            .iter()
                            .map(|topic| {
                                let results_by_partition = topic
                                    .partitions
                                    .as_deref()
                                    .unwrap_or(&[])
                                    .iter()
                                    .map(|p| {
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*p)
                                            .partition_error_code(error_code.into())
                                    })
                                    .collect();
                                AddPartitionsToTxnTopicResult::default()
                                    .name(topic.name.clone())
                                    .results_by_partition(Some(results_by_partition))
                            })
                            .collect()
                    };

                let mut transactions: Transactions =
                    self.load_metadata(&tx, Self::TRANSACTIONS).await?;

                let Some(transaction) = transactions.get_mut(&transaction_id) else {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::TransactionalIdNotFound),
                    ));
                };

                if transaction.producer != producer_id {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::UnknownProducerId),
                    ));
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::ProducerFenced),
                    ));
                };

                if &producer_epoch != current_epoch.key() {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::ProducerFenced),
                    ));
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
                            .insert(*partition_index, None);

                        results_by_partition.push(
                            AddPartitionsToTxnPartitionResult::default()
                                .partition_index(*partition_index)
                                .partition_error_code(ErrorCode::None.into()),
                        );
                    }
                    results.push(
                        AddPartitionsToTxnTopicResult::default()
                            .name(topic.name.clone())
                            .results_by_partition(Some(results_by_partition)),
                    );
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

                tx.commit().await.map_err(Error::from)?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
            }

            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                use jansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnResult;

                let tx = self
                    .db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .inspect_err(|err| debug!(?err))?;

                let mut stored_transactions: Transactions =
                    self.load_metadata(&tx, Self::TRANSACTIONS).await?;

                let mut results = Vec::with_capacity(transactions.len());

                for txn_request in transactions {
                    let transaction_id = txn_request.transactional_id.clone();
                    let producer_id = txn_request.producer_id;
                    let producer_epoch = txn_request.producer_epoch;
                    let topics = txn_request.topics.as_deref().unwrap_or(&[]);

                    let make_topic_results =
                        |error_code: ErrorCode| -> Vec<AddPartitionsToTxnTopicResult> {
                            topics
                                .iter()
                                .map(|topic| {
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(
                                            topic
                                                .partitions
                                                .as_deref()
                                                .unwrap_or(&[])
                                                .iter()
                                                .map(|p| {
                                                    AddPartitionsToTxnPartitionResult::default()
                                                        .partition_index(*p)
                                                        .partition_error_code(error_code.into())
                                                })
                                                .collect(),
                                        ))
                                })
                                .collect()
                        };

                    let topic_results = if let Some(transaction) =
                        stored_transactions.get_mut(&transaction_id)
                    {
                        if transaction.producer != producer_id {
                            make_topic_results(ErrorCode::UnknownProducerId)
                        } else if let Some(mut current_epoch) = transaction.epochs.last_entry() {
                            if &producer_epoch != current_epoch.key() {
                                make_topic_results(ErrorCode::ProducerFenced)
                            } else {
                                // Success - add partitions
                                let txn_detail = current_epoch.get_mut();
                                topics
                                    .iter()
                                    .map(|topic| {
                                        let partition_results: Vec<_> = topic
                                            .partitions
                                            .as_deref()
                                            .unwrap_or(&[])
                                            .iter()
                                            .map(|p| {
                                                _ = txn_detail
                                                    .produces
                                                    .entry(topic.name.clone())
                                                    .or_default()
                                                    .insert(*p, None);

                                                AddPartitionsToTxnPartitionResult::default()
                                                    .partition_index(*p)
                                                    .partition_error_code(ErrorCode::None.into())
                                            })
                                            .collect();

                                        AddPartitionsToTxnTopicResult::default()
                                            .name(topic.name.clone())
                                            .results_by_partition(Some(partition_results))
                                    })
                                    .collect()
                            }
                        } else {
                            // No epoch found
                            make_topic_results(ErrorCode::ProducerFenced)
                        }
                    } else {
                        // Transaction not found
                        make_topic_results(ErrorCode::TransactionalIdNotFound)
                    };

                    results.push(
                        AddPartitionsToTxnResult::default()
                            .transactional_id(transaction_id)
                            .topic_results(Some(topic_results)),
                    );
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &stored_transactions)?;

                tx.commit().await.map_err(Error::from)?;

                Ok(TxnAddPartitionsResponse::VersionFourPlus(results))
            }
        }
    }

    pub(super) async fn impl_txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut transactions: Transactions = self.load_metadata(&tx, Self::TRANSACTIONS).await?;

        let error_response = |error_code: ErrorCode| -> Vec<TxnOffsetCommitResponseTopic> {
            offsets
                .topics
                .iter()
                .map(|topic| {
                    TxnOffsetCommitResponseTopic::default()
                        .name(topic.name.clone())
                        .partitions(Some(
                            topic
                                .partitions
                                .as_deref()
                                .unwrap_or(&[])
                                .iter()
                                .map(|p| {
                                    TxnOffsetCommitResponsePartition::default()
                                        .partition_index(p.partition_index)
                                        .error_code(error_code.into())
                                })
                                .collect(),
                        ))
                })
                .collect()
        };

        let Some(transaction) = transactions.get_mut(&offsets.transaction_id) else {
            return Ok(error_response(ErrorCode::TransactionalIdNotFound));
        };

        if transaction.producer != offsets.producer_id {
            return Ok(error_response(ErrorCode::UnknownProducerId));
        }

        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
            return Ok(error_response(ErrorCode::ProducerFenced));
        };

        if &offsets.producer_epoch != current_epoch.key() {
            return Ok(error_response(ErrorCode::ProducerFenced));
        }

        let txn_detail = current_epoch.get_mut();
        let now = SystemTime::now();
        let expires_at = now.checked_add(DEFAULT_OFFSET_RETENTION);
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    _ = txn_detail
                        .offsets
                        .entry(offsets.group_id.clone())
                        .or_default()
                        .entry(topic.name.clone())
                        .or_default()
                        .insert(
                            partition.partition_index,
                            super::types::TxnCommitOffset {
                                committed_offset: partition.committed_offset,
                                leader_epoch: partition.committed_leader_epoch,
                                metadata: partition.committed_metadata.clone(),
                                commit_timestamp: Some(now),
                                expires_at,
                            },
                        );

                    partition_responses.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(ErrorCode::None.into()),
                    );
                }
            }

            responses.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        postcard::to_stdvec(&transactions)
            .map_err(Error::from)
            .and_then(|encoded| tx.put(Self::TRANSACTIONS, encoded).map_err(Into::into))?;

        tx.commit().await.map_err(Error::from)?;

        Ok(responses)
    }
}
