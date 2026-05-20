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

//! Transaction offset-commit dispatch.
//!
//! Inherent `DynoStore` methods backing the `Storage` trait implementation.

use super::*;

impl DynoStore {
    pub(crate) async fn txn_offset_commit_dispatch(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let now = SystemTime::now();
        let expires_at = now.checked_add(DEFAULT_OFFSET_RETENTION);

        self.meta
            .with_mut(&self.object_store, |meta| {
                let Some(transaction) = meta.transactions.get_mut(&offsets.transaction_id) else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::TransactionalIdNotFound,
                    );
                };

                if transaction.producer != offsets.producer_id {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::UnknownProducerId,
                    );
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                };

                if &offsets.producer_epoch != current_epoch.key() {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                }

                let txn_detail = current_epoch.get_mut();

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
                                    TxnCommitOffset {
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
                            .name(topic.name.to_string())
                            .partitions(Some(partition_responses)),
                    );
                }

                Ok(responses)
            })
            .await
    }
}
