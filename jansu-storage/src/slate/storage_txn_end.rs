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

//! txn_end Storage impl helper

use bytes::{Bytes, BytesMut};
use jansu_sans_io::{
    BatchAttribute, ControlBatch, EndTransactionMarker, ErrorCode,
    record::{Record, deflated::Batch, inflated::Batch as InflatedBatch},
    ser::RecordBatchEncoder,
};
use serde::Serialize;
use tracing::debug;

use crate::{Error, Result, TxnState};

use super::engine::Engine;
use super::types::{
    BatchKey, OffsetCommitKey, OffsetCommitValue, Transactions, Watermark, WatermarkKey,
};

impl Engine {
    pub(super) async fn impl_txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut transactions: Transactions = self.load_metadata(&tx, Self::TRANSACTIONS).await?;

        // First, validate the transaction and collect necessary information
        let (current_produces, current_offsets) = {
            let Some(transaction) = transactions.get_mut(transaction_id) else {
                return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
            };

            if transaction.producer != producer_id {
                return Err(Error::Api(ErrorCode::UnknownProducerId));
            }

            let Some(mut current_epoch_entry) = transaction.epochs.last_entry() else {
                return Err(Error::Api(ErrorCode::ProducerFenced));
            };

            if &producer_epoch != current_epoch_entry.key() {
                return Err(Error::Api(ErrorCode::ProducerFenced));
            }

            let txn_detail = current_epoch_entry.get_mut();

            if txn_detail.state == Some(TxnState::Begin) {
                txn_detail.state = Some(if committed {
                    TxnState::PrepareCommit
                } else {
                    TxnState::PrepareAbort
                });
            }

            // Clone the produces and offsets for later use
            let produces = txn_detail.produces.clone();
            let offsets = txn_detail.offsets.clone();

            (produces, offsets)
        };

        // Produce commit/abort marker batches for each partition
        // Track the maximum offset after producing control batches (for overlap detection)
        let topics = self.get_topics().await?;
        let mut current_offset_end: i64 = 0;

        for (topic_name, partitions) in &current_produces {
            for partition in partitions.keys() {
                let Some(metadata) = topics.get(topic_name.as_str()) else {
                    continue;
                };

                let topition = crate::Topition::new(topic_name.clone(), *partition);

                // Create the control batch marker
                let control_batch: Bytes = if committed {
                    ControlBatch::default().commit().try_into()?
                } else {
                    ControlBatch::default().abort().try_into()?
                };
                let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

                let batch: Batch = InflatedBatch::builder()
                    .record(
                        Record::builder()
                            .key(control_batch.into())
                            .value(end_transaction_marker.into()),
                    )
                    .attributes(
                        BatchAttribute::default()
                            .control(true)
                            .transaction(true)
                            .into(),
                    )
                    .producer_id(producer_id)
                    .producer_epoch(producer_epoch)
                    .base_sequence(-1)
                    .build()
                    .and_then(TryInto::try_into)?;

                // Get current watermark and increment it
                let watermark_key =
                    postcard::to_stdvec(&WatermarkKey::new(metadata.id, *partition))?;
                let mut watermark =
                    tx.get(&watermark_key)
                        .await
                        .map_err(Error::from)
                        .and_then(|watermark| {
                            watermark.map_or(Ok(Watermark::default()), |encoded| {
                                postcard::from_bytes(&encoded[..]).map_err(Into::into)
                            })
                        })?;

                let offset = watermark.high.unwrap_or(0_i64);

                // Track the control batch offset (this is the new offset_end for overlap detection)
                current_offset_end = current_offset_end.max(offset);

                watermark.high = watermark
                    .high
                    .map_or(Some(batch.last_offset_delta as i64 + 1i64), |high| {
                        Some(high + batch.last_offset_delta as i64 + 1i64)
                    });

                _ = watermark
                    .timestamps
                    .get_or_insert_default()
                    .insert(batch.base_timestamp, offset);

                // Encode and store the batch
                let encoded = {
                    let mut encoder = RecordBatchEncoder::new(BytesMut::new());
                    batch.serialize(&mut encoder)?;
                    Bytes::from(encoder)
                };

                let batch_key =
                    postcard::to_stdvec(&BatchKey::new(metadata.id, topition.partition, offset))?;
                tx.put(batch_key, &encoded[..])?;

                // Save updated watermark
                let watermark_value = postcard::to_stdvec(&watermark)?;
                tx.put(watermark_key, watermark_value)?;
            }
        }

        // Check for overlapping transactions and collect prepared ones
        // An overlapping transaction is one whose offset range intersects with this transaction
        let mut prepared_overlaps: Vec<(String, i16)> = vec![]; // (transaction_id, epoch)
        let mut has_unprepared_overlap = false;

        for (other_txn_id, other_txn) in transactions.iter() {
            if other_txn_id == transaction_id {
                continue;
            }

            for (other_epoch, other_detail) in &other_txn.epochs {
                // Check if there's any partition overlap where other's offset_start < current's offset_end
                let has_overlap = other_detail
                    .produces
                    .iter()
                    .any(|(topic, other_partitions)| {
                        if let Some(current_partitions) = current_produces.get(topic) {
                            other_partitions.iter().any(|(partition, other_range)| {
                                if current_partitions.contains_key(partition)
                                    && let Some(range) = other_range
                                {
                                    return range.offset_start < current_offset_end;
                                }
                                false
                            })
                        } else {
                            false
                        }
                    });

                if has_overlap {
                    match other_detail.state {
                        Some(TxnState::Begin) => {
                            has_unprepared_overlap = true;
                        }
                        Some(TxnState::PrepareCommit) | Some(TxnState::PrepareAbort) => {
                            prepared_overlaps.push((other_txn_id.clone(), *other_epoch));
                        }
                        _ => {}
                    }
                }
            }
        }

        // If there are unprepared overlapping transactions, only mark as PrepareCommit/PrepareAbort
        // Otherwise, complete all prepared overlapping transactions and the current transaction
        if !has_unprepared_overlap {
            // First, apply offset commits for the current transaction
            if committed {
                for (group_id, group_topics) in &current_offsets {
                    for (topic_name, partitions) in group_topics {
                        for (partition, commit_offset) in partitions {
                            let key = postcard::to_stdvec(&OffsetCommitKey::new(
                                group_id, topic_name, *partition,
                            ))?;

                            let value = postcard::to_stdvec(&OffsetCommitValue {
                                offset: commit_offset.committed_offset,
                                leader_epoch: commit_offset.leader_epoch,
                                metadata: commit_offset.metadata.clone(),
                                commit_timestamp: commit_offset.commit_timestamp,
                                expires_at: commit_offset.expires_at,
                            })?;

                            tx.put(key, value)?;
                        }
                    }
                }
            }

            // Mark current transaction as complete
            {
                let transaction = transactions.get_mut(transaction_id).unwrap();
                let txn_detail = transaction.epochs.get_mut(&producer_epoch).unwrap();
                txn_detail.state = Some(if committed {
                    TxnState::Committed
                } else {
                    TxnState::Aborted
                });
            }

            // Also complete all prepared overlapping transactions
            for (overlap_txn_id, overlap_epoch) in &prepared_overlaps {
                let Some(overlap_txn) = transactions.get_mut(overlap_txn_id) else {
                    continue;
                };
                let Some(overlap_detail) = overlap_txn.epochs.get_mut(overlap_epoch) else {
                    continue;
                };

                // Apply offset commits for PrepareCommit transactions
                if overlap_detail.state == Some(TxnState::PrepareCommit) {
                    for (group_id, group_topics) in &overlap_detail.offsets {
                        for (topic_name, partitions) in group_topics {
                            for (partition, commit_offset) in partitions {
                                let key = postcard::to_stdvec(&OffsetCommitKey::new(
                                    group_id, topic_name, *partition,
                                ))?;

                                let value = postcard::to_stdvec(&OffsetCommitValue {
                                    offset: commit_offset.committed_offset,
                                    leader_epoch: commit_offset.leader_epoch,
                                    metadata: commit_offset.metadata.clone(),
                                    commit_timestamp: commit_offset.commit_timestamp,
                                    expires_at: commit_offset.expires_at,
                                })?;

                                tx.put(key, value)?;
                            }
                        }
                    }
                }

                // Update state to final
                overlap_detail.state =
                    Some(if overlap_detail.state == Some(TxnState::PrepareCommit) {
                        TxnState::Committed
                    } else {
                        TxnState::Aborted
                    });
            }
        }

        self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

        tx.commit().await.map_err(Error::from)?;

        Ok(ErrorCode::None)
    }
}
