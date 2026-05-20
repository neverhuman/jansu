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

//! Record-level operations (delete records, fetch, offset stage) for the SlateDB engine.

use jansu_sans_io::{
    ErrorCode, IsolationLevel,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    record::deflated::Batch,
};
use tracing::debug;

use crate::{Error, OffsetStage, Result, Topition, TxnState};

use super::super::engine::Engine;
use super::super::types::{BatchKey, BatchKeyPrefix, Topics, Watermark, WatermarkKey};

impl Engine {
    pub(in crate::slate) async fn delete_records_op(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let all_topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let mut results = Vec::with_capacity(topics.len());

        for topic in topics {
            let mut partition_results = vec![];

            let topic_metadata = all_topics.get(&topic.name[..]);

            if let Some(partitions) = topic.partitions.as_ref() {
                for partition in partitions {
                    let (error_code, low_watermark) = if let Some(metadata) = topic_metadata {
                        if partition.partition_index < 0
                            || partition.partition_index >= metadata.topic.num_partitions
                        {
                            (ErrorCode::UnknownTopicOrPartition, 0)
                        } else {
                            // Delete batches below the specified offset
                            let batch_prefix = postcard::to_stdvec(&BatchKeyPrefix::new(
                                metadata.id,
                                partition.partition_index,
                            ))?;
                            let scan_start = postcard::to_stdvec(&BatchKey::scan_from(
                                metadata.id,
                                partition.partition_index,
                                0,
                            ))?;

                            let mut scan = self.db.scan(scan_start..).await?;
                            while let Some(kv) = scan.next().await? {
                                if !kv.key.starts_with(&batch_prefix) {
                                    break;
                                }

                                let batch_key: BatchKey = match postcard::from_bytes(&kv.key) {
                                    Ok(key) => key,
                                    Err(_) => continue,
                                };

                                // Delete batches with offset < specified offset
                                if batch_key.offset >= partition.offset {
                                    break;
                                }

                                tx.delete(&kv.key)?;
                            }

                            // The new low watermark is the requested offset
                            let new_low_watermark = partition.offset;

                            // Update the watermark
                            let watermark_key = postcard::to_stdvec(&WatermarkKey::new(
                                metadata.id,
                                partition.partition_index,
                            ))?;

                            let mut watermark =
                                tx.get(&watermark_key).await.map_err(Error::from).and_then(
                                    |watermark| {
                                        watermark.map_or(Ok(Watermark::default()), |encoded| {
                                            postcard::from_bytes(&encoded[..]).map_err(Into::into)
                                        })
                                    },
                                )?;

                            watermark.low = Some(new_low_watermark);

                            // Remove timestamps before the new low watermark
                            if let Some(ref mut timestamps) = watermark.timestamps {
                                timestamps.retain(|_, offset| *offset >= new_low_watermark);
                            }

                            let watermark_value = postcard::to_stdvec(&watermark)?;
                            tx.put(&watermark_key, watermark_value)?;

                            (ErrorCode::None, new_low_watermark)
                        }
                    } else {
                        (ErrorCode::UnknownTopicOrPartition, 0)
                    };

                    partition_results.push(
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(low_watermark)
                            .error_code(error_code.into()),
                    );
                }
            }

            results.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_results)),
            );
        }

        tx.commit().await.map_err(Error::from)?;

        Ok(results)
    }

    pub(in crate::slate) async fn fetch_op(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<Batch>> {
        // Get the high watermark based on isolation level
        let offset_stage = self.offset_stage_op(topition).await?;
        let high_watermark = if isolation_level == IsolationLevel::ReadCommitted {
            offset_stage.last_stable
        } else {
            offset_stage.high_watermark
        };

        debug!(
            ?isolation_level,
            high_watermark, offset, min_bytes, max_bytes
        );

        let topics = self
            .db
            .get(Self::TOPICS)
            .await
            .map_err(Error::from)
            .and_then(|topics| {
                topics.map_or(Ok(Topics::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let prefix = postcard::to_stdvec(&BatchKeyPrefix::new(metadata.id, topition.partition))?;

        let mut i = {
            let from = postcard::to_stdvec(&BatchKey::scan_from(
                metadata.id,
                topition.partition,
                offset,
            ))?;

            self.db.scan(from..).await?
        };

        let mut batches = vec![];
        let mut total_bytes: usize = 0;
        let min_bytes = min_bytes as usize;
        let max_bytes = max_bytes as usize;

        while let Some(kv) = i.next().await? {
            // Check if the key still belongs to the same topic/partition
            if !kv.key.starts_with(&prefix) {
                break;
            }

            let size = kv.value.len();

            let key: BatchKey = postcard::from_bytes(&kv.key)?;

            // Stop if we've reached the high watermark (respecting isolation level)
            if key.offset >= high_watermark {
                break;
            }

            // TODO: Performance - Avoid full decode
            // We decode the entire batch just to check size limits or return it.
            // For scanning/filtering, we should only decode the header or use a lightweight check.
            let mut batch = self.decode(kv.value)?;
            batch.base_offset = key.offset;
            batches.push(batch);
            total_bytes += size;

            // Stop if we've exceeded max_bytes (unless we haven't reached min_bytes yet)
            if total_bytes >= max_bytes
                || (total_bytes >= min_bytes && size > (max_bytes - total_bytes))
            {
                break;
            }
        }

        Ok(batches)
    }

    pub(in crate::slate) async fn offset_stage_op(
        &self,
        topition: &Topition,
    ) -> Result<OffsetStage> {
        let topics = self.get_topics().await?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let watermark_key =
            postcard::to_stdvec(&WatermarkKey::new(metadata.id, topition.partition))?;

        let watermark = self
            .db
            .get(&watermark_key)
            .await
            .map_err(Error::from)
            .and_then(|watermark| {
                watermark.map_or(Ok(Watermark::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let high_watermark = watermark.high.unwrap_or(0);

        // Calculate last_stable by finding the minimum offset of any in-progress transaction
        let transactions = self.get_transactions().await?;
        let mut last_stable = high_watermark;

        for txn in transactions.values() {
            for txn_detail in txn.epochs.values() {
                // Consider transactions that are in-progress (Begin, PrepareCommit, or PrepareAbort)
                // These states indicate the transaction is not yet fully committed/aborted
                let is_in_progress = matches!(
                    txn_detail.state,
                    Some(TxnState::Begin)
                        | Some(TxnState::PrepareCommit)
                        | Some(TxnState::PrepareAbort)
                );
                if is_in_progress {
                    // Check if this transaction has produced to this topic/partition
                    if let Some(partitions) = txn_detail.produces.get(&topition.topic)
                        && let Some(Some(offset_range)) = partitions.get(&topition.partition)
                    {
                        // The last_stable should be the minimum of all in-flight txn start offsets
                        last_stable = last_stable.min(offset_range.offset_start);
                    }
                }
            }
        }

        Ok(OffsetStage {
            log_start: watermark.low.unwrap_or(0),
            last_stable,
            high_watermark,
        })
    }
}
