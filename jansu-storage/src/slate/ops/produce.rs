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

//! Produce operation for the SlateDB engine.

use bytes::{Bytes, BytesMut};
use jansu_sans_io::{
    BatchAttribute, ConfigResource, ErrorCode,
    record::{deflated::Batch, inflated::Batch as InflatedBatch},
    ser::RecordBatchEncoder,
};
use jansu_schema::lake::LakeHouse as _;
use serde::Serialize;
use tracing::debug;

use crate::{Error, Result, Topition};

use super::super::engine::Engine;
use super::super::types::{
    BatchKey, LeaderEpochKey, LeaderEpochKeyPrefix, LeaderEpochValue, Producers, Topics,
    Transactions, TxnProduceOffset, Watermark, WatermarkKey,
};

impl Engine {
    pub(in crate::slate) async fn produce_op(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: Batch,
    ) -> Result<i64> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        // Schema validation (if schemas registry is configured)
        if let Some(ref schemas) = self.schemas {
            let inflated = InflatedBatch::try_from(deflated.clone())?;
            let attributes = BatchAttribute::try_from(inflated.attributes)?;

            // Only validate non-control batches
            if !attributes.control {
                schemas.validate(topition.topic(), &inflated).await?;
            }
        }

        // Idempotent message check
        if deflated.is_idempotent() {
            // NOTE: Contention Hotspot
            // Loading all producers to check/update sequence numbers prevents high-throughput idempotent production.
            // This map needs to be sharded or converted to per-producer keys.
            let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;

            let Some(producer_detail) = producers.get_mut(&deflated.producer_id) else {
                return Err(Error::Api(ErrorCode::UnknownProducerId));
            };

            // Get current epoch for this producer
            let Some(current_epoch) = producer_detail.sequences.last_key_value().map(|(e, _)| *e)
            else {
                return Err(Error::Api(ErrorCode::UnknownProducerId));
            };

            // Get current sequence for this topic/partition
            let current_sequence = producer_detail
                .sequences
                .get(&deflated.producer_epoch)
                .and_then(|topics| topics.get(&topition.topic))
                .and_then(|partitions| partitions.get(&topition.partition))
                .copied()
                .unwrap_or(0);

            debug!(
                producer_id = deflated.producer_id,
                producer_epoch = deflated.producer_epoch,
                current_epoch,
                current_sequence,
                base_sequence = deflated.base_sequence,
            );

            // Check sequence validity
            let increment =
                Self::idempotent_sequence_check(&current_epoch, &current_sequence, &deflated)?;

            // Update sequence
            _ = producer_detail
                .sequences
                .entry(deflated.producer_epoch)
                .or_default()
                .entry(topition.topic.clone())
                .or_default()
                .insert(topition.partition, current_sequence + increment);

            // Save updated producers
            self.save_metadata(&tx, Self::PRODUCERS, &producers)?;
        }

        let mut watermark = tx
            .get(postcard::to_stdvec(&WatermarkKey::new(
                metadata.id,
                topition.partition,
            ))?)
            .await
            .map_err(Error::from)
            .and_then(|watermark| {
                watermark.map_or(Ok(Watermark::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let offset = watermark.high.unwrap_or(0);
        let offset_end = offset + deflated.last_offset_delta as i64;
        let batch_leader_epoch = deflated.partition_leader_epoch;

        let leader_epoch_prefix =
            postcard::to_stdvec(&LeaderEpochKeyPrefix::new(metadata.id, topition.partition))?;
        let mut leader_epoch_scan = self.db.scan(leader_epoch_prefix.clone()..).await?;
        let mut current_epoch: Option<i32> = None;
        while let Some(kv) = leader_epoch_scan.next().await? {
            if !kv.key.starts_with(&leader_epoch_prefix) {
                break;
            }

            let key: LeaderEpochKey = postcard::from_bytes(&kv.key)?;
            current_epoch = Some(current_epoch.map_or(key.epoch, |current| current.max(key.epoch)));
        }

        if current_epoch.is_none_or(|current| batch_leader_epoch > current) {
            let key = postcard::to_stdvec(&LeaderEpochKey::new(
                metadata.id,
                topition.partition,
                batch_leader_epoch,
            ))?;
            let value = postcard::to_stdvec(&LeaderEpochValue {
                start_offset: offset,
            })?;
            tx.put(key, value)?;
        }

        // Handle transactional produce - update transaction state with offset range
        if let Some(transaction_id) = transaction_id {
            let mut transactions: Transactions =
                self.load_metadata(&tx, Self::TRANSACTIONS).await?;

            if let Some(txn) = transactions.get_mut(transaction_id)
                && let Some(txn_detail) = txn.epochs.get_mut(&deflated.producer_epoch)
            {
                // Get or create the partition entry in produces map
                let partition_entry = txn_detail
                    .produces
                    .entry(topition.topic.clone())
                    .or_default()
                    .entry(topition.partition)
                    .or_insert(None);

                // Update offset range - keep original offset_start if already set
                if let Some(existing) = partition_entry {
                    // Just update offset_end
                    existing.offset_end = offset_end;
                } else {
                    // First produce to this partition - set both start and end
                    *partition_entry = Some(TxnProduceOffset {
                        offset_start: offset,
                        offset_end,
                    });
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;
            }
        }

        watermark.high = watermark
            .high
            .map_or(Some(deflated.last_offset_delta as i64 + 1i64), |high| {
                Some(high + deflated.last_offset_delta as i64 + 1i64)
            });

        _ = watermark
            .timestamps
            .get_or_insert_default()
            .insert(deflated.base_timestamp, offset);

        debug!(?watermark);

        let encoded = {
            let mut encoder = RecordBatchEncoder::new(BytesMut::new());
            deflated.serialize(&mut encoder)?;

            Bytes::from(encoder)
        };

        let batch_key =
            postcard::to_stdvec(&BatchKey::new(metadata.id, topition.partition, offset))?;

        tx.put(batch_key, &encoded[..])?;

        // Also save the updated watermark
        let watermark_key =
            postcard::to_stdvec(&WatermarkKey::new(metadata.id, topition.partition))?;
        let watermark_value = postcard::to_stdvec(&watermark)?;
        tx.put(watermark_key, watermark_value)?;

        // Store to data lake if configured
        if let Some(ref lake) = self.lake {
            let inflated = InflatedBatch::try_from(deflated.clone())?;
            let attributes = BatchAttribute::try_from(inflated.attributes)?;

            if !attributes.control {
                // TODO: Optimization - Avoid synchronous call
                // Loading config and writing to lake synchronously inside the transaction critical path
                // increases latency and lock holding time. Consider moving this to an async background task.
                let config = self
                    .describe_config_op(topition.topic(), ConfigResource::Topic, None)
                    .await?;

                lake.store(
                    topition.topic(),
                    topition.partition(),
                    offset,
                    &inflated,
                    config,
                )
                .await?;
            }
        }

        tx.commit().await.map_err(Error::from).and(Ok(offset))
    }
}
