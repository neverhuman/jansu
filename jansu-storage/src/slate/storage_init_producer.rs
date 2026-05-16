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

//! init_producer Storage impl helper

use std::{collections::BTreeMap, time::SystemTime};

use bytes::{Bytes, BytesMut};
use jansu_sans_io::{
    BatchAttribute, ControlBatch, EndTransactionMarker, ErrorCode,
    record::{Record, deflated::Batch, inflated::Batch as InflatedBatch},
    ser::RecordBatchEncoder,
};
use serde::Serialize;
use tracing::debug;

use crate::{Error, ProducerIdResponse, Result, TxnState};

use super::engine::Engine;
use super::types::{
    BatchKey, Producers, Transactions, TxnDetail, Txn, Watermark, WatermarkKey,
};

impl Engine {
    pub(super) async fn impl_init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        if let Some(transaction_id) = transaction_id {
            // Transactional producer initialization
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            let mut transactions: Transactions =
                self.load_metadata(&tx, Self::TRANSACTIONS).await?;
            let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;

            // Check if transaction already exists
            if transactions.contains_key(transaction_id) {
                let existing_txn = transactions.get_mut(transaction_id).unwrap();
                let producer_id = existing_txn.producer;

                // Check if there's an active epoch that needs to be aborted
                let (old_epoch, needs_abort) = existing_txn
                    .epochs
                    .last_key_value()
                    .map(|(e, detail)| (*e, detail.state == Some(TxnState::Begin)))
                    .unwrap_or((0, false));

                // If prior epoch is in Begin state, we need to abort it
                if needs_abort && let Some(old_detail) = existing_txn.epochs.get_mut(&old_epoch) {
                    // Write abort markers for all partitions this transaction produced to
                    let topics = self.get_topics().await?;
                    for (topic_name, partitions) in &old_detail.produces {
                        for partition in partitions.keys() {
                            let Some(metadata) = topics.get(topic_name.as_str()) else {
                                continue;
                            };

                            // Create abort marker batch
                            let control_batch: Bytes =
                                ControlBatch::default().abort().try_into()?;
                            let end_transaction_marker: Bytes =
                                EndTransactionMarker::default().try_into()?;

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
                                .producer_epoch(old_epoch)
                                .base_sequence(-1)
                                .build()
                                .and_then(TryInto::try_into)?;

                            // Get current watermark and increment it
                            let watermark_key =
                                postcard::to_stdvec(&WatermarkKey::new(metadata.id, *partition))?;
                            let mut watermark =
                                tx.get(&watermark_key).await.map_err(Error::from).and_then(
                                    |watermark| {
                                        watermark.map_or(Ok(Watermark::default()), |encoded| {
                                            postcard::from_bytes(&encoded[..]).map_err(Into::into)
                                        })
                                    },
                                )?;

                            let offset = watermark.high.unwrap_or_default();

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

                            let batch_key = postcard::to_stdvec(&BatchKey::new(
                                metadata.id,
                                *partition,
                                offset,
                            ))?;
                            tx.put(batch_key, &encoded[..])?;

                            // Save updated watermark
                            let watermark_value = postcard::to_stdvec(&watermark)?;
                            tx.put(watermark_key, watermark_value)?;
                        }
                    }

                    // Mark prior epoch as aborted
                    old_detail.state = Some(TxnState::Aborted);
                }

                // Bump epoch for existing transaction
                let new_epoch = old_epoch + 1;

                // Re-get mutable reference after potential modification
                let existing_txn = transactions.get_mut(transaction_id).unwrap();
                _ = existing_txn.epochs.insert(
                    new_epoch,
                    TxnDetail {
                        transaction_timeout_ms,
                        started_at: Some(SystemTime::now()),
                        state: Some(TxnState::Begin),
                        ..Default::default()
                    },
                );

                // Also update producer's sequences with the new epoch
                if let Some(producer_detail) = producers.get_mut(&producer_id) {
                    _ = producer_detail.sequences.insert(new_epoch, BTreeMap::new());
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;
                self.save_metadata(&tx, Self::PRODUCERS, &producers)?;

                tx.commit().await.map_err(Error::from)?;

                return Ok(ProducerIdResponse {
                    id: producer_id,
                    epoch: new_epoch,
                    ..Default::default()
                });
            }

            // Create new transactional producer
            let new_producer_id = producers.last_key_value().map_or(1, |(k, _)| k + 1);
            let epoch = 0i16;

            let mut pd = super::types::ProducerDetail::default();
            _ = pd.sequences.insert(epoch, BTreeMap::new());
            _ = producers.insert(new_producer_id, pd);

            let mut txn = Txn {
                producer: new_producer_id,
                epochs: BTreeMap::new(),
            };
            _ = txn.epochs.insert(
                epoch,
                TxnDetail {
                    transaction_timeout_ms,
                    started_at: Some(SystemTime::now()),
                    state: Some(TxnState::Begin),
                    ..Default::default()
                },
            );
            _ = transactions.insert(transaction_id.to_string(), txn);

            self.save_metadata(&tx, Self::PRODUCERS, &producers)?;
            self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

            tx.commit().await.map_err(Error::from)?;

            Ok(ProducerIdResponse {
                id: new_producer_id,
                epoch,
                ..Default::default()
            })
        } else if Some(-1) == producer_id && Some(-1) == producer_epoch {
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;

            let producer = producers.last_key_value().map_or(1.into(), |(k, _v)| k + 1);

            let epoch = 0;
            let mut pd = super::types::ProducerDetail::default();
            _ = pd.sequences.insert(epoch, BTreeMap::new());
            debug!(?producer, ?pd);
            _ = producers.insert(producer, pd);

            self.save_metadata(&tx, Self::PRODUCERS, &producers)?;

            tx.commit()
                .await
                .map_err(Error::from)
                .and(Ok(ProducerIdResponse {
                    id: producer,
                    epoch,
                    ..Default::default()
                }))
        } else {
            Ok(ProducerIdResponse {
                id: -1,
                epoch: -1,
                error: ErrorCode::UnknownServerError,
            })
        }
    }
}
