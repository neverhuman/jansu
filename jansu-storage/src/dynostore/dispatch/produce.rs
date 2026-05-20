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

//! Produce dispatch.
//!
//! Inherent `DynoStore` methods backing the `Storage` trait implementation.

use super::*;

impl DynoStore {
    pub(crate) async fn produce_dispatch(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let config = self
            .describe_config(topition.topic(), ConfigResource::Topic, None)
            .await
            .inspect_err(|err| debug!(?err))?;

        if self.lake.is_some()
            && config
                .configs
                .as_ref()
                .map(|configs| {
                    configs
                        .iter()
                        .inspect(|config| debug!(?config))
                        .any(|config| {
                            config.name.as_str() == "jansu.lake.sink"
                                && config
                                    .value
                                    .as_deref()
                                    .and_then(|value| bool::from_str(value).ok())
                                    .unwrap_or(false)
                        })
                })
                .unwrap_or(false)
        {
            // Get watermark to calculate proper offset for lake sink
            let watermark = self.watermarks.lock().map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert_with(|| OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                    .to_owned()
            })?;

            let offset = watermark
                .with_mut(&self.object_store, |watermark| {
                    debug!(?watermark);

                    let offset = watermark.high.unwrap_or(0);
                    watermark.high = Some(offset + deflated.last_offset_delta as i64 + 1i64);

                    watermark.timestamps = None;

                    debug!(?watermark);

                    Ok(offset)
                })
                .await
                .inspect(|offset| debug!(offset, transaction_id, ?topition))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;

            self.meta
                .with_mut(&self.object_store, |meta| {
                    meta.record_leader_epoch_boundary(
                        topition,
                        deflated.partition_leader_epoch,
                        offset,
                    );
                    Ok(())
                })
                .await?;

            if let Some(ref registry) = self.schemas {
                let batch_attribute = BatchAttribute::try_from(deflated.attributes)
                    .inspect(|batch_attribute| debug!(?batch_attribute))
                    .inspect_err(|err| debug!(?err))?;

                if !batch_attribute.control {
                    let inflated = inflated::Batch::try_from(&deflated)
                        .inspect(|inflated| debug!(?inflated))
                        .inspect_err(|err| debug!(?err))?;

                    registry
                        .validate(topition.topic(), &inflated)
                        .await
                        .inspect(|validation| debug!(?validation))
                        .inspect_err(|err| debug!(?err))?;

                    if let Some(ref lake) = self.lake {
                        lake.store(
                            topition.topic(),
                            topition.partition(),
                            offset,
                            &inflated,
                            config,
                        )
                        .await
                        .inspect(|store| debug!(?store))
                        .inspect_err(|err| debug!(?err))?;
                    }
                }
            }

            // Wake `fetch_wait` waiters on this topition; see `fetch_wait`.
            self.produce_notifier(topition).notify_waiters();
            Ok(offset)
        } else {
            if deflated.is_idempotent() {
                self.meta
                .with_mut(&self.object_store, |meta| {
                    let Some(pd) = meta.producers.get_mut(&deflated.producer_id) else {
                        debug!(producer_id = deflated.producer_id, ?meta.producers);
                        return Err(Error::Api(ErrorCode::UnknownProducerId));
                    };

                    let Some(mut current) = pd.sequences.last_entry() else {
                        debug!(last_entry = ?pd.sequences.last_entry());
                        return Err(Error::Api(ErrorCode::UnknownServerError));
                    };

                    if current.key() != &deflated.producer_epoch {
                        debug!(current = ?current.key(), producer_epoch = deflated.producer_epoch);
                        return Err(Error::Api(ErrorCode::ProducerFenced));
                    }

                    let sequences = current.get_mut();
                    debug!(?sequences);

                    match sequences
                        .entry(topition.topic.clone())
                        .or_default()
                        .entry(topition.partition)
                        .or_default()
                    {
                        sequence if *sequence < deflated.base_sequence => {
                            debug!(?sequence, base_sequence = deflated.base_sequence);

                            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
                        }

                        sequence if *sequence > deflated.base_sequence => {
                            debug!(?sequence, base_sequence = deflated.base_sequence);

                            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
                        }

                        sequence => {
                            debug!(?sequence, delta = deflated.last_offset_delta + 1);

                            *sequence += deflated.last_offset_delta + 1;
                            Ok(())
                        }
                    }
                })
                .await
                .inspect(|outcome| debug!(transaction_id, ?topition, ?outcome))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
            }

            if let Some(ref registry) = self.schemas {
                let batch_attribute = BatchAttribute::try_from(deflated.attributes)
                    .inspect_err(|err| debug!(?err))?;

                if !batch_attribute.control {
                    let inflated =
                        inflated::Batch::try_from(&deflated).inspect_err(|err| debug!(?err))?;

                    registry
                        .validate(topition.topic(), &inflated)
                        .await
                        .inspect_err(|err| debug!(?err))?;
                }
            }

            let watermark = self.watermarks.lock().map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert_with(|| OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                    .to_owned()
            })?;

            let offset = watermark
                .with_mut(&self.object_store, |watermark| {
                    debug!(?watermark);

                    let offset = watermark.high.unwrap_or(0);
                    watermark.high = Some(offset + deflated.last_offset_delta as i64 + 1i64);

                    watermark.timestamps = None;

                    debug!(?watermark);

                    Ok(offset)
                })
                .await
                .inspect(|offset| debug!(offset, transaction_id, ?topition))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;

            self.meta
                .with_mut(&self.object_store, |meta| {
                    meta.record_leader_epoch_boundary(
                        topition,
                        deflated.partition_leader_epoch,
                        offset,
                    );
                    Ok(())
                })
                .await?;

            let attributes =
                BatchAttribute::try_from(deflated.attributes).inspect_err(|err| debug!(?err))?;

            if !attributes.control
                && let Some(ref lake) = self.lake
            {
                let inflated =
                    inflated::Batch::try_from(&deflated).inspect_err(|err| debug!(?err))?;

                lake.store(
                    topition.topic(),
                    topition.partition(),
                    offset,
                    &inflated,
                    config,
                )
                .await
                .inspect(|store| debug!(?store))
                .inspect_err(|err| debug!(?err))?;
            }

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        if let Some(transaction) = meta.transactions.get_mut(transaction_id) {
                            debug!(?transaction);

                            if let Some(txn_detail) =
                                transaction.epochs.get_mut(&deflated.producer_epoch)
                            {
                                debug!(?txn_detail);

                                let offset_end = offset + deflated.last_offset_delta as i64;

                                _ = txn_detail
                                    .produces
                                    .entry(topition.topic.clone())
                                    .or_default()
                                    .entry(topition.partition)
                                    .and_modify(|entry| {
                                        let range = entry.get_or_insert(TxnProduceOffset {
                                            offset_start: offset,
                                            offset_end,
                                        });

                                        if offset_end > range.offset_end {
                                            range.offset_end = offset_end;
                                        }
                                    })
                                    .or_insert(Some(TxnProduceOffset {
                                        offset_start: offset,
                                        offset_end,
                                    }));
                            }
                        }

                        Ok(())
                    })
                    .await
                    .inspect(|outcome| debug!(?outcome, transaction_id, ?topition))
                    .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
            }

            let location = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
                self.cluster, topition.topic, topition.partition, offset,
            ));

            let payload = self.encode(deflated).inspect_err(|err| debug!(?err))?;

            _ = self
                .object_store
                .put_opts(
                    &location,
                    payload,
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::new(),
                        ..Default::default()
                    },
                )
                .await
                .inspect(|outcome| debug!(?outcome, transaction_id, ?topition))
                .inspect_err(|error| error!(?error, transaction_id, ?topition))?;

            // Wake `fetch_wait` waiters on this topition; see `fetch_wait`.
            self.produce_notifier(topition).notify_waiters();
            Ok(offset)
        }
    }
}
