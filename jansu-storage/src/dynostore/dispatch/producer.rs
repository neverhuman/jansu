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

//! Producer initialization dispatch.
//!
//! Inherent `DynoStore` methods backing the `Storage` trait implementation.

use super::*;

impl DynoStore {
    pub(crate) async fn init_producer_dispatch(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        #[derive(Clone, Debug)]
        enum InitProducer {
            Completed(ProducerIdResponse),
            NeedToRollback {
                producer_id: i64,
                producer_epoch: i16,
            },
        }

        if let Some(transaction_id) = transaction_id {
            match self
                .meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            match meta.transactions.entry(transaction_id.to_string()) {
                                Entry::Vacant(vacant) => {
                                    let id = meta
                                        .producers
                                        .last_key_value()
                                        .map_or(1.into(), |(k, _v)| k + 1);

                                    let mut pd = ProducerDetail::default();
                                    assert_eq!(None, pd.sequences.insert(0, BTreeMap::new()));
                                    assert_eq!(None, meta.producers.insert(id, pd));

                                    let mut epochs = BTreeMap::new();
                                    assert_eq!(
                                        None,
                                        epochs.insert(
                                            0,
                                            TxnDetail {
                                                transaction_timeout_ms,
                                                ..Default::default()
                                            },
                                        )
                                    );

                                    _ = vacant.insert(Txn {
                                        producer: id,
                                        epochs,
                                    });

                                    Ok(InitProducer::Completed(ProducerIdResponse {
                                        id,
                                        epoch: 0,
                                        error: ErrorCode::None,
                                    }))
                                }

                                Entry::Occupied(mut occupied) => {
                                    if let Some((current_epoch, txn_detail)) =
                                        occupied.get().epochs.last_key_value()
                                    {
                                        if txn_detail.state == Some(TxnState::Begin) {
                                            Ok(InitProducer::NeedToRollback {
                                                producer_id: occupied.get().producer,
                                                producer_epoch: *current_epoch,
                                            })
                                        } else {
                                            let id = occupied.get().producer;
                                            let epoch = current_epoch + 1;

                                            _ = meta.producers.entry(id).and_modify(|pd| {
                                                assert_eq!(
                                                    None,
                                                    pd.sequences.insert(epoch, BTreeMap::new())
                                                );
                                            });

                                            assert_eq!(
                                                None,
                                                occupied.get_mut().epochs.insert(
                                                    epoch,
                                                    TxnDetail {
                                                        transaction_timeout_ms,
                                                        ..Default::default()
                                                    }
                                                )
                                            );

                                            Ok(InitProducer::Completed(ProducerIdResponse {
                                                id,
                                                epoch,
                                                error: ErrorCode::None,
                                            }))
                                        }
                                    } else {
                                        let id = occupied.get().producer;
                                        let epoch = 0;
                                        _ = occupied.get_mut().epochs.insert(
                                            epoch,
                                            TxnDetail {
                                                transaction_timeout_ms,
                                                ..Default::default()
                                            },
                                        );
                                        Ok(InitProducer::Completed(ProducerIdResponse {
                                            id,
                                            epoch,
                                            error: ErrorCode::None,
                                        }))
                                    }
                                }
                            }
                        }

                        (producer, epoch) => {
                            error!(?producer, ?epoch);
                            Ok(InitProducer::Completed(ProducerIdResponse {
                                id: -1,
                                epoch: -1,
                                error: ErrorCode::UnknownServerError,
                            }))
                        }
                    }
                })
                .await?
            {
                InitProducer::Completed(completed) => Ok(completed),
                InitProducer::NeedToRollback {
                    producer_id: rollback_producer_id,
                    producer_epoch: rollback_producer_epoch,
                } => {
                    let error_code = self
                        .txn_end(
                            transaction_id,
                            rollback_producer_id,
                            rollback_producer_epoch,
                            false,
                        )
                        .await?;

                    debug!(?rollback_producer_id, ?rollback_producer_epoch, ?error_code);

                    if error_code == ErrorCode::None {
                        return self
                            .init_producer(
                                Some(transaction_id),
                                transaction_timeout_ms,
                                producer_id,
                                producer_epoch,
                            )
                            .await;
                    } else {
                        Ok(ProducerIdResponse {
                            id: -1,
                            epoch: -1,
                            error: ErrorCode::UnknownServerError,
                        })
                    }
                }
            }
        } else {
            self.meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            let producer = meta
                                .producers
                                .last_key_value()
                                .map_or(1.into(), |(k, _v)| k + 1);

                            let epoch = 0;
                            let mut pd = ProducerDetail::default();
                            assert_eq!(None, pd.sequences.insert(epoch, BTreeMap::new()));
                            debug!(?producer, ?pd);
                            assert_eq!(None, meta.producers.insert(producer, pd));

                            Ok(ProducerIdResponse {
                                id: producer,
                                epoch,
                                ..Default::default()
                            })
                        }

                        (producer, epoch) => {
                            error!(?producer, ?epoch);
                            Ok(ProducerIdResponse {
                                id: -1,
                                epoch: -1,
                                error: ErrorCode::UnknownServerError,
                            })
                        }
                    }
                })
                .await
        }
    }
}
