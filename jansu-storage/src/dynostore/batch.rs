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

//! Batch/transaction operations for DynoStore.

use super::*;

impl DynoStore {
    pub(super) async fn init_producer_inner(
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
                                        Ok(InitProducer::Completed(ProducerIdResponse {
                                            id: -1,
                                            epoch: -1,
                                            error: ErrorCode::UnknownServerError,
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

                        (Some(producer_id), Some(producer_epoch)) => {
                            if let Some(pd) = meta.producers.get_mut(&producer_id) {
                                let current_epoch = pd.sequences.last_key_value().map(|(k, _)| *k).unwrap_or(0);
                                if producer_epoch != current_epoch {
                                    Ok(ProducerIdResponse {
                                        id: -1,
                                        epoch: -1,
                                        error: ErrorCode::ProducerFenced,
                                    })
                                } else {
                                    let new_epoch = if current_epoch == i16::MAX { 0 } else { current_epoch + 1 };
                                    assert_eq!(None, pd.sequences.insert(new_epoch, BTreeMap::new()));
                                    Ok(ProducerIdResponse {
                                        id: producer_id,
                                        epoch: new_epoch,
                                        ..Default::default()
                                    })
                                }
                            } else {
                                Ok(ProducerIdResponse {
                                    id: -1,
                                    epoch: -1,
                                    error: ErrorCode::UnknownProducerId,
                                })
                            }
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

    pub(super) async fn txn_add_offsets_inner(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    pub(super) async fn txn_add_partitions_inner(
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

                                for partition_index in topic.partitions.iter().flat_map(|p| p.iter()) {
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

                                for partition_index in topic.partitions.iter().flat_map(|p| p.iter()) {
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

                                for partition_index in topic.partitions.iter().flat_map(|p| p.iter()) {
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

                                for partition_index in topic.partitions.iter().flat_map(|p| p.iter()) {
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

                            for partition_index in topic.partitions.iter().flat_map(|p| p.iter()) {
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
                use jansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnResult;

                self.meta
                    .with_mut(&self.object_store, |meta| {
                        let mut results = Vec::with_capacity(transactions.len());

                        for txn_request in &transactions {
                            let transaction_id = txn_request.transactional_id.clone();
                            let producer_id = txn_request.producer_id;
                            let producer_epoch = txn_request.producer_epoch;
                            let verify_only = txn_request.verify_only;
                            let topics = txn_request.topics.as_deref().unwrap_or(&[]);

                            let make_topic_results = |error_code: ErrorCode| -> Vec<AddPartitionsToTxnTopicResult> {
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

                            let topic_results = if let Some(transaction) = meta.transactions.get_mut(&transaction_id) {
                                if transaction.producer != producer_id {
                                    make_topic_results(ErrorCode::UnknownProducerId)
                                } else if let Some(mut current_epoch) = transaction.epochs.last_entry() {
                                    if &producer_epoch != current_epoch.key() {
                                        make_topic_results(ErrorCode::ProducerFenced)
                                    } else {
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
                                                        if verify_only {
                                                            let found = txn_detail
                                                                .produces
                                                                .get(&topic.name)
                                                                .is_some_and(|partitions| partitions.contains_key(p));
                                                            
                                                            AddPartitionsToTxnPartitionResult::default()
                                                                .partition_index(*p)
                                                                .partition_error_code(
                                                                    if found { ErrorCode::None.into() } else { ErrorCode::InvalidTxnState.into() }
                                                                )
                                                        } else {
                                                            _ = txn_detail
                                                                .produces
                                                                .entry(topic.name.clone())
                                                                .or_default()
                                                                .entry(*p)
                                                                .or_default();
                                                                
                                                            AddPartitionsToTxnPartitionResult::default()
                                                                .partition_index(*p)
                                                                .partition_error_code(ErrorCode::None.into())
                                                        }
                                                    })
                                                    .collect();

                                                AddPartitionsToTxnTopicResult::default()
                                                    .name(topic.name.clone())
                                                    .results_by_partition(Some(partition_results))
                                            })
                                            .collect()
                                    }
                                } else {
                                    make_topic_results(ErrorCode::ProducerFenced)
                                }
                            } else {
                                make_topic_results(ErrorCode::TransactionalIdNotFound)
                            };

                            if !verify_only {
                                if let Some(transaction) = meta.transactions.get_mut(&transaction_id) {
                                    if let Some(mut current_epoch) = transaction.epochs.last_entry() {
                                        if &producer_epoch == current_epoch.key() {
                                            let txn_detail = current_epoch.get_mut();
                                            txn_detail.started_at = Some(SystemTime::now());
                                            txn_detail.state = Some(TxnState::Begin);
                                        }
                                    }
                                }
                            }

                            results.push(
                                AddPartitionsToTxnResult::default()
                                    .transactional_id(transaction_id)
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
