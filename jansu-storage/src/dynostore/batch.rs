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
                                        todo!()
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

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                todo!()
            }
        }
    }

    pub(super) async fn txn_offset_commit_inner(
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

    pub(super) async fn txn_end_inner(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let produced = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let txn_detail = current_epoch.get_mut();

                let mut produced = vec![];

                if txn_detail.state == Some(TxnState::Begin) {
                    assert_eq!(
                        Some(TxnState::Begin),
                        txn_detail.state.replace(if committed {
                            TxnState::PrepareCommit
                        } else {
                            TxnState::PrepareAbort
                        })
                    );

                    for (topic, partitions) in &txn_detail.produces {
                        for (partition, offset_range) in partitions {
                            debug!(?topic, partition, ?offset_range);

                            if offset_range.is_some() {
                                produced.push(Topition::new(topic.to_owned(), *partition));
                            }
                        }
                    }
                }

                Ok(produced)
            })
            .await
            .inspect(|produced| debug!(?produced))
            .inspect_err(|err| error!(?err))?;

        for topition in produced {
            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };

            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = inflated::Batch::builder()
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
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?;

            _ = self
                .produce(Some(transaction_id), &topition, batch)
                .await
                .inspect(|offset| {
                    debug!(
                        offset,
                        ?topition,
                        producer_id,
                        producer_epoch,
                        transaction_id,
                        committed,
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        ?topition,
                        producer_id,
                        producer_epoch,
                        transaction_id,
                        committed,
                    )
                })?;
        }

        let offsets_to_commit = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let mut overlaps =
                    meta.overlapping_transactions(transaction_id, producer_id, producer_epoch)?;
                debug!(?overlaps);

                let mut offsets_to_commit: BTreeMap<
                    Group,
                    BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>,
                > = BTreeMap::new();

                if overlaps.iter().all(|txn_id| txn_id.state.is_prepared()) {
                    let txn_ids = {
                        overlaps.push(TxnId {
                            transaction: transaction_id.into(),
                            producer_id,
                            producer_epoch,
                            state: if committed {
                                TxnState::PrepareCommit
                            } else {
                                TxnState::PrepareAbort
                            },
                        });

                        overlaps
                    };

                    for txn_id in txn_ids {
                        debug!(?txn_id);

                        if let Some(txn) = meta.transactions.get_mut(txn_id.transaction.as_str())
                            && let Some(txn_detail) = txn.epochs.get_mut(&txn_id.producer_epoch)
                        {
                            debug!(?txn_detail);

                            match txn_detail.state {
                                None | Some(TxnState::PrepareCommit) => {
                                    _ = txn_detail.state.replace(TxnState::Committed);
                                }

                                Some(TxnState::PrepareAbort) => {
                                    _ = txn_detail.state.replace(TxnState::Aborted);
                                }

                                otherwise => {
                                    warn!(
                                        transaction = txn_id.transaction,
                                        producer = txn_id.producer_id,
                                        epoch = txn_id.producer_epoch,
                                        ?otherwise,
                                    );

                                    continue;
                                }
                            }

                            if txn_id.state == TxnState::PrepareCommit {
                                for (group, topics) in txn_detail.offsets.iter() {
                                    for (topic, partitions) in topics.iter() {
                                        for (partition, committed_offset) in partitions {
                                            _ = offsets_to_commit
                                                .entry(group.to_owned())
                                                .or_default()
                                                .entry(topic.to_owned())
                                                .or_default()
                                                .insert(*partition, committed_offset.to_owned());
                                        }
                                    }
                                }
                            }

                            txn_detail.produces.clear();
                            txn_detail.offsets.clear();
                            _ = txn_detail.started_at.take();
                        }
                    }
                }

                Ok(offsets_to_commit)
            })
            .await
            .inspect(|outcome| debug!(?outcome))
            .inspect_err(|err| error!(?err))?;

        debug!(?offsets_to_commit);

        for (group, topics) in offsets_to_commit.iter() {
            let mut offsets = vec![];

            for (topic, partitions) in topics.iter() {
                for (partition, txn_co) in partitions {
                    let tp = Topition::new(topic.to_owned(), *partition);
                    let ocr = OffsetCommitRequest {
                        offset: txn_co.committed_offset,
                        leader_epoch: txn_co.leader_epoch,
                        timestamp: txn_co.commit_timestamp,
                        metadata: txn_co.metadata.clone(),
                    };

                    offsets.push((tp, ocr));
                }
            }

            _ = self.offset_commit(group, None, &offsets[..]).await?;
        }

        Ok(ErrorCode::None)
    }

}
