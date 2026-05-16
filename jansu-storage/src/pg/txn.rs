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

//! Transaction management: init_producer, txn_add_*, txn_offset_commit, txn_end

use super::*;

impl Postgres {
    #[instrument(skip_all)]
    pub(super) async fn init_producer_storage(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch
        );

        if producer_id.is_some_and(|producer_id| producer_id == -1)
            && producer_epoch.is_some_and(|producer_epoch| producer_epoch == -1)
        {
            if let Some(transaction_id) = transaction_id {
                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                if let Some(row) = self
                    .tx_prepare_query_opt(
                        &tx,
                        "producer_epoch_for_current_txn.sql",
                        &[&self.cluster, &transaction_id],
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let id: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;
                    let epoch: i16 = row.try_get(1).inspect_err(|err| error!(?err))?;
                    let status = row
                        .try_get::<_, Option<String>>(2)
                        .inspect_err(|err| error!(?err))?
                        .map_or(Ok(None), |status| {
                            TxnState::from_str(status.as_str()).map(Some)
                        })?;

                    debug!(transaction_id, id, epoch, ?status);

                    if let Some(TxnState::Begin) = status {
                        let error = self
                            .end_in_tx(transaction_id, id, epoch, false, &tx)
                            .await?;

                        if error != ErrorCode::None {
                            _ = tx
                                .rollback()
                                .await
                                .inspect_err(|err| error!(?err, ?transaction_id, id, epoch));

                            return Ok(ProducerIdResponse { error, id, epoch });
                        }
                    }
                }

                let (producer, epoch) = if let Some(row) = self
                    .tx_prepare_query_opt(
                        &tx,
                        "txn_select_name.sql",
                        &[&self.cluster, &transaction_id],
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let producer: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;

                    let row = self
                        .tx_prepare_query_one(
                            &tx,
                            "producer_epoch_insert.sql",
                            &[&self.cluster, &producer],
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch: i16 = row.try_get(0)?;

                    (producer, epoch)
                } else {
                    let row = self
                        .tx_prepare_query_one(&tx, "producer_insert.sql", &[&self.cluster])
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let producer: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;

                    let row = self
                        .tx_prepare_query_one(
                            &tx,
                            "producer_epoch_insert.sql",
                            &[&self.cluster, &producer],
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch: i16 = row.try_get(0)?;

                    assert_eq!(
                        1,
                        self.tx_prepare_execute(
                            &tx,
                            "txn_insert.sql",
                            &[&self.cluster, &transaction_id, &producer],
                        )
                        .await
                        .inspect_err(|err| error!(
                            self.cluster,
                            transaction_id,
                            producer,
                            ?err
                        ))?
                    );

                    (producer, epoch)
                };

                debug!(transaction_id, producer, epoch);

                assert_eq!(
                    1,
                    self.tx_prepare_execute(
                        &tx,
                        "txn_detail_insert.sql",
                        &[
                            &transaction_timeout_ms,
                            &self.cluster,
                            &transaction_id,
                            &producer,
                            &epoch,
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(
                        self.cluster,
                        transaction_id,
                        producer,
                        epoch,
                        transaction_timeout_ms,
                        ?err
                    ))?
                );

                let error = match tx.commit().await.inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        transaction_id,
                        producer,
                        epoch
                    )
                }) {
                    Ok(()) => ErrorCode::None,
                    Err(_) => ErrorCode::UnknownServerError,
                };

                Ok(ProducerIdResponse {
                    error,
                    id: producer,
                    epoch,
                })
            } else {
                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let row = self
                    .tx_prepare_query_one(&tx, "producer_insert.sql", &[&self.cluster])
                    .await
                    .inspect_err(|err| error!(self.cluster, ?err))?;

                let producer: i64 = row.try_get(0)?;

                let row = self
                    .tx_prepare_query_one(
                        &tx,
                        "producer_epoch_insert.sql",
                        &[&self.cluster, &producer],
                    )
                    .await
                    .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                let epoch: i16 = row.try_get(0)?;

                let error = match tx
                    .commit()
                    .await
                    .inspect_err(|err| error!(?err, ?transaction_id, producer, epoch))
                {
                    Ok(()) => ErrorCode::None,
                    Err(_) => ErrorCode::UnknownServerError,
                };

                Ok(ProducerIdResponse {
                    error,
                    id: producer,
                    epoch,
                })
            }
        } else if let (Some(producer_id), Some(producer_epoch)) = (producer_id, producer_epoch) {
            let mut c = self.connection().await?;
            let tx = c.transaction().await?;

            let row = self
                .tx_prepare_query_one(
                    &tx,
                    "producer_epoch_max_select.sql",
                    &[&self.cluster, &producer_id],
                )
                .await
                .inspect_err(|err| error!(self.cluster, producer_id, ?err))?;

            let max_epoch: i16 = row.try_get(0)?;

            if max_epoch == -1 {
                // Producer not found
                return Ok(ProducerIdResponse {
                    error: ErrorCode::UnknownProducerId,
                    id: -1,
                    epoch: -1,
                });
            } else if max_epoch != producer_epoch {
                return Ok(ProducerIdResponse {
                    error: ErrorCode::ProducerFenced,
                    id: -1,
                    epoch: -1,
                });
            }

            // Valid, so we bump the epoch. The producer_epoch_insert.sql bumps automatically!
            let row = self
                .tx_prepare_query_one(
                    &tx,
                    "producer_epoch_insert.sql",
                    &[&self.cluster, &producer_id],
                )
                .await
                .inspect_err(|err| error!(self.cluster, producer_id, ?err))?;

            let new_epoch: i16 = row.try_get(0)?;

            let error = match tx
                .commit()
                .await
                .inspect_err(|err| error!(?err, producer_id, new_epoch))
            {
                Ok(()) => ErrorCode::None,
                Err(_) => ErrorCode::UnknownServerError,
            };

            Ok(ProducerIdResponse {
                error,
                id: producer_id,
                epoch: new_epoch,
            })
        } else {
            Ok(ProducerIdResponse {
                error: ErrorCode::UnknownServerError,
                id: -1,
                epoch: -1,
            })
        }
    }


    #[instrument(skip_all)]
    pub(super) async fn txn_add_offsets_storage(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None)
    }


    #[instrument(skip_all)]
    pub(super) async fn txn_add_partitions_storage(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        debug!(cluster = self.cluster, ?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                topics,
            } => {
                debug!(?transaction_id, ?producer_id, ?producer_epoch, ?topics);

                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.into_iter().flatten() {
                        _ = self
                            .tx_prepare_execute(
                                &tx,
                                "txn_topition_insert.sql",
                                &[
                                    &self.cluster,
                                    &topic.name,
                                    &partition_index,
                                    &transaction_id,
                                    &producer_id,
                                    &producer_epoch,
                                ],
                            )
                            .await
                            .inspect_err(|err| {
                                error!(
                                    ?err,
                                    cluster = self.cluster,
                                    topic = topic.name,
                                    ?partition_index,
                                    transaction_id
                                )
                            })?;

                        results_by_partition.push(
                            AddPartitionsToTxnPartitionResult::default()
                                .partition_index(partition_index)
                                .partition_error_code(i16::from(ErrorCode::None)),
                        );
                    }

                    results.push(
                        AddPartitionsToTxnTopicResult::default()
                            .name(topic.name)
                            .results_by_partition(Some(results_by_partition)),
                    )
                }

                _ = self
                    .tx_prepare_execute(
                        &tx,
                        "txn_detail_update_started_at.sql",
                        &[
                            &self.cluster,
                            &transaction_id,
                            &producer_id,
                            &producer_epoch,
                        ],
                    )
                    .await
                    .inspect_err(|err| {
                        error!(
                            ?err,
                            cluster = self.cluster,
                            transaction_id,
                            producer_id,
                            producer_epoch,
                        )
                    })?;

                tx.commit().await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
            }

            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                debug!(?transactions);

                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let mut results = vec![];

                for transaction in transactions {
                    let transaction_id = &transaction.transactional_id;
                    let producer_id = transaction.producer_id;
                    let producer_epoch = transaction.producer_epoch;
                    let verify_only = transaction.verify_only;

                    let mut topic_results = vec![];

                    for topic in transaction.topics.unwrap_or(vec![]) {
                        let mut results_by_partition = vec![];

                        for partition_index in topic.partitions.unwrap_or(vec![]) {
                            if verify_only {
                                let rows = self
                                    .tx_prepare_query(
                                        &tx,
                                        "txn_topition_select.sql",
                                        &[
                                            &self.cluster,
                                            &producer_id,
                                            &producer_epoch,
                                            &topic.name,
                                            &partition_index,
                                        ],
                                    )
                                    .await?;

                                if !rows.is_empty() {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(partition_index)
                                            .partition_error_code(i16::from(ErrorCode::None)),
                                    );
                                } else {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(partition_index)
                                            .partition_error_code(i16::from(ErrorCode::InvalidTxnState)),
                                    );
                                }
                            } else {
                                _ = self
                                    .tx_prepare_execute(
                                        &tx,
                                        "txn_topition_insert.sql",
                                        &[
                                            &self.cluster,
                                            &topic.name,
                                            &partition_index,
                                            &transaction_id,
                                            &producer_id,
                                            &producer_epoch,
                                        ],
                                    )
                                    .await
                                    .inspect_err(|err| {
                                        error!(
                                            ?err,
                                            cluster = self.cluster,
                                            topic = topic.name,
                                            ?partition_index,
                                            transaction_id
                                        )
                                    })?;

                                results_by_partition.push(
                                    AddPartitionsToTxnPartitionResult::default()
                                        .partition_index(partition_index)
                                        .partition_error_code(i16::from(ErrorCode::None)),
                                );
                            }
                        }

                        topic_results.push(
                            AddPartitionsToTxnTopicResult::default()
                                .name(topic.name)
                                .results_by_partition(Some(results_by_partition)),
                        )
                    }

                    if !verify_only {
                        _ = self
                            .tx_prepare_execute(
                                &tx,
                                "txn_detail_update_started_at.sql",
                                &[
                                    &self.cluster,
                                    &transaction_id,
                                    &producer_id,
                                    &producer_epoch,
                                ],
                            )
                            .await
                            .inspect_err(|err| {
                                error!(
                                    ?err,
                                    cluster = self.cluster,
                                    transaction_id,
                                    producer_id,
                                    producer_epoch,
                                )
                            })?;
                    }

                    results.push(
                        AddPartitionsToTxnResult::default()
                            .transactional_id(transaction_id.clone())
                            .topic_results(Some(topic_results)),
                    );
                }

                tx.commit().await?;

                Ok(TxnAddPartitionsResponse::VersionFourPlus(results))
            }
        }
    }


    #[instrument(skip_all)]
    pub(super) async fn txn_offset_commit_storage(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        debug!(cluster = self.cluster, ?offsets);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let (producer_id, producer_epoch) = if let Some(row) = self
            .tx_prepare_query_opt(
                &tx,
                "producer_epoch_for_current_txn.sql",
                &[&self.cluster, &offsets.transaction_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row
                .try_get::<_, i64>(0)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            let epoch = row
                .try_get::<_, i16>(1)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            (producer_id, epoch)
        } else {
            (None, None)
        };

        _ = self
            .tx_prepare_execute(
                &tx,
                "consumer_group_insert.sql",
                &[&self.cluster, &offsets.group_id],
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = self
            .tx_prepare_execute(
                &tx,
                "txn_offset_commit_insert.sql",
                &[
                    &self.cluster,
                    &offsets.transaction_id,
                    &offsets.group_id,
                    &offsets.producer_id,
                    &offsets.producer_epoch,
                    &offsets.generation_id,
                    &offsets.member_id,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let mut topics = vec![];

        for topic in offsets.topics {
            let mut partitions = vec![];

            for partition in topic.partitions.into_iter().flatten() {
                if producer_id.is_some_and(|producer_id| producer_id == offsets.producer_id) {
                    if producer_epoch
                        .is_some_and(|producer_epoch| producer_epoch == offsets.producer_epoch)
                    {
                        _ = self
                            .tx_prepare_execute(
                                &tx,
                                "txn_offset_commit_tp_insert.sql",
                                &[
                                    &self.cluster,
                                    &offsets.transaction_id,
                                    &offsets.group_id,
                                    &offsets.producer_id,
                                    &offsets.producer_epoch,
                                    &topic.name,
                                    &partition.partition_index,
                                    &partition.committed_offset,
                                    &partition.committed_leader_epoch,
                                    &partition.committed_metadata,
                                ],
                            )
                            .await
                            .inspect_err(|err| error!(?err))?;

                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::None)),
                        );
                    } else {
                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::InvalidProducerEpoch)),
                        );
                    }
                } else {
                    partitions.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(i16::from(ErrorCode::UnknownProducerId)),
                    );
                }
            }

            topics.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name)
                    .partitions(Some(partitions)),
            );
        }

        tx.commit().await?;

        Ok(topics)
    }


    #[instrument(skip_all)]
    pub(super) async fn txn_end_storage(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, transaction_id, producer_id, producer_epoch, committed);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let error_code = self
            .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &tx)
            .await?;

        tx.commit().await?;

        Ok(error_code)
    }


}
