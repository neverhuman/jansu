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

use super::*;

pub(super) async fn init_producer(
    this: &Engine,
    transaction_id: Option<&str>,
    transaction_timeout_ms: i32,
    producer_id: Option<i64>,
    producer_epoch: Option<i16>,
) -> Result<ProducerIdResponse> {
    debug!(
        cluster = this.cluster,
        transaction_id, transaction_timeout_ms, producer_id, producer_epoch
    );
    match (producer_id, producer_epoch, transaction_id) {
        (Some(-1), Some(-1), Some(transaction_id)) => {
            let mut c = this.connection().await.inspect_err(|err| error!(?err))?;
            let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

            if let Some(row) = this
                .prepare_query_opt(
                    &tx,
                    &sql_lookup("producer_epoch_for_current_txn.sql")?,
                    (this.cluster.as_str(), transaction_id),
                )
                .await
                .inspect_err(|err| error!(?err))?
            {
                let id = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .copied()
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect_err(|err| error!(?err))?;
                let epoch = row
                    .get_value(1)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| *i as i16)
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect_err(|err| error!(?err))?;
                let status = row
                    .get_value(2)
                    .map_err(Error::from)
                    .and_then(|value| {
                        value.as_text().map_or(Ok(None), |status| {
                            TxnState::from_str(status.as_str()).map(Some)
                        })
                    })
                    .inspect_err(|err| error!(?err))?;

                debug!(transaction_id, id, epoch, ?status);

                if let Some(TxnState::Begin) = status {
                    let error = this
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

            let (producer, epoch) = if let Some(row) = this
                .prepare_query_opt(
                    &tx,
                    &sql_lookup("txn_select_name.sql")?,
                    (this.cluster.as_str(), transaction_id),
                )
                .await
                .inspect_err(|err| error!(?err))?
            {
                let producer = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .cloned()
                            .ok_or(Error::UnexpectedValue(value.clone()))
                    })
                    .inspect_err(|err| error!(?err))
                    .inspect(|producer| debug!(producer))?;

                let row = this
                    .prepare_query_one(
                        &tx,
                        &sql_lookup("producer_epoch_insert.sql")?,
                        (this.cluster.as_str(), producer),
                    )
                    .await
                    .inspect_err(|err| error!(this.cluster, producer, ?err))?;

                let epoch = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| *i as i16)
                            .ok_or(Error::UnexpectedValue(value.clone()))
                    })
                    .inspect(|epoch| debug!(epoch))
                    .inspect_err(|err| error!(?err))? as i16;

                (producer, epoch)
            } else {
                let row = this
                    .prepare_query_one(
                        &tx,
                        &sql_lookup("producer_insert.sql")?,
                        &[this.cluster.as_str()],
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                let producer = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .copied()
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect_err(|err| error!(?err))?;

                let row = this
                    .prepare_query_one(
                        &tx,
                        &sql_lookup("producer_epoch_insert.sql")?,
                        (this.cluster.as_str(), producer),
                    )
                    .await
                    .inspect_err(|err| error!(this.cluster, producer, ?err))?;

                let epoch = row.get_value(0).map_err(Into::into).and_then(|value| {
                    value
                        .as_integer()
                        .map(|i| *i as i16)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

                assert_eq!(
                    1,
                    this.prepare_execute(
                        &tx,
                        &sql_lookup("txn_insert.sql")?,
                        (this.cluster.as_str(), transaction_id, producer),
                    )
                    .await
                    .inspect_err(|err| error!(
                        this.cluster,
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
                this.prepare_execute(
                    &tx,
                    &sql_lookup("txn_detail_insert.sql")?,
                    (
                        transaction_timeout_ms,
                        this.cluster.as_str(),
                        transaction_id,
                        producer,
                        epoch,
                    ),
                )
                .await
                .inspect_err(|err| error!(
                    this.cluster,
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
                    cluster = this.cluster,
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
        }

        (Some(-1), Some(-1), None) => {
            let mut connection = this.connection().await.inspect_err(|err| error!(?err))?;
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await?;

            let mut rows = tx
                .query(
                    &sql_lookup("producer_insert.sql")?,
                    &[this.cluster.as_str()],
                )
                .await?;

            if let Some(row) = rows.next().await? {
                let producer = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .copied()
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect(|producer| debug!(producer))
                    .inspect_err(|err| error!(?err))?;

                while let Some(row) = rows
                    .next()
                    .await
                    .inspect(|row| debug!(?row))
                    .inspect_err(|err| error!(?err))?
                {
                    debug!(?row)
                }

                let mut rows = tx
                    .query(
                        &sql_lookup("producer_epoch_insert.sql")?,
                        (this.cluster.as_str(), producer),
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = this.cluster, producer))?;

                if let Some(row) = rows.next().await? {
                    let epoch = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .map(|i| *i as i16)
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect(|epoch| debug!(epoch))?;

                    while let Some(row) = rows.next().await? {
                        debug!(?row)
                    }

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
                    .inspect(|response| debug!(?response))
                } else {
                    Ok(ProducerIdResponse {
                        error: ErrorCode::UnknownServerError,
                        id: producer,
                        epoch: -1,
                    })
                    .inspect(|response| debug!(?response))
                }
            } else {
                Ok(ProducerIdResponse {
                    error: ErrorCode::UnknownServerError,
                    id: -1,
                    epoch: -1,
                })
                .inspect(|response| debug!(?response))
            }
        }

        (Some(producer_id), Some(producer_epoch), None) => {
            let mut connection = this.connection().await.inspect_err(|err| error!(?err))?;
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await?;

            let mut rows = tx
                .query(
                    &sql_lookup("producer_epoch_max_select.sql")?,
                    (this.cluster.as_str(), producer_id),
                )
                .await?;

            let max_epoch = if let Some(row) = rows.next().await? {
                row.get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| *i as i16)
                            .ok_or(Error::UnexpectedValue(value))
                    })?
            } else {
                -1
            };

            while let Some(row) = rows.next().await? {
                debug!(?row)
            }

            if max_epoch == -1 {
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

            let mut rows = tx
                .query(
                    &sql_lookup("producer_epoch_insert.sql")?,
                    (this.cluster.as_str(), producer_id),
                )
                .await
                .inspect_err(|err| error!(?err, cluster = this.cluster, producer_id))?;

            if let Some(row) = rows.next().await? {
                let new_epoch = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| *i as i16)
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect(|epoch| debug!(epoch))?;

                while let Some(row) = rows.next().await? {
                    debug!(?row)
                }

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

        (_, _, _) => Ok(ProducerIdResponse {
            error: ErrorCode::UnknownServerError,
            id: -1,
            epoch: -1,
        }),
    }
}

pub(super) async fn txn_add_offsets(
    this: &Engine,
    transaction_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
) -> Result<ErrorCode> {
    debug!(
        cluster = this.cluster,
        transaction_id, producer_id, producer_epoch, group_id
    );

    Ok(ErrorCode::None)
}

pub(super) async fn txn_add_partitions(
    this: &Engine,
    partitions: TxnAddPartitionsRequest,
) -> Result<TxnAddPartitionsResponse> {
    debug!(cluster = this.cluster, ?partitions);

    match partitions {
        TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id,
            producer_id,
            producer_epoch,
            topics,
        } => {
            debug!(?transaction_id, ?producer_id, ?producer_epoch, ?topics);

            let mut c = this.connection().await.inspect_err(|err| error!(?err))?;
            let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

            let mut results = vec![];

            for topic in topics {
                let mut results_by_partition = vec![];

                for partition_index in topic.partitions.unwrap_or(vec![]) {
                    _ = this
                        .prepare_execute(
                            &tx,
                            &sql_lookup("txn_topition_insert.sql")?,
                            (
                                this.cluster.as_str(),
                                topic.name.as_str(),
                                partition_index,
                                transaction_id.as_str(),
                                producer_id,
                                producer_epoch,
                            ),
                        )
                        .await
                        .inspect_err(|err| {
                            error!(
                                ?err,
                                cluster = this.cluster,
                                topic = topic.name,
                                partition_index,
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

            _ = this
                .prepare_execute(
                    &tx,
                    &sql_lookup("txn_detail_update_started_at.sql")?,
                    (
                        this.cluster.as_str(),
                        transaction_id.as_str(),
                        producer_id,
                        producer_epoch,
                    ),
                )
                .await
                .inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = this.cluster,
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

            let mut c = this.connection().await.inspect_err(|err| error!(?err))?;
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
                            let row = this
                                .prepare_query_opt(
                                    &tx,
                                    &sql_lookup("txn_topition_select.sql")?,
                                    (
                                        this.cluster.as_str(),
                                        producer_id,
                                        producer_epoch,
                                        topic.name.as_str(),
                                        partition_index,
                                    ),
                                )
                                .await?;

                            if row.is_some() {
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
                            _ = this
                                .prepare_execute(
                                    &tx,
                                    &sql_lookup("txn_topition_insert.sql")?,
                                    (
                                        this.cluster.as_str(),
                                        topic.name.as_str(),
                                        partition_index,
                                        transaction_id.as_str(),
                                        producer_id,
                                        producer_epoch,
                                    ),
                                )
                                .await
                                .inspect_err(|err| {
                                    error!(
                                        ?err,
                                        cluster = this.cluster,
                                        topic = topic.name,
                                        partition_index,
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
                    _ = this
                        .prepare_execute(
                            &tx,
                            &sql_lookup("txn_detail_update_started_at.sql")?,
                            (
                                this.cluster.as_str(),
                                transaction_id.as_str(),
                                producer_id,
                                producer_epoch,
                            ),
                        )
                        .await
                        .inspect_err(|err| {
                            error!(
                                ?err,
                                cluster = this.cluster,
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
