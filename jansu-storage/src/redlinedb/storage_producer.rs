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

impl Delegate {
    async fn insert_producer(&self, pc: &PoolConnection) -> Result<i64> {
        let next_id = if let Some(row) = pc
            .query_opt(
                "redlinedb/producer_select_current_id.sql",
                (self.cluster.as_str(),),
            )
            .await
            .inspect_err(|err| error!(self.cluster, ?err))?
        {
            row.get::<i64>(0).inspect_err(|err| error!(?err))? + 1
        } else {
            1
        };

        _ = pc
            .execute(
                "redlinedb/producer_insert_id.sql",
                (self.cluster.as_str(), next_id),
            )
            .await
            .inspect_err(|err| error!(self.cluster, next_id, ?err))?;

        Ok(next_id)
    }

    async fn insert_producer_epoch(&self, pc: &PoolConnection, producer: i64) -> Result<i16> {
        let next_epoch = if let Some(row) = pc
            .query_opt(
                "producer_epoch_current_for_producer.sql",
                (self.cluster.as_str(), producer),
            )
            .await
            .inspect_err(|err| error!(self.cluster, producer, ?err))?
        {
            row.get::<i32>(0)
                .inspect_err(|err| error!(?err, producer))?
                + 1
        } else {
            0
        };

        _ = pc
            .execute(
                "redlinedb/producer_epoch_insert_value.sql",
                (producer, next_epoch),
            )
            .await
            .inspect_err(|err| error!(self.cluster, producer, next_epoch, ?err))?;

        i16::try_from(next_epoch).map_err(Into::into)
    }

    pub(super) async fn delegate_init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let start = SystemTime::now();

        debug!(
            cluster = self.cluster,
            transaction_id, transaction_timeout_ms, producer_id, producer_epoch
        );

        match (producer_id, producer_epoch, transaction_id) {
            (Some(-1), Some(-1), Some(transaction_id)) => {
                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                if let Some(row) = pc
                    .query_opt(
                        "producer_epoch_for_current_txn.sql",
                        (self.cluster.as_str(), transaction_id),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let id = row.get::<i64>(0).inspect_err(|err| error!(?err))?;
                    let epoch = row.get::<i32>(1).inspect_err(|err| error!(?err))? as i16;
                    let status = row
                        .get::<Option<String>>(2)
                        .inspect_err(|err| error!(?err))?
                        .map_or(Ok(None), |status| {
                            TxnState::from_str(status.as_str()).map(Some)
                        })?;

                    debug!(transaction_id, id, epoch, ?status);

                    if let Some(TxnState::Begin) = status {
                        let error = self
                            .end_in_tx(transaction_id, id, epoch, false, &pc)
                            .await?;

                        if error != ErrorCode::None {
                            _ = tx
                                .rollback()
                                .await
                                .inspect_err(|err| error!(?err, ?transaction_id, id, epoch));

                            return Ok(ProducerIdResponse { error, id, epoch }).inspect(|_| {
                                DELEGATE_REQUEST_DURATION.record(
                                    elapsed_millis(start),
                                    &[KeyValue::new("operation", "init_producer")],
                                )
                            });
                        }
                    }
                }

                let (producer, epoch) = if let Some(row) = pc
                    .query_opt(
                        "txn_select_name.sql",
                        (self.cluster.as_str(), transaction_id),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let producer: i64 = row
                        .get(0)
                        .inspect_err(|err| error!(?err))
                        .inspect(|producer| debug!(producer))?;

                    let epoch = self
                        .insert_producer_epoch(&pc, producer)
                        .await
                        .inspect(|epoch| debug!(epoch))?;

                    (producer, epoch)
                } else {
                    let producer = self.insert_producer(&pc).await?;
                    let epoch = self.insert_producer_epoch(&pc, producer).await?;

                    assert_eq!(
                        1,
                        pc.execute(
                            "txn_insert.sql",
                            (self.cluster.as_str(), transaction_id, producer),
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
                    pc.execute(
                        "txn_detail_insert.sql",
                        (
                            transaction_timeout_ms,
                            self.cluster.as_str(),
                            transaction_id,
                            producer,
                            epoch,
                        ),
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

                let error = match pc.commit(tx).await.inspect_err(|err| {
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
            }

            (Some(-1), Some(-1), None) => {
                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                let producer = self
                    .insert_producer(&pc)
                    .await
                    .inspect(|producer| debug!(producer))?;

                let epoch = self
                    .insert_producer_epoch(&pc, producer)
                    .await
                    .inspect(|epoch| debug!(epoch))?;

                let error = match pc
                    .commit(tx)
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
                .inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "init_producer")],
                    )
                })
            }

            (Some(producer_id), Some(producer_epoch), None) => {
                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                let max_epoch = if let Some(row) = pc
                    .query_opt(
                        "producer_epoch_current_for_producer.sql",
                        (self.cluster.as_str(), producer_id),
                    )
                    .await?
                {
                    row.get::<i32>(0)
                        .ok()
                        .and_then(|epoch| i16::try_from(epoch).ok())
                        .unwrap_or(-1)
                } else {
                    -1
                };

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

                let new_epoch = self
                    .insert_producer_epoch(&pc, producer_id)
                    .await
                    .inspect(|epoch| debug!(epoch))?;

                let error = match pc
                    .commit(tx)
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
            }

            (_, _, _) => Ok(ProducerIdResponse {
                error: ErrorCode::UnknownServerError,
                id: -1,
                epoch: -1,
            }),
        }
    }

    pub(super) async fn delegate_txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();

        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_add_offsets")],
            )
        })
    }
}
