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
//! Inherent `Postgres` methods backing the `Storage` trait implementation.

use super::*;

impl Postgres {
    pub(crate) async fn init_producer_dispatch(
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
        } else {
            // Non-sentinel init_producer: a client requested an explicit
            // producer_id/epoch (e.g. KIP-360 epoch bumping). The PostgreSQL
            // backend does not yet implement that path; mirror the limbo
            // backend by surfacing a typed error instead of panicking.
            Ok(ProducerIdResponse {
                error: ErrorCode::UnknownServerError,
                id: producer_id.unwrap_or(-1),
                epoch: producer_epoch.unwrap_or(-1),
            })
        }
    }
}
