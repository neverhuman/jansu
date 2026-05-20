//! Producer initialization for the Turso `Engine`.

use super::sql::sql_lookup;
use super::*;

impl Engine {
    pub(super) async fn init_producer_impl(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            cluster = self.cluster,
            transaction_id, transaction_timeout_ms, producer_id, producer_epoch
        );
        match (producer_id, producer_epoch, transaction_id) {
            (Some(-1), Some(-1), Some(transaction_id)) => {
                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                if let Some(row) = self
                    .prepare_query_opt(
                        &tx,
                        &sql_lookup("producer_epoch_for_current_txn.sql")?,
                        (self.cluster.as_str(), transaction_id),
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
                    .prepare_query_opt(
                        &tx,
                        &sql_lookup("txn_select_name.sql")?,
                        (self.cluster.as_str(), transaction_id),
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

                    let row = self
                        .prepare_query_one(
                            &tx,
                            &sql_lookup("producer_epoch_insert.sql")?,
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

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
                    let row = self
                        .prepare_query_one(
                            &tx,
                            &sql_lookup("producer_insert.sql")?,
                            &[self.cluster.as_str()],
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

                    let row = self
                        .prepare_query_one(
                            &tx,
                            &sql_lookup("producer_epoch_insert.sql")?,
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch = row.get_value(0).map_err(Into::into).and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| *i as i16)
                            .ok_or(Error::UnexpectedValue(value))
                    })?;

                    assert_eq!(
                        1,
                        self.prepare_execute(
                            &tx,
                            &sql_lookup("txn_insert.sql")?,
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
                    self.prepare_execute(
                        &tx,
                        &sql_lookup("txn_detail_insert.sql")?,
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
            }

            (Some(-1), Some(-1), None) => {
                let mut connection = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .await?;

                let mut rows = tx
                    .query(
                        &sql_lookup("producer_insert.sql")?,
                        &[self.cluster.as_str()],
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
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(?err, cluster = self.cluster, producer))?;

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

            (producer_id, producer_epoch, _) => Ok(ProducerIdResponse {
                error: ErrorCode::UnknownServerError,
                id: producer_id.unwrap_or(-1),
                epoch: producer_epoch.unwrap_or(-1),
            })
            .inspect(|response| debug!(?response)),
        }
    }
}
