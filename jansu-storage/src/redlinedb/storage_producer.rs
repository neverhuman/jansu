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
    fn insert_producer(&self, pc: &mut PoolConnection) -> Result<i64> {
        let next_id = {
            let s = sql("redlinedb/producer_select_current_id.sql").map_err(Error::from)?;
            let mut rows = pc
                .query(&s, (self.cluster.as_str(),))
                .map_err(Error::from)
                .inspect_err(|err| error!(self.cluster, ?err))?;
            match rows.step().map_err(Error::from)? {
                Step::Row(row) => {
                    row.get::<i64>(0)
                        .map_err(Error::from)
                        .inspect_err(|err| error!(?err))?
                        + 1
                }
                Step::Done => 1,
            }
        };

        let s = sql("redlinedb/producer_insert_id.sql").map_err(Error::from)?;
        let _ = pc
            .execute(&s, (self.cluster.as_str(), next_id))
            .map_err(Error::from)
            .inspect_err(|err| error!(self.cluster, next_id, ?err))?;

        Ok(next_id)
    }

    fn insert_producer_epoch(&self, pc: &mut PoolConnection, producer: i64) -> Result<i16> {
        let next_epoch = {
            let s = sql("producer_epoch_current_for_producer.sql").map_err(Error::from)?;
            let mut rows = pc
                .query(&s, (self.cluster.as_str(), producer))
                .map_err(Error::from)
                .inspect_err(|err| error!(self.cluster, producer, ?err))?;
            match rows.step().map_err(Error::from)? {
                Step::Row(row) => {
                    row.get::<i32>(0)
                        .map_err(Error::from)
                        .inspect_err(|err| error!(?err, producer))?
                        + 1
                }
                Step::Done => 0,
            }
        };

        let s = sql("redlinedb/producer_epoch_insert_value.sql").map_err(Error::from)?;
        let _ = pc
            .execute(&s, (producer, next_epoch))
            .map_err(Error::from)
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
                let mut pc = self.connection().await?;
                pc.begin(BeginMode::Immediate).map_err(Error::from)?;

                let txn_data = {
                    let s = sql("producer_epoch_for_current_txn.sql").map_err(Error::from)?;
                    let mut rows = pc
                        .query(&s, (self.cluster.as_str(), transaction_id))
                        .map_err(Error::from)
                        .inspect_err(|err| error!(?err))?;
                    match rows.step().map_err(Error::from)? {
                        Step::Row(row) => {
                            let id = row
                                .get::<i64>(0)
                                .map_err(Error::from)
                                .inspect_err(|err| error!(?err))?;
                            let epoch = row
                                .get::<i32>(1)
                                .map_err(Error::from)
                                .inspect_err(|err| error!(?err))?
                                as i16;
                            let status = row
                                .get::<Option<String>>(2)
                                .map_err(Error::from)
                                .inspect_err(|err| error!(?err))?
                                .map_or(Ok(None), |status| {
                                    TxnState::from_str(status.as_str()).map(Some)
                                })?;
                            Some((id, epoch, status))
                        }
                        Step::Done => None,
                    }
                };

                if let Some((id, epoch, status)) = txn_data {
                    debug!(transaction_id, id, epoch, ?status);

                    if let Some(TxnState::Begin) = status {
                        let error = self
                            .end_in_tx(transaction_id, id, epoch, false, &mut pc)
                            .await?;

                        if error != ErrorCode::None {
                            pc.rollback()
                                .map_err(Error::from)
                                .inspect_err(|err| error!(?err, ?transaction_id, id, epoch))?;

                            return Ok(ProducerIdResponse { error, id, epoch }).inspect(|_| {
                                DELEGATE_REQUEST_DURATION.record(
                                    elapsed_millis(start),
                                    &[KeyValue::new("operation", "init_producer")],
                                )
                            });
                        }
                    }
                }

                let producer_data = {
                    let s = sql("txn_select_name.sql").map_err(Error::from)?;
                    let mut rows = pc
                        .query(&s, (self.cluster.as_str(), transaction_id))
                        .map_err(Error::from)
                        .inspect_err(|err| error!(?err))?;
                    match rows.step().map_err(Error::from)? {
                        Step::Row(row) => Some(
                            row.get::<i64>(0)
                                .map_err(Error::from)
                                .inspect_err(|err| error!(?err))
                                .inspect(|producer| debug!(producer))?,
                        ),
                        Step::Done => None,
                    }
                };

                let (producer, epoch) = if let Some(producer) = producer_data {
                    let epoch = self
                        .insert_producer_epoch(&mut pc, producer)
                        .inspect(|epoch| debug!(epoch))?;
                    (producer, epoch)
                } else {
                    let producer = self.insert_producer(&mut pc)?;
                    let epoch = self.insert_producer_epoch(&mut pc, producer)?;

                    let s = sql("txn_insert.sql").map_err(Error::from)?;
                    let _ = pc
                        .execute(&s, (self.cluster.as_str(), transaction_id, producer))
                        .map_err(Error::from)
                        .inspect_err(|err| error!(self.cluster, transaction_id, producer, ?err))?;

                    (producer, epoch)
                };

                debug!(transaction_id, producer, epoch);

                let s = sql("txn_detail_insert.sql").map_err(Error::from)?;
                let _ = pc
                    .execute(
                        &s,
                        (
                            transaction_timeout_ms,
                            self.cluster.as_str(),
                            transaction_id,
                            producer,
                            epoch,
                        ),
                    )
                    .map_err(Error::from)
                    .inspect_err(|err| {
                        error!(
                            self.cluster,
                            transaction_id,
                            producer,
                            epoch,
                            transaction_timeout_ms,
                            ?err
                        )
                    })?;

                let error = match pc.commit().map_err(Error::from).inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        transaction_id,
                        producer,
                        epoch
                    )
                }) {
                    Ok(_) => ErrorCode::None,
                    Err(_) => ErrorCode::UnknownServerError,
                };

                Ok(ProducerIdResponse {
                    error,
                    id: producer,
                    epoch,
                })
            }

            (Some(-1), Some(-1), None) => {
                let mut pc = self.connection().await?;
                pc.begin(BeginMode::Immediate).map_err(Error::from)?;

                let producer = self
                    .insert_producer(&mut pc)
                    .inspect(|producer| debug!(producer))?;

                let epoch = self
                    .insert_producer_epoch(&mut pc, producer)
                    .inspect(|epoch| debug!(epoch))?;

                let error = match pc
                    .commit()
                    .map_err(Error::from)
                    .inspect_err(|err| error!(?err, ?transaction_id, producer, epoch))
                {
                    Ok(_) => ErrorCode::None,
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
                let mut pc = self.connection().await?;
                pc.begin(BeginMode::Immediate).map_err(Error::from)?;

                let max_epoch = {
                    let s = sql("producer_epoch_current_for_producer.sql").map_err(Error::from)?;
                    let mut rows = pc
                        .query(&s, (self.cluster.as_str(), producer_id))
                        .map_err(Error::from)?;
                    match rows.step().map_err(Error::from)? {
                        Step::Row(row) => row
                            .get::<i32>(0)
                            .map_err(Error::from)
                            .map(|epoch| i16::try_from(epoch).unwrap_or(-1))
                            .unwrap_or(-1),
                        Step::Done => -1,
                    }
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
                    .insert_producer_epoch(&mut pc, producer_id)
                    .inspect(|epoch| debug!(epoch))?;

                let error = match pc
                    .commit()
                    .map_err(Error::from)
                    .inspect_err(|err| error!(?err, producer_id, new_epoch))
                {
                    Ok(_) => ErrorCode::None,
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
