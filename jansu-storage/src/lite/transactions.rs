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

use std::{
    str::FromStr,
    time::SystemTime,
};

use bytes::Bytes;
use jansu_schema::lake::LakeHouse as _;
use jansu_sans_io::{
    BatchAttribute, ConfigResource, ControlBatch, EndTransactionMarker, ErrorCode,
    record::{Record, deflated, inflated},
};
use libsql::Row;
use opentelemetry::KeyValue;
use tracing::{debug, error, instrument};

use crate::{DEFAULT_OFFSET_RETENTION, Error, Result, Storage, Topition, TxnState,
    sql::idempotent_sequence_check,
};

use super::{
    Delegate, LiteTimestamp,
    connection::PoolConnection,
    metrics::{PRODUCE_IN_TX_DURATION, elapsed_millis},
    unique_constraint,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct Txn {
    pub(super) name: String,
    pub(super) producer_id: i64,
    pub(super) producer_epoch: i16,
    pub(super) status: TxnState,
}

impl TryFrom<Row> for Txn {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let name = row.get::<String>(0).inspect_err(|err| error!(?err))?;
        let producer_id = row.get::<i64>(1).inspect_err(|err| error!(?err))?;
        let producer_epoch = row.get::<i32>(2).inspect_err(|err| error!(?err))? as i16;
        let status = row
            .get::<Option<String>>(3)
            .map_err(Into::into)
            .and_then(|status| status.map_or(Ok(TxnState::Begin), TxnState::try_from))
            .inspect_err(|err| error!(?err))?;

        Ok(Self {
            name,
            producer_id,
            producer_epoch,
            status,
        })
    }
}

impl Delegate {
    #[instrument(skip_all)]
    pub(super) async fn idempotent_message_check(
        &self,
        _transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &PoolConnection,
    ) -> Result<()> {
        let mut rows = connection
            .query(
                "producer_epoch_current_for_producer.sql",
                (self.cluster.as_str(), deflated.producer_id),
            )
            .await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            let current_epoch = row
                .get::<i32>(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))?
                as i16;

            let row = connection
                .query_one(
                    "producer_select_for_update.sql",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                    ),
                )
                .await
                .inspect_err(|err| {
                    error!(
                        self.cluster,
                        ?topition,
                        deflated.producer_id,
                        deflated.producer_epoch,
                        ?err
                    )
                })?;

            let sequence = row.get::<i32>(0).inspect_err(|err| error!(?err))?;

            debug!(
                self.cluster,
                ?topition,
                deflated.producer_id,
                deflated.producer_epoch,
                current_epoch,
                sequence,
            );

            let increment = idempotent_sequence_check(&current_epoch, &sequence, deflated)?;

            debug!(increment);

            assert_eq!(
                1,
                connection
                    .execute(
                        "producer_detail_insert.sql",
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            deflated.producer_id,
                            deflated.producer_epoch,
                            increment,
                        ),
                    )
                    .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    #[instrument(skip_all)]
    pub(super) async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        connection: &PoolConnection,
    ) -> Result<(Option<i64>, Option<i64>)> {
        debug!(?topition);

        let mut rows = connection
            .query(
                "watermark_select_no_update.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        if let Some(row) = rows
            .next()
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Ok((
                row.get::<Option<i64>>(0).inspect_err(|err| error!(?err))?,
                row.get::<Option<i64>>(1).inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    #[instrument(skip_all)]
    pub(super) async fn maybe_record_leader_epoch_boundary(
        &self,
        topition: &Topition,
        epoch: i32,
        start_offset: i64,
        connection: &PoolConnection,
    ) -> Result<()> {
        let mut rows = connection
            .query(
                "leader_epoch_history.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        let mut current_epoch: Option<i32> = None;
        while let Some(row) = rows.next().await? {
            let row_epoch = row.get::<i32>(0).inspect_err(|err| error!(?err))?;
            current_epoch = Some(current_epoch.map_or(row_epoch, |current| current.max(row_epoch)));
        }

        if current_epoch.is_none_or(|current| epoch > current) {
            _ = connection
                .execute(
                    "leader_epoch_history_insert.sql",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        epoch,
                        start_offset,
                    ),
                )
                .await
                .inspect_err(|err| error!(?err, ?topition, epoch, start_offset))?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub(super) async fn produce_in_tx(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        connection: &PoolConnection,
    ) -> Result<i64> {
        let start = SystemTime::now();

        let topic = topition.topic();
        let partition = topition.partition();

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, connection)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        debug!(after_idempotent_check = elapsed_millis(start));

        let (low, high) = self
            .watermark_select_for_update(topition, connection)
            .await
            .inspect_err(|err| error!(?err))?;

        debug!(after_watermark_select_for_update = elapsed_millis(start));

        debug!(?low, ?high);

        let batch_leader_epoch = deflated.partition_leader_epoch;
        let append_start_offset = high.unwrap_or_default();

        self.maybe_record_leader_epoch_boundary(
            topition,
            batch_leader_epoch,
            append_start_offset,
            connection,
        )
        .await?;

        let inflated = inflated::Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        debug!(after_inflate = elapsed_millis(start));

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

        debug!(after_attributes = elapsed_millis(start));

        if !attributes.control
            && let Some(ref schemas) = self.schemas
            && self
                .describe_config(topic, ConfigResource::Topic, None)
                .await
                .map(|resources| {
                    resources
                        .configs
                        .as_ref()
                        .and_then(|configs| {
                            configs
                                .iter()
                                .inspect(|config| debug!(?config))
                                .find(|config| config.name.as_str() == "jansu.schema.validation")
                                .and_then(|config| config.value.as_deref())
                                .and_then(|value| bool::from_str(value).ok())
                        })
                        .unwrap_or(true)
                })
                .inspect(|schema_validation| debug!(schema_validation))?
        {
            schemas.validate(topition.topic(), &inflated).await?;
        }

        debug!(after_validation = elapsed_millis(start));

        let last_offset_delta = i64::from(inflated.last_offset_delta);

        if self.schemas.is_none()
            || self.lake.is_none()
            || (self.lake.is_some()
                && !self
                    .describe_config(topic, ConfigResource::Topic, None)
                    .await
                    .inspect(|resources| debug!(?resources))
                    .map(|resources| {
                        resources
                            .configs
                            .as_ref()
                            .and_then(|configs| {
                                configs
                                    .iter()
                                    .inspect(|config| debug!(?config))
                                    .find(|config| config.name.as_str() == "jansu.lake.sink")
                                    .and_then(|config| config.value.as_deref())
                                    .and_then(|value| bool::from_str(value).ok())
                            })
                            .unwrap_or_default()
                    })
                    .inspect(|jansu_lake_sink| debug!(jansu_lake_sink))?)
        {
            for (delta, record) in inflated.records.iter().enumerate() {
                debug!(delta, elapsed = elapsed_millis(start));

                let delta = i64::try_from(delta)?;
                let offset = high.unwrap_or_default() + delta;
                let key = record.key.as_deref();
                let value = record.value.as_deref();

                debug!(?delta, ?offset);

                _ = connection
                    .execute(
                        "record_insert.sql",
                        (
                            self.cluster.as_str(),
                            topic,
                            partition,
                            offset,
                            inflated.attributes,
                            if transaction_id.is_none() {
                                None
                            } else {
                                Some(inflated.producer_id)
                            },
                            if transaction_id.is_none() {
                                None
                            } else {
                                Some(inflated.producer_epoch)
                            },
                            inflated.base_timestamp + record.timestamp_delta,
                            key,
                            value,
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, ?topic, ?partition, ?offset, ?key, ?value))
                    .map_err(unique_constraint(ErrorCode::UnknownServerError))?;

                debug!(delta, after_record_insert = elapsed_millis(start));

                for header in record.headers.iter().as_ref() {
                    let key = header.key.as_deref();
                    let value = header.value.as_deref();

                    _ = connection
                        .execute(
                            "header_insert.sql",
                            (self.cluster.as_str(), topic, partition, offset, key, value),
                        )
                        .await
                        .inspect_err(|err| {
                            error!(?err, ?topic, ?partition, ?offset, ?key, ?value);
                        });
                }

                debug!(delta, after_header_insert = elapsed_millis(start));
            }

            debug!(after_record_insert = elapsed_millis(start));

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                let offset_start = high.unwrap_or_default();
                let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

                _ = connection
                        .execute(
                            "txn_produce_offset_insert.sql",
                            (
                                self.cluster.as_str(),
                                transaction_id,
                                inflated.producer_id,
                                inflated.producer_epoch,
                                topic,
                                partition,
                                offset_start,
                                offset_end,
                            ),
                        )
                        .await
                        .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                        .inspect_err(|err| error!(?err))?;
            }

            debug!(after_some_transaction_id = elapsed_millis(start));
        }

        _ = connection
            .execute(
                "watermark_update.sql",
                (
                    self.cluster.as_str(),
                    topic,
                    partition,
                    low.unwrap_or_default(),
                    high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ),
            )
            .await
            .inspect(|n| debug!(?n, after_watermark_update = elapsed_millis(start)))
            .inspect_err(|err| error!(?err))?;

        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await
                .inspect_err(|err| error!(?err))?;

            lake.store(
                topition.topic(),
                topition.partition(),
                high.unwrap_or_default(),
                &inflated,
                config,
            )
            .await
            .inspect_err(|err| error!(?err))?;
        }

        debug!(after_all_done = elapsed_millis(start));

        Ok(high.unwrap_or_default()).inspect(|_| {
            PRODUCE_IN_TX_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            );
        })
    }

    pub(super) async fn end_in_tx(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        connection: &PoolConnection,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let mut overlaps = vec![];

        let mut rows = connection
            .query(
                "txn_select_produced_topitions.sql",
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                ),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let topic = row.get::<String>(0)?;
            let partition = row.get::<i32>(1)?;

            let topition = Topition::new(topic.clone(), partition);

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

            let offset = self
                .produce_in_tx(Some(transaction_id), &topition, batch, connection)
                .await?;

            debug!(offset, ?topition);

            let mut rows = connection
                .query(
                    "txn_produce_offset_select_offset_range.sql",
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        topic.as_str(),
                        partition,
                    ),
                )
                .await?;

            if let Some(row) = rows.next().await? {
                let offset_start = row.get::<i64>(0)?;
                let offset_end = row.get::<i64>(1)?;
                debug!(offset_start, offset_end);

                let mut rows = connection
                    .query(
                        "txn_produce_offset_select_overlapping_txn.sql",
                        (
                            self.cluster.as_str(),
                            transaction_id,
                            producer_id,
                            producer_epoch,
                            topic.as_str(),
                            partition,
                            offset_end,
                        ),
                    )
                    .await?;

                while let Some(row) = rows.next().await? {
                    overlaps.push(Txn::try_from(row).inspect(|txn| debug!(?txn))?);
                }
            }
        }

        if overlaps.iter().all(|txn| txn.status.is_prepared()) {
            let txns = {
                let mut txns = Vec::with_capacity(overlaps.len() + 1);

                txns.append(&mut overlaps);

                txns.push(Txn {
                    name: transaction_id.into(),
                    producer_id,
                    producer_epoch,
                    status: if committed {
                        TxnState::PrepareCommit
                    } else {
                        TxnState::PrepareAbort
                    },
                });

                txns
            };

            debug!(?txns);

            for txn in txns {
                debug!(?txn);

                _ = connection
                    .execute(
                        "txn_produce_offset_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = connection
                    .execute(
                        "txn_topition_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    let expires_at = SystemTime::now().checked_add(DEFAULT_OFFSET_RETENTION);

                    _ = connection
                        .execute(
                            "consumer_offset_insert_from_txn.sql",
                            (
                                self.cluster.as_str(),
                                txn.name.as_str(),
                                txn.producer_id,
                                txn.producer_epoch,
                                expires_at.map(LiteTimestamp::from),
                            ),
                        )
                        .await?;
                }

                _ = connection
                    .execute(
                        "txn_offset_commit_tp_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = connection
                    .execute(
                        "txn_offset_commit_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                _ = connection
                    .execute(
                        "txn_status_update.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                            outcome,
                        ),
                    )
                    .await?;
            }
        } else {
            debug!(?overlaps);

            let outcome = if committed {
                String::from(TxnState::PrepareCommit)
            } else {
                String::from(TxnState::PrepareAbort)
            };

            _ = connection
                .execute(
                    "txn_status_update.sql",
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        outcome.as_str(),
                    ),
                )
                .await
                .inspect(|n| {
                    debug!(
                        cluster = self.cluster,
                        transaction_id, producer_id, producer_epoch, outcome, n
                    )
                })?;
        }

        Ok(ErrorCode::None)
    }
}
