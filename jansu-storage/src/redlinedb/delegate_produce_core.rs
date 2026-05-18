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
    fn delete_selected_ids<P>(
        &self,
        connection: &mut PoolConnection,
        select_sql: &str,
        delete_sql: &str,
        params: P,
    ) -> Result<usize>
    where
        P: ::redlinedb::Params,
    {
        let ids: Vec<i64> = {
            let s = sql(select_sql).map_err(Error::from)?;
            let mut rows = connection.query(&s, params).map_err(Error::from)?;
            let mut ids = vec![];
            while let Step::Row(row) = rows.step().map_err(Error::from)? {
                ids.push(row.get::<i64>(0).map_err(Error::from)?);
            }
            ids
        };

        let ds = sql(delete_sql).map_err(Error::from)?;
        let mut deleted = 0_usize;
        for id in ids {
            deleted += connection
                .execute(&ds, (id,))
                .map_err(Error::from)?
                .rows_affected as usize;
        }

        Ok(deleted)
    }

    fn update_txn_status(
        &self,
        connection: &mut PoolConnection,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        status: &str,
    ) -> Result<usize> {
        let ids: Vec<i64> = {
            let s = sql("redlinedb/txn_status_select_ids.sql").map_err(Error::from)?;
            let mut rows = connection
                .query(
                    &s,
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                    ),
                )
                .map_err(Error::from)?;
            let mut ids = vec![];
            while let Step::Row(row) = rows.step().map_err(Error::from)? {
                ids.push(row.get::<i64>(0).map_err(Error::from)?);
            }
            ids
        };

        let last_updated = Value::from(SystemTime::now());
        let ds = sql("redlinedb/txn_status_update_id.sql").map_err(Error::from)?;
        let mut updated = 0_usize;
        for id in ids {
            updated += connection
                .execute(&ds, (status, last_updated.clone(), id))
                .map_err(Error::from)?
                .rows_affected as usize;
        }

        Ok(updated)
    }

    #[instrument(skip_all)]
    pub(super) async fn produce_in_tx(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        connection: &mut PoolConnection,
    ) -> Result<i64> {
        let start = SystemTime::now();

        let topic = topition.topic();
        let partition = topition.partition();

        if deflated.is_idempotent() {
            self.ensure_topition_for_produce(topition, connection)?;

            self.idempotent_message_check(transaction_id, topition, &deflated, connection)
                .inspect_err(|err| error!(?err))?;
        }

        debug!(after_idempotent_check = elapsed_millis(start));

        let (low, high) = self
            .watermark_select_for_update(topition, connection)
            .inspect_err(|err| error!(?err))?;

        debug!(after_watermark_select_for_update = elapsed_millis(start));

        debug!(?low, ?high);

        let batch_leader_epoch = deflated.partition_leader_epoch;
        let append_start_offset = high.unwrap_or(0_i64);

        self.maybe_record_leader_epoch_boundary(
            topition,
            batch_leader_epoch,
            append_start_offset,
            connection,
        )?;

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
                            .unwrap_or(false)
                    })
                    .inspect(|jansu_lake_sink| debug!(jansu_lake_sink))?)
        {
            let ri_sql = sql("record_insert.sql").map_err(Error::from)?;
            let hi_sql = sql("header_insert.sql").map_err(Error::from)?;
            let tpo_sql = sql("txn_produce_offset_insert.sql").map_err(Error::from)?;

            for (delta, record) in inflated.records.iter().enumerate() {
                debug!(delta, elapsed = elapsed_millis(start));

                let delta = i64::try_from(delta)?;
                let offset = high.unwrap_or(0_i64) + delta;
                let key = record.key.as_deref();
                let value = record.value.as_deref();

                debug!(?delta, ?offset);

                let _ = connection
                    .execute(
                        &ri_sql,
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
                    .inspect_err(|err| error!(?err, ?topic, ?partition, ?offset, ?key, ?value))
                    .map_err(unique_constraint(ErrorCode::UnknownServerError))?;

                debug!(delta, after_record_insert = elapsed_millis(start));

                for header in record.headers.iter().as_ref() {
                    let key = header.key.as_deref();
                    let value = header.value.as_deref();

                    let _ = connection
                        .execute(
                            &hi_sql,
                            (self.cluster.as_str(), topic, partition, offset, key, value),
                        )
                        .map_err(Error::from)
                        .inspect_err(|err| {
                            error!(?err, ?topic, ?partition, ?offset, ?key, ?value);
                        })?;
                }

                debug!(delta, after_header_insert = elapsed_millis(start));
            }

            debug!(after_record_insert = elapsed_millis(start));

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                let offset_start = high.unwrap_or(0_i64);
                let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

                let _ = connection
                    .execute(
                        &tpo_sql,
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
                    .map_err(Error::from)
                    .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                    .inspect_err(|err| error!(?err))?;
            }

            debug!(after_some_transaction_id = elapsed_millis(start));
        }

        let topition_id = {
            let tid_sql = sql("topition_select_id.sql").map_err(Error::from)?;
            let mut tid_rows = connection
                .query(&tid_sql, (self.cluster.as_str(), topic, partition))
                .map_err(Error::from)
                .inspect_err(|err| error!(?err, ?topic, ?partition))?;
            let Step::Row(tid_row) = tid_rows.step().map_err(Error::from)? else {
                return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
            };
            tid_row
                .get::<i64>(0)
                .map_err(Error::from)
                .inspect_err(|err| error!(?err, ?topic, ?partition))?
        }; // tid_rows and tid_row dropped here, before any .await

        let wm_sql = sql("redlinedb/watermark_update_by_topition_id.sql").map_err(Error::from)?;
        let _ = connection
            .execute(
                &wm_sql,
                (
                    topition_id,
                    low.unwrap_or(0_i64),
                    high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ),
            )
            .map_err(Error::from)
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
                high.unwrap_or(0_i64),
                &inflated,
                config,
            )
            .await
            .inspect_err(|err| error!(?err))?;
        }

        debug!(after_all_done = elapsed_millis(start));

        Ok(high.unwrap_or(0_i64)).inspect(|_| {
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
        connection: &mut PoolConnection,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let mut overlaps = vec![];

        let topics_partitions: Vec<(String, i32)> = {
            let s = sql("txn_select_produced_topitions.sql").map_err(Error::from)?;
            let mut rows = connection
                .query(
                    &s,
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                    ),
                )
                .map_err(Error::from)?;
            let mut tp = vec![];
            while let Step::Row(row) = rows.step().map_err(Error::from)? {
                tp.push((
                    row.get::<String>(0).map_err(Error::from)?,
                    row.get::<i32>(1).map_err(Error::from)?,
                ));
            }
            tp
        };

        for (topic, partition) in topics_partitions {
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

            let s2 = sql("txn_produce_offset_select_offset_range.sql").map_err(Error::from)?;
            let mut rows2 = connection
                .query(
                    &s2,
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        topic.as_str(),
                        partition,
                    ),
                )
                .map_err(Error::from)?;

            if let Step::Row(row) = rows2.step().map_err(Error::from)? {
                let offset_start = row.get::<i64>(0).map_err(Error::from)?;
                let offset_end = row.get::<i64>(1).map_err(Error::from)?;
                debug!(offset_start, offset_end);
                drop(rows2);

                let s3 = sql("redlinedb/txn_produce_offset_select_overlapping_txn.sql")
                    .map_err(Error::from)?;
                let mut rows3 = connection
                    .query(
                        &s3,
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
                    .map_err(Error::from)?;

                while let Step::Row(row) = rows3.step().map_err(Error::from)? {
                    overlaps.push(parse_txn(&row).inspect(|txn| debug!(?txn))?);
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

                let _ = self.delete_selected_ids(
                    connection,
                    "redlinedb/txn_produce_offset_select_ids_by_txn.sql",
                    "redlinedb/txn_produce_offset_delete_id.sql",
                    (
                        self.cluster.as_str(),
                        txn.name.as_str(),
                        txn.producer_id,
                        txn.producer_epoch,
                    ),
                )?;

                let _ = self.delete_selected_ids(
                    connection,
                    "redlinedb/txn_topition_select_ids_by_txn.sql",
                    "redlinedb/txn_topition_delete_id.sql",
                    (
                        self.cluster.as_str(),
                        txn.name.as_str(),
                        txn.producer_id,
                        txn.producer_epoch,
                    ),
                )?;

                if txn.status == TxnState::PrepareCommit {
                    let expires_at = SystemTime::now().checked_add(DEFAULT_OFFSET_RETENTION);
                    let s = sql("consumer_offset_insert_from_txn.sql").map_err(Error::from)?;
                    let _ = connection
                        .execute(
                            &s,
                            (
                                self.cluster.as_str(),
                                txn.name.as_str(),
                                txn.producer_id,
                                txn.producer_epoch,
                                expires_at.map(Value::from),
                            ),
                        )
                        .map_err(Error::from)?;
                }

                let _ = self.delete_selected_ids(
                    connection,
                    "redlinedb/txn_offset_commit_tp_select_ids_by_txn.sql",
                    "redlinedb/txn_offset_commit_tp_delete_id.sql",
                    (
                        self.cluster.as_str(),
                        txn.name.as_str(),
                        txn.producer_id,
                        txn.producer_epoch,
                    ),
                )?;

                let _ = self.delete_selected_ids(
                    connection,
                    "redlinedb/txn_offset_commit_select_ids_by_txn.sql",
                    "redlinedb/txn_offset_commit_delete_id.sql",
                    (
                        self.cluster.as_str(),
                        txn.name.as_str(),
                        txn.producer_id,
                        txn.producer_epoch,
                    ),
                )?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                let _ = self.update_txn_status(
                    connection,
                    txn.name.as_str(),
                    txn.producer_id,
                    txn.producer_epoch,
                    outcome.as_str(),
                )?;
            }
        } else {
            debug!(?overlaps);

            let outcome = if committed {
                String::from(TxnState::PrepareCommit)
            } else {
                String::from(TxnState::PrepareAbort)
            };

            let _ = self
                .update_txn_status(
                    connection,
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    outcome.as_str(),
                )
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
