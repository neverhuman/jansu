use super::*;

impl Engine {
    pub(super) async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &Connection,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        let mut rows = connection
            .query(
                &sql_lookup("producer_epoch_current_for_producer.sql")?,
                (self.cluster.as_str(), deflated.producer_id),
            )
            .await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            let current_epoch = row
                .get_value(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|value| value as i16)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

            let row = self
                .prepare_query_one(
                    connection,
                    &sql_lookup("producer_select_for_update.sql")?,
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

            let sequence = row
                .get_value(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|value| value as i32)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

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
                self.prepare_execute(
                    connection,
                    &sql_lookup("producer_detail_insert.sql")?,
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

    pub(super) async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        tx: &Connection,
    ) -> Result<(Option<i64>, Option<i64>)> {
        debug!(?topition, ?tx);

        let mut rows = tx
            .query(
                &sql_lookup("watermark_select_no_update.sql")?,
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
                row.get_value(0)
                    .map(|value| value.as_integer().copied())
                    .inspect_err(|err| error!(?err))?,
                row.get_value(1)
                    .map(|value| value.as_integer().copied())
                    .inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    pub(super) async fn maybe_record_leader_epoch_boundary<'conn>(
        &self,
        topition: &Topition,
        epoch: i32,
        start_offset: i64,
        tx: &Transaction<'conn>,
    ) -> Result<()> {
        let mut rows = tx
            .query(
                &sql_lookup("leader_epoch_history.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        let mut current_epoch: Option<i32> = None;
        while let Some(row) = rows.next().await? {
            let row_epoch = row.get::<Option<i32>>(0)?.unwrap_or_default();
            current_epoch = Some(current_epoch.map_or(row_epoch, |current| current.max(row_epoch)));
        }

        if current_epoch.is_none_or(|current| epoch > current) {
            _ = self
                .prepare_execute(
                    tx,
                    &sql_lookup("leader_epoch_history_insert.sql")?,
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

    pub(super) async fn produce_in_tx<'conn>(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        tx: &Transaction<'conn>,
    ) -> Result<i64> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?topition, ?deflated);

        let topic = topition.topic();
        let partition = topition.partition();

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, tx.deref())
                .await
                .inspect_err(|err| error!(?err))?;
        }

        let (low, high) = self.watermark_select_for_update(topition, tx).await?;

        debug!(?low, ?high);

        let batch_leader_epoch = deflated.partition_leader_epoch;
        let append_start_offset = high.unwrap_or_default();

        self.maybe_record_leader_epoch_boundary(
            topition,
            batch_leader_epoch,
            append_start_offset,
            tx,
        )
        .await?;

        let inflated = inflated::Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

        if !attributes.control
            && let Some(ref schemas) = self.schemas
        {
            schemas.validate(topition.topic(), &inflated).await?;
        }

        let last_offset_delta = i64::from(inflated.last_offset_delta);

        for (delta, record) in inflated.records.iter().enumerate() {
            let delta = i64::try_from(delta)?;
            let offset = high.unwrap_or_default() + delta;
            let key = record.key.as_deref();
            let value = record.value.as_deref();

            debug!(?delta, ?record, ?offset);

            _ = self
                .prepare_execute(
                    tx,
                    &sql_lookup("record_insert.sql")?,
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

            for header in record.headers.iter().as_ref() {
                let key = header.key.as_deref();
                let value = header.value.as_deref();

                _ = self
                    .prepare_execute(
                        tx,
                        &sql_lookup("header_insert.sql")?,
                        (self.cluster.as_str(), topic, partition, offset, key, value),
                    )
                    .await
                    .inspect_err(|err| {
                        error!(?err, ?topic, ?partition, ?offset, ?key, ?value);
                    });
            }
        }

        if let Some(transaction_id) = transaction_id
            && attributes.transaction
        {
            let offset_start = high.unwrap_or_default();
            let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

            _ = self
                    .prepare_execute(tx,
                        &sql_lookup("txn_produce_offset_insert.sql")?,
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

        _ = self
            .prepare_execute(
                tx,
                &sql_lookup("watermark_update.sql")?,
                (
                    self.cluster.as_str(),
                    topic,
                    partition,
                    low.unwrap_or_default(),
                    high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ),
            )
            .await
            .inspect(|n| debug!(?n))
            .inspect_err(|err| error!(?err))?;

        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await?;

            lake.store(
                topition.topic(),
                topition.partition(),
                high.unwrap_or_default(),
                &inflated,
                config,
            )
            .await
            .inspect(|store| debug!(?store))
            .inspect_err(|err| debug!(?err))?;
        }

        Ok(high.unwrap_or_default())
    }

    pub(super) async fn end_in_tx<'conn>(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        tx: &Transaction<'conn>,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let mut overlaps = vec![];

        let mut rows = tx
            .deref()
            .query(
                &sql_lookup("txn_select_produced_topitions.sql")?,
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                ),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let topic = row.get_value(0).map_err(Into::into).and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })?;

            let partition = row.get_value(1).map_err(Into::into).and_then(|value| {
                value
                    .as_integer()
                    .map(|i| *i as i32)
                    .ok_or(Error::UnexpectedValue(value))
            })?;

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
                .produce_in_tx(Some(transaction_id), &topition, batch, tx)
                .await?;

            debug!(offset, ?topition);

            let mut rows = tx
                .query(
                    &sql_lookup("txn_produce_offset_select_offset_range.sql")?,
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
                let offset_start = row.get_value(0).map_err(Into::into).and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .ok_or(Error::UnexpectedValue(value))
                })?;

                let offset_end = row.get_value(1).map_err(Into::into).and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .ok_or(Error::UnexpectedValue(value))
                })?;

                debug!(offset_start, offset_end);

                let mut rows = tx
                    .query(
                        &sql_lookup("txn_produce_offset_select_overlapping_txn.sql")?,
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

                _ = tx
                    .execute(
                        &sql_lookup("txn_produce_offset_delete_by_txn.sql")?,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = tx
                    .execute(
                        &sql_lookup("txn_topition_delete_by_txn.sql")?,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    _ = tx
                        .execute(
                            &sql_lookup("consumer_offset_insert_from_txn.sql")?,
                            (
                                self.cluster.as_str(),
                                txn.name.as_str(),
                                txn.producer_id,
                                txn.producer_epoch,
                            ),
                        )
                        .await?;
                }

                _ = tx
                    .execute(
                        &sql_lookup("txn_offset_commit_tp_delete_by_txn.sql")?,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = tx
                    .execute(
                        &sql_lookup("txn_offset_commit_delete_by_txn.sql")?,
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

                _ = tx
                    .execute(
                        &sql_lookup("txn_status_update.sql")?,
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

            _ = tx
                .execute(
                    &sql_lookup("txn_status_update.sql")?,
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
