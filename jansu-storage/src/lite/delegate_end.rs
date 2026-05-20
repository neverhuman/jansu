//! Transaction completion (commit/abort) path for the libSQL `Delegate`.

use super::*;

impl Delegate {
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
