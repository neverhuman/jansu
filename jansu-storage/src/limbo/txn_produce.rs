//! In-transaction produce path and the `produce` entry point for the Turso `Engine`.

use super::sql::{sql_lookup, unique_constraint};
use super::*;

impl Engine {
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
        let append_start_offset = match high {
            Some(high) => high,
            None => 0,
        };

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
        let base_offset = match high {
            Some(high) => high,
            None => 0,
        };

        for (delta, record) in inflated.records.iter().enumerate() {
            let delta = i64::try_from(delta)?;
            let offset = base_offset + delta;
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
            let offset_start = base_offset;
            let offset_end = base_offset + last_offset_delta;

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

        let log_start_offset = match low {
            Some(low) => low,
            None => 0,
        };

        _ = self
            .prepare_execute(
                tx,
                &sql_lookup("watermark_update.sql")?,
                (
                    self.cluster.as_str(),
                    topic,
                    partition,
                    log_start_offset,
                    base_offset + last_offset_delta + 1,
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
                base_offset,
                &inflated,
                config,
            )
            .await
            .inspect(|store| debug!(?store))
            .inspect_err(|err| debug!(?err))?;
        }

        Ok(base_offset)
    }

    pub(super) async fn produce_impl(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        debug!(cluster = self.cluster, transaction_id, ?topition, ?deflated);

        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, &tx)
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(high))
    }
}
