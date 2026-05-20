//! `Storage` produce and offset-stage operations for the libSQL `Delegate`.

use super::*;

impl Delegate {
    pub(super) async fn produce_inner(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let start = SystemTime::now();

        let pc = self.connection().await?;

        let tx = pc.transaction().await.inspect(|_| {
            debug!(after_produce_transaction = elapsed_millis(start));
        })?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, &pc)
            .await
            .inspect(|_| {
                debug!(after_produce_in_tx = elapsed_millis(start));
            })
            .inspect_err(|err| error!(?err))?;

        pc.commit(tx)
            .await
            .and(Ok(high))
            .inspect_err(|err| error!(?err))
            .inspect(|_| {
                debug!(after_produce_commit = elapsed_millis(start));

                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "produce")],
                )
            })
    }

    pub(super) async fn offset_stage_inner(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topition);

        let c = self.connection().await?;

        let row = c
            .query_one(
                "watermark_select.sql",
                (
                    self.cluster.as_str(),
                    self.base_topic(topition.topic()).await?,
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?topition, ?err))?;

        let log_start = row
            .get::<Option<i64>>(0)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(0);

        let high_watermark = row
            .get::<Option<i64>>(1)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(0);

        let last_stable = row
            .get::<Option<i64>>(1)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(high_watermark);

        debug!(cluster = self.cluster, ?topition, log_start, high_watermark,);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_stage")],
            )
        })
    }
}
