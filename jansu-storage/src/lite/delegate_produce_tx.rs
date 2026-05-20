//! In-transaction produce path for the libSQL `Delegate`.

use super::*;

impl Delegate {
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
        let append_start_offset = high.unwrap_or(0);

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
                            .unwrap_or(false)
                    })
                    .inspect(|jansu_lake_sink| debug!(jansu_lake_sink))?)
        {
            for (delta, record) in inflated.records.iter().enumerate() {
                debug!(delta, elapsed = elapsed_millis(start));

                let delta = i64::try_from(delta)?;
                let offset = high.unwrap_or(0) + delta;
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
                let offset_start = high.unwrap_or(0);
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
                    low.unwrap_or(0),
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
                high.unwrap_or(0),
                &inflated,
                config,
            )
            .await
            .inspect_err(|err| error!(?err))?;
        }

        debug!(after_all_done = elapsed_millis(start));

        Ok(high.unwrap_or(0)).inspect(|_| {
            PRODUCE_IN_TX_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            );
        })
    }
}
