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

//! In-transaction produce path for the PostgreSQL backend.

use super::*;

impl Postgres {
    #[instrument(skip_all)]
    pub(super) async fn produce_in_tx(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        tx: &Transaction<'_>,
    ) -> Result<i64> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?topition, ?deflated);

        let topic = topition.topic();
        let partition = topition.partition();

        let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "topition_select_id.sql",
                &[&self.cluster, &topic, &partition],
            )
            .await
            .inspect_err(|err| debug!(?err))?
        else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        let topition_id = row.try_get::<_, i32>(0).inspect_err(|err| error!(?err))?;
        debug!(topition_id);

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, tx)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        let (low, high) = self.watermark_select_for_update(topition, tx).await?;

        debug!(?low, ?high);

        let batch_leader_epoch = deflated.partition_leader_epoch;
        let append_start_offset = high.unwrap_or(0);

        self.maybe_record_leader_epoch_boundary(
            topition,
            batch_leader_epoch,
            append_start_offset,
            tx,
        )
        .await?;

        let inflated = Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

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
            {
                let record_sink = tx.copy_in(self.sql_lookup("record_copy.sql")?).await?;

                let record_column_types = [
                    Type::INT4,
                    Type::INT8,
                    Type::INT2,
                    Type::INT8,
                    Type::INT2,
                    Type::TIMESTAMPTZ,
                    Type::BYTEA,
                    Type::BYTEA,
                ];

                let record_writer = BinaryCopyInWriter::new(record_sink, &record_column_types);
                pin_mut!(record_writer);

                for (delta, record) in inflated.records.iter().enumerate() {
                    let delta = i64::try_from(delta)?;
                    let offset = high.unwrap_or(0) + delta;
                    let attributes = inflated.attributes;
                    let key = record.key.as_deref();
                    let value = record.value.as_deref();

                    let producer_id = transaction_id.and(Some(inflated.producer_id));
                    let producer_epoch = transaction_id.and(Some(inflated.producer_epoch));
                    let ts = to_system_time(inflated.base_timestamp + record.timestamp_delta)?;

                    let mut row: Vec<&(dyn ToSql + Sync)> =
                        Vec::with_capacity(record_column_types.len());

                    row.push(&topition_id);
                    row.push(&offset);
                    row.push(&attributes);
                    row.push(&producer_id);
                    row.push(&producer_epoch);
                    row.push(&ts);
                    row.push(&key);
                    row.push(&value);

                    record_writer
                        .as_mut()
                        .write(&row)
                        .await
                        .inspect_err(|err| {
                            error!(?err, ?topic, ?partition, ?offset, ?key, ?value)
                        })?;
                }

                _ = record_writer
                    .finish()
                    .await
                    .inspect(|record_row_count| debug!(?record_row_count))
                    .inspect_err(|err| error!(?err))?;
            }

            {
                let header_sink = tx.copy_in(self.sql_lookup("header_copy.sql")?).await?;
                let header_column_types = [Type::INT4, Type::INT8, Type::BYTEA, Type::BYTEA];
                let header_writer = BinaryCopyInWriter::new(header_sink, &header_column_types);
                pin_mut!(header_writer);

                for (delta, record) in inflated.records.iter().enumerate() {
                    let delta = i64::try_from(delta)?;
                    let offset = high.unwrap_or(0) + delta;

                    for header in record.headers.iter().as_ref() {
                        let key = header.key.as_deref();
                        let value = header.value.as_deref();

                        let mut row: Vec<&(dyn ToSql + Sync)> =
                            Vec::with_capacity(header_column_types.len());

                        row.push(&topition_id);
                        row.push(&offset);
                        row.push(&key);
                        row.push(&value);

                        header_writer
                            .as_mut()
                            .write(&row)
                            .await
                            .inspect_err(|err| {
                                error!(?err, ?topic, ?partition, ?offset, ?key, ?value)
                            })?;
                    }
                }

                _ = header_writer
                    .finish()
                    .await
                    .inspect(|header_row_count| debug!(?header_row_count))
                    .inspect_err(|err| error!(?err))?;
            }

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                let offset_start = high.unwrap_or(0);
                let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

                _ = self
                .tx_prepare_execute(tx,
                    "txn_produce_offset_insert.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &inflated.producer_id,
                        &inflated.producer_epoch,
                        &topic,
                        &partition,
                        &offset_start,
                        &offset_end,
                    ],
                )
                .await
                .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                .inspect_err(|err| error!(?err))?;
            }
        }

        _ = self
            .tx_prepare_execute(
                tx,
                "watermark_update.sql",
                &[
                    &self.cluster,
                    &topic,
                    &partition,
                    &low.unwrap_or(0),
                    &high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ],
            )
            .await
            .inspect(|n| debug!(?n))
            .inspect_err(|err| error!(?err))?;

        self.lake_store(&attributes, topition, high, &inflated)
            .await?;

        Ok(high.unwrap_or(0))
    }
}
