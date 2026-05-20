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

//! Produce and fetch dispatch.
//!
//! Inherent `Postgres` methods backing the `Storage` trait implementation.

use super::*;

impl Postgres {
    pub(crate) async fn produce_dispatch(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        debug!(cluster = self.cluster, transaction_id, ?topition, ?deflated);

        let mut c = self.connection().await?;

        let tx = c.transaction().await?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, &tx)
            .await?;

        tx.commit().await?;

        Ok(high)
    }

    pub(crate) async fn fetch_dispatch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let (base_topic, key_filter): (&str, Option<&str>) =
            self.topic_with_key(topition.topic()).await?;

        let high_watermark = self.offset_stage(topition).await.map(|offset_stage| {
            if isolation_level == IsolationLevel::ReadCommitted {
                offset_stage.last_stable
            } else {
                offset_stage.high_watermark
            }
        })?;

        debug!(
            cluster = self.cluster,
            ?topition,
            offset,
            ?isolation_level,
            high_watermark,
            min_bytes,
            max_bytes
        );

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let records = if let Some(key) = key_filter {
            self.tx_prepare_query(
                &tx,
                "record_fetch_pg_keyed.sql",
                &[
                    &self.cluster,
                    &base_topic,
                    &topition.partition(),
                    &offset,
                    &(max_bytes as i64),
                    &high_watermark,
                    &key,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?
        } else {
            self.tx_prepare_query(
                &tx,
                "record_fetch_pg.sql",
                &[
                    &self.cluster,
                    &topition.topic(),
                    &topition.partition(),
                    &offset,
                    &(max_bytes as i64),
                    &high_watermark,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?
        };

        let mut batches = vec![];

        if let Some(first) = records.first() {
            let mut batch_builder = Batch::builder()
                .base_offset(
                    first
                        .try_get::<_, i64>(0)
                        .inspect(|base_offset| debug!(base_offset))
                        .inspect_err(|err| error!(?err))?,
                )
                .attributes(
                    first
                        .try_get::<_, Option<i16>>(1)
                        .map(|attributes| attributes.unwrap_or(0))
                        .inspect_err(|err| error!(?err))?,
                )
                .base_timestamp(
                    first
                        .try_get::<_, SystemTime>(2)
                        .map_err(Error::from)
                        .and_then(|system_time| to_timestamp(&system_time).map_err(Into::into))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_id(
                    first
                        .try_get::<_, Option<i64>>(6)
                        .map(|producer_id| producer_id.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_epoch(
                    first
                        .try_get::<_, Option<i16>>(7)
                        .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                );

            let mut previous_offset = None;

            for record in records.iter() {
                let attributes = record
                    .try_get::<_, Option<i16>>(1)
                    .map(|attributes| attributes.unwrap_or(0))
                    .inspect_err(|err| error!(?err))?;

                let producer_id = record
                    .try_get::<_, Option<i64>>(6)
                    .map(|producer_id| producer_id.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;
                let producer_epoch = record
                    .try_get::<_, Option<i16>>(7)
                    .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;

                let completed = record
                    .try_get::<_, bool>(8)
                    .inspect(|completed| debug!(?completed))
                    .inspect_err(|err| error!(?err))?;

                if batch_builder.attributes != attributes
                    || batch_builder.producer_id != producer_id
                    || batch_builder.producer_epoch != producer_epoch
                {
                    batches.push(batch_builder.build().and_then(TryInto::try_into)?);

                    batch_builder = Batch::builder()
                        .base_offset(
                            record
                                .try_get::<_, i64>(0)
                                .inspect(|base_offset| debug!(base_offset))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .base_timestamp(
                            record
                                .try_get::<_, SystemTime>(2)
                                .map_err(Error::from)
                                .and_then(|system_time| {
                                    to_timestamp(&system_time).map_err(Into::into)
                                })
                                .inspect_err(|err| error!(?err))?,
                        )
                        .attributes(attributes)
                        .producer_id(producer_id)
                        .producer_epoch(producer_epoch);
                }

                let offset = record
                    .try_get::<_, i64>(0)
                    .inspect(|offset| debug!(offset))
                    .inspect_err(|err| error!(?err))?;

                if !completed
                    && previous_offset
                        .inspect(|previous_offset| debug!(previous_offset))
                        .is_none_or(|previous_offset| previous_offset + 1 != offset)
                {
                    break;
                }

                let offset_delta = i32::try_from(offset - batch_builder.base_offset)?;

                let timestamp_delta = record
                    .try_get::<_, SystemTime>(2)
                    .map_err(Error::from)
                    .and_then(|system_time| {
                        to_timestamp(&system_time)
                            .map(|timestamp| timestamp - batch_builder.base_timestamp)
                            .map_err(Into::into)
                    })
                    .inspect(|timestamp| debug!(?timestamp))
                    .inspect_err(|err| error!(?err))?;

                let k = record
                    .try_get::<_, Option<&[u8]>>(3)
                    .map(|o| o.map(Bytes::copy_from_slice))
                    .inspect(|k| debug!(?k))
                    .inspect_err(|err| error!(?err))?;

                let v = record
                    .try_get::<_, Option<&[u8]>>(4)
                    .map(|o| o.map(Bytes::copy_from_slice))
                    .inspect(|v| debug!(?v))
                    .inspect_err(|err| error!(?err))?;

                let mut record_builder = Record::builder()
                    .offset_delta(offset_delta)
                    .timestamp_delta(timestamp_delta)
                    .key(k)
                    .value(v);

                for header in self
                    .tx_prepare_query(
                        &tx,
                        "header_fetch.sql",
                        &[
                            &self.cluster,
                            &topition.topic(),
                            &topition.partition(),
                            &offset,
                        ],
                    )
                    .await
                    .inspect(|row| debug!(?row))
                    .inspect_err(|err| error!(?err))?
                {
                    let mut header_builder = Header::builder();

                    if let Some(k) = header
                        .try_get::<_, Option<&[u8]>>(0)
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.key(Bytes::copy_from_slice(k));
                    }

                    if let Some(v) = header
                        .try_get::<_, Option<&[u8]>>(1)
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.value(Bytes::copy_from_slice(v));
                    }

                    record_builder = record_builder.header(header_builder);
                }

                previous_offset = Some(offset);

                batch_builder = batch_builder
                    .record(record_builder)
                    .last_offset_delta(offset_delta);
            }

            batches.push(batch_builder.build().and_then(TryInto::try_into)?);
        } else {
            batches.push(Batch::builder().build().and_then(TryInto::try_into)?);
        }

        tx.commit().await?;

        debug!(batches_len = batches.len());

        Ok(batches)
    }
}
