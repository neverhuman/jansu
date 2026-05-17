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

struct RecordRow {
    offset: i64,
    attributes: i16,
    base_timestamp: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    producer_id: i64,
    producer_epoch: i16,
}

impl Delegate {
    pub(super) async fn delegate_produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let start = SystemTime::now();

        let mut pc = self.connection().await?;

        pc.begin(BeginMode::Immediate)
            .map_err(Error::from)
            .inspect(|_| {
                debug!(after_produce_transaction = elapsed_millis(start));
            })?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, &mut pc)
            .await
            .inspect(|_| {
                debug!(after_produce_in_tx = elapsed_millis(start));
            })
            .inspect_err(|err| error!(?err))?;

        pc.commit()
            .map_err(Error::from)
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

    pub(super) async fn delegate_fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let start = SystemTime::now();

        debug!(?topition, offset, min_bytes, max_bytes, ?isolation_level);

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

        let record_rows: Vec<RecordRow> = {
            let mut rows = match key_filter {
                Some(key) => {
                    let key_bytes = key.as_bytes().to_vec();
                    let s = sql("redlinedb/record_fetch_keyed.sql").map_err(Error::from)?;
                    c.query(
                        &s,
                        (
                            self.cluster.as_str(),
                            base_topic,
                            topition.partition(),
                            offset,
                            i64::from(max_bytes),
                            high_watermark,
                            key_bytes,
                        ),
                    )
                    .map_err(Error::from)
                    .inspect_err(|err| error!(?err))?
                }
                None => {
                    let s = sql("redlinedb/record_fetch.sql").map_err(Error::from)?;
                    c.query(
                        &s,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            offset,
                            i64::from(max_bytes),
                            high_watermark,
                        ),
                    )
                    .map_err(Error::from)
                    .inspect_err(|err| error!(?err))?
                }
            };

            let mut out = vec![];
            while let Step::Row(row) = rows.step().map_err(Error::from)? {
                out.push(RecordRow {
                    offset: row
                        .get::<i64>(0)
                        .map_err(Error::from)
                        .inspect(|o| debug!(base_offset = o))
                        .inspect_err(|err| error!(?err))?,
                    attributes: row
                        .get::<Option<i32>>(1)
                        .map_err(Error::from)
                        .map(|a| a.unwrap_or(0))
                        .inspect_err(|err| error!(?err))? as i16,
                    base_timestamp: {
                        let v = row.get::<Value>(2).map_err(Error::from)?;
                        let st = SystemTime::try_from(&v).map_err(Error::from)?;
                        to_timestamp(&st)
                            .map_err(Error::from)
                            .inspect_err(|err| error!(?err))?
                    },
                    key: row
                        .get::<Option<Vec<u8>>>(3)
                        .map_err(Error::from)
                        .map(|o| o.map(Bytes::from))
                        .inspect(|k| debug!(?k))
                        .inspect_err(|err| error!(?err))?,
                    value: row
                        .get::<Option<Vec<u8>>>(4)
                        .map_err(Error::from)
                        .map(|o| o.map(Bytes::from))
                        .inspect(|v| debug!(?v))
                        .inspect_err(|err| error!(?err))?,
                    producer_id: row
                        .get::<Option<i64>>(6)
                        .map_err(Error::from)
                        .map(|p| p.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                    producer_epoch: row
                        .get::<Option<i32>>(7)
                        .map_err(Error::from)
                        .map(|p| p.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))? as i16,
                });
            }
            out
        }; // rows dropped here, c borrow released

        let mut batches = vec![];

        if record_rows.is_empty() {
            batches.push(
                inflated::Batch::builder()
                    .build()
                    .and_then(TryInto::try_into)?,
            );
        } else {
            let first = &record_rows[0];

            let first_headers = {
                let hs = sql("header_fetch.sql").map_err(Error::from)?;
                let mut header_rows = c
                    .query(
                        &hs,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            first.offset,
                        ),
                    )
                    .map_err(Error::from)?;
                let mut headers = vec![];
                while let Step::Row(hr) = header_rows.step().map_err(Error::from)? {
                    headers.push((
                        hr.get::<Option<Vec<u8>>>(0)
                            .map_err(Error::from)
                            .inspect_err(|err| error!(?err))?,
                        hr.get::<Option<Vec<u8>>>(1)
                            .map_err(Error::from)
                            .inspect_err(|err| error!(?err))?,
                    ));
                }
                headers
            };

            let mut record_builder = Record::builder()
                .offset_delta(0)
                .timestamp_delta(0)
                .key(first.key.clone())
                .value(first.value.clone());
            for (k, v) in first_headers {
                let mut hb = Header::builder();
                if let Some(k) = k {
                    hb = hb.key(Bytes::from(k));
                }
                if let Some(v) = v {
                    hb = hb.value(Bytes::from(v));
                }
                record_builder = record_builder.header(hb);
            }

            let mut batch_builder = inflated::Batch::builder()
                .base_offset(first.offset)
                .attributes(first.attributes)
                .base_timestamp(first.base_timestamp)
                .producer_id(first.producer_id)
                .producer_epoch(first.producer_epoch)
                .record(record_builder)
                .last_offset_delta(0);

            for rr in &record_rows[1..] {
                let row_headers = {
                    let hs = sql("header_fetch.sql").map_err(Error::from)?;
                    let mut header_rows = c
                        .query(
                            &hs,
                            (
                                self.cluster.as_str(),
                                topition.topic(),
                                topition.partition(),
                                rr.offset,
                            ),
                        )
                        .map_err(Error::from)?;
                    let mut headers = vec![];
                    while let Step::Row(hr) = header_rows.step().map_err(Error::from)? {
                        headers.push((
                            hr.get::<Option<Vec<u8>>>(0).map_err(Error::from)?,
                            hr.get::<Option<Vec<u8>>>(1).map_err(Error::from)?,
                        ));
                    }
                    headers
                };

                if batch_builder.attributes != rr.attributes
                    || batch_builder.producer_id != rr.producer_id
                    || batch_builder.producer_epoch != rr.producer_epoch
                {
                    batches.push(batch_builder.build().and_then(TryInto::try_into)?);

                    let offset_delta = 0_i32;
                    let mut record_builder = Record::builder()
                        .offset_delta(offset_delta)
                        .timestamp_delta(0)
                        .key(rr.key.clone())
                        .value(rr.value.clone());
                    for (k, v) in row_headers {
                        let mut hb = Header::builder();
                        if let Some(k) = k {
                            hb = hb.key(Bytes::from(k));
                        }
                        if let Some(v) = v {
                            hb = hb.value(Bytes::from(v));
                        }
                        record_builder = record_builder.header(hb);
                    }

                    batch_builder = inflated::Batch::builder()
                        .base_offset(rr.offset)
                        .base_timestamp(rr.base_timestamp)
                        .attributes(rr.attributes)
                        .producer_id(rr.producer_id)
                        .producer_epoch(rr.producer_epoch)
                        .record(record_builder)
                        .last_offset_delta(offset_delta);
                } else {
                    let offset_delta = i32::try_from(rr.offset - batch_builder.base_offset)?;
                    let timestamp_delta = rr.base_timestamp - batch_builder.base_timestamp;

                    let mut record_builder = Record::builder()
                        .offset_delta(offset_delta)
                        .timestamp_delta(timestamp_delta)
                        .key(rr.key.clone())
                        .value(rr.value.clone());
                    for (k, v) in row_headers {
                        let mut hb = Header::builder();
                        if let Some(k) = k {
                            hb = hb.key(Bytes::from(k));
                        }
                        if let Some(v) = v {
                            hb = hb.value(Bytes::from(v));
                        }
                        record_builder = record_builder.header(hb);
                    }

                    batch_builder = batch_builder
                        .record(record_builder)
                        .last_offset_delta(offset_delta);
                }
            }

            batches.push(batch_builder.build().and_then(TryInto::try_into)?);
        }

        Ok(batches).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "fetch")],
            )
        })
    }
}
