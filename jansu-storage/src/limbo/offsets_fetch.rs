//! Fetch `Storage` operation for the Turso `Engine`.

use super::offsets::integer_or_default;
use super::sql::sql_lookup;
use super::*;

impl Engine {
    pub(super) async fn fetch_impl(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        debug!(?topition, offset, min_bytes, max_bytes, ?isolation_level);
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

        let c = self.connection().await?;

        let mut records = c
            .query(
                &sql_lookup("record_fetch.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    offset,
                    (max_bytes as i64),
                    high_watermark,
                ),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let mut batches = vec![];

        if let Some(row) = records.next().await? {
            let offset_delta = 0;
            let timestamp_delta = 0;

            let record_builder = {
                let mut record_builder = Record::builder()
                    .offset_delta(offset_delta)
                    .timestamp_delta(timestamp_delta)
                    .key(
                        row.get_value(3)
                            .map(|o| o.as_blob().map(|blob| Bytes::copy_from_slice(blob)))
                            .inspect(|k| debug!(?k))
                            .inspect_err(|err| error!(?err))?,
                    )
                    .value(
                        row.get_value(4)
                            .map(|o| o.as_blob().map(|blob| Bytes::copy_from_slice(blob)))
                            .inspect(|v| debug!(?v))
                            .inspect_err(|err| error!(?err))?,
                    );

                let mut headers = c
                    .query(
                        &sql_lookup("header_fetch.sql")?,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            offset,
                        ),
                    )
                    .await?;

                while let Some(header) = headers.next().await? {
                    let mut header_builder = Header::builder();

                    if let Some(k) = header
                        .get_value(0)
                        .map(|value| value.as_blob().cloned())
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.key(Bytes::from(k));
                    }

                    if let Some(v) = header
                        .get_value(1)
                        .map(|value| value.as_blob().cloned())
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.value(Bytes::from(v));
                    }

                    record_builder = record_builder.header(header_builder);
                }

                record_builder
            };

            let mut batch_builder = inflated::Batch::builder()
                .base_offset(
                    row.get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .copied()
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect(|base_offset| debug!(base_offset))
                        .inspect_err(|err| error!(?err))?,
                )
                .attributes(integer_or_default(row.get_value(1).map_err(Error::from)?, 0) as i16)
                .base_timestamp(
                    row.get_value(2)
                        .map_err(Error::from)
                        .and_then(LiteTimestamp::try_from)
                        .and_then(|system_time| to_timestamp(&system_time.0).map_err(Into::into))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_id(integer_or_default(
                    row.get_value(6).map_err(Error::from)?,
                    -1,
                ))
                .producer_epoch(
                    integer_or_default(row.get_value(7).map_err(Error::from)?, -1) as i16,
                )
                .record(record_builder)
                .last_offset_delta(offset_delta);

            while let Some(row) = records.next().await? {
                let attributes =
                    integer_or_default(row.get_value(1).map_err(Error::from)?, 0) as i16;

                let producer_id = integer_or_default(row.get_value(6).map_err(Error::from)?, -1);

                let producer_epoch =
                    integer_or_default(row.get_value(7).map_err(Error::from)?, -1) as i16;

                if batch_builder.attributes != attributes
                    || batch_builder.producer_id != producer_id
                    || batch_builder.producer_epoch != producer_epoch
                {
                    batches.push(batch_builder.build().and_then(TryInto::try_into)?);

                    batch_builder = inflated::Batch::builder()
                        .base_offset(
                            row.get_value(0)
                                .map_err(Error::from)
                                .and_then(|value| {
                                    value
                                        .as_integer()
                                        .copied()
                                        .ok_or(Error::UnexpectedValue(value))
                                })
                                .inspect(|base_offset| debug!(base_offset))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .base_timestamp(
                            row.get_value(2)
                                .map_err(Error::from)
                                .and_then(LiteTimestamp::try_from)
                                .and_then(|system_time| {
                                    to_timestamp(&system_time.0).map_err(Into::into)
                                })
                                .inspect_err(|err| error!(?err))?,
                        )
                        .attributes(attributes)
                        .producer_id(producer_id)
                        .producer_epoch(producer_epoch);
                }

                let offset = row
                    .get_value(0)
                    .map_err(Error::from)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .copied()
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect(|offset| debug!(offset))
                    .inspect_err(|err| error!(?err))?;

                let offset_delta = i32::try_from(offset - batch_builder.base_offset)?;

                let timestamp_delta = row
                    .get_value(2)
                    .map_err(Error::from)
                    .and_then(LiteTimestamp::try_from)
                    .and_then(|system_time| {
                        to_timestamp(&system_time.0)
                            .map(|timestamp| timestamp - batch_builder.base_timestamp)
                            .map_err(Into::into)
                    })
                    .inspect(|timestamp| debug!(?timestamp))
                    .inspect_err(|err| error!(?err))?;

                let record_builder = {
                    let mut record_builder = Record::builder()
                        .offset_delta(offset_delta)
                        .timestamp_delta(timestamp_delta)
                        .key(
                            row.get_value(3)
                                .map(|value| value.as_blob().cloned())
                                .map(|o| o.map(Bytes::from))
                                .inspect(|k| debug!(?k))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .value(
                            row.get_value(4)
                                .map(|value| value.as_blob().cloned())
                                .map(|o| o.map(Bytes::from))
                                .inspect(|v| debug!(?v))
                                .inspect_err(|err| error!(?err))?,
                        );

                    let mut headers = c
                        .query(
                            &sql_lookup("header_fetch.sql")?,
                            (
                                self.cluster.as_str(),
                                topition.topic(),
                                topition.partition(),
                                offset,
                            ),
                        )
                        .await?;

                    while let Some(header) = headers.next().await? {
                        let mut header_builder = Header::builder();

                        if let Some(k) = header
                            .get_value(0)
                            .map(|value| value.as_blob().cloned())
                            .map(|o| o.map(Bytes::from))
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.key(k);
                        }

                        if let Some(v) = header
                            .get_value(1)
                            .map(|value| value.as_blob().cloned())
                            .map(|o| o.map(Bytes::from))
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.value(v);
                        }

                        record_builder = record_builder.header(header_builder);
                    }

                    record_builder
                };

                batch_builder = batch_builder
                    .record(record_builder)
                    .last_offset_delta(offset_delta);
            }

            batches.push(batch_builder.build().and_then(TryInto::try_into)?);
        } else {
            batches.push(
                inflated::Batch::builder()
                    .build()
                    .and_then(TryInto::try_into)?,
            );
        }

        Ok(batches)
    }
}
