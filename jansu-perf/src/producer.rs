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

use std::{
    num::{NonZero, NonZeroU32},
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use governor::{DefaultDirectRateLimiter, Jitter};
use jansu_client::Client;
use jansu_sans_io::{
    ByteSize as _, ErrorCode, ProduceRequest,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use opentelemetry::{KeyValue, metrics::Counter};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};

use crate::{METER, Result};

static RATE_LIMIT_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("rate_limit_duration")
            .with_unit("ms")
            .with_description("Rate limit latencies in milliseconds")
            .build()
    });

static PRODUCE_RECORD_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("produce_record_count")
        .with_description("Produced record count")
        .build()
});

static PRODUCE_API_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("produce_duration")
            .with_unit("ms")
            .with_description("Produce API latencies in milliseconds")
            .build()
    });

#[derive(Clone, Debug)]
pub(crate) struct Producer {
    pub(crate) id: u32,
    pub(crate) rate_limiter: Option<Arc<DefaultDirectRateLimiter>>,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) record_data: Bytes,
    pub(crate) token: CancellationToken,
    pub(crate) client: Client,
    pub(crate) batch_size: NonZero<u32>,
    pub(crate) throughput: Option<u32>,
}

impl Producer {
    #[instrument(skip_all, fields(record_data_len = self.record_data.len()))]
    fn frame(&self) -> Result<deflated::Frame> {
        let mut batch = inflated::Batch::builder();
        let offset_deltas = 0..i32::try_from(self.batch_size.get()).unwrap_or(i32::MAX);

        for offset_delta in offset_deltas {
            batch = batch.record(
                Record::builder()
                    .value(Some(self.record_data.clone()))
                    .offset_delta(offset_delta),
            )
        }

        batch
            .last_offset_delta(i32::try_from(self.batch_size.get()).unwrap_or(i32::MAX))
            .build()
            .map(|batch| inflated::Frame {
                batches: vec![batch],
            })
            .and_then(deflated::Frame::try_from)
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn produce(&self, frame: deflated::Frame) -> Result<()> {
        let req = ProduceRequest::default().topic_data(Some(
            [TopicProduceData::default()
                .name(self.topic.clone())
                .partition_data(Some(
                    [PartitionProduceData::default()
                        .index(self.partition)
                        .records(Some(frame))]
                    .into(),
                ))]
            .into(),
        ));

        let response = self.client.call(req).await?;

        assert!(response.responses.into_iter().flatten().all(|topic| {
            topic
                .partition_responses
                .into_iter()
                .flatten()
                .all(|partition| partition.error_code == i16::from(ErrorCode::None))
        }));

        Ok(())
    }

    #[instrument(skip_all, fields(id = self.id))]
    pub(crate) async fn rate_limited(&self) -> Result<bool> {
        let attributes = [KeyValue::new("producer", self.id.to_string())];

        let frame = self.frame()?;

        if let Some(ref rate_limiter) = self.rate_limiter {
            let rate_limit_start = SystemTime::now();

            let cells =
                self.throughput
                    .and(frame.size_in_bytes().ok().and_then(|bytes| {
                        NonZeroU32::new(u32::try_from(bytes).unwrap_or(u32::MAX))
                    }))
                    .unwrap_or(self.batch_size);

            tokio::select! {
                cancelled = self.token.cancelled() => {
                    debug!(?cancelled);
                    return Ok(false)
                },

                Ok(_) = rate_limiter.until_n_ready_with_jitter(cells, Jitter::up_to(Duration::from_millis(10))) => {
                    RATE_LIMIT_DURATION.record(
                    rate_limit_start
                        .elapsed()
                        .inspect(|duration|debug!(rate_limit_duration_ms = duration.as_millis()))
                        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
                        &attributes)

                },
            }
        }

        let produce_start = SystemTime::now();

        tokio::select! {
            cancelled = self.token.cancelled() => {
                debug!(?cancelled);
                return Ok(false)
            },

            Ok(_) = self.produce(frame) => {
                PRODUCE_RECORD_COUNT.add(u64::from(self.batch_size.get()), &attributes);
                PRODUCE_API_DURATION.record(produce_start.elapsed().inspect(|duration|debug!(produce_duration_ms = duration.as_millis())).map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)), &attributes);
            },
        }

        Ok(!self.token.is_cancelled())
    }
}
