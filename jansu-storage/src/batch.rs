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

//! [`ProduceRequestBatcher`]: a [`Storage`] adapter that coalesces produce
//! requests within a time/size window before forwarding to the inner storage.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use jansu_sans_io::record::deflated;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
use uuid::Uuid;

use crate::{Error, METER, Result, Storage, Topition};

mod combine;
mod produce;
mod storage_impl;

#[cfg(test)]
mod tests;

pub(crate) use combine::combine;

static BATCH_REQUESTS_LENGTH: LazyLock<Gauge<u64>> =
    LazyLock::new(|| METER.u64_gauge("batch_request_gauge").build());

static BATCH_RESPONSES_LENGTH: LazyLock<Gauge<u64>> =
    LazyLock::new(|| METER.u64_gauge("batch_response_gauge").build());

static BATCH_TICKET_POLL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_ticket_poll")
        .with_description("The number of ticket polls")
        .build()
});

static SEND_QUEUED_PRODUCED_RECORDS_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_send_queued_records")
        .with_description("The number of produced send queued records")
        .build()
});

static SEND_QUEUED_WAKE_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_send_queued_wake_event")
        .with_description("The number of wake events sent")
        .build()
});

static PRODUCE_REQUEST_MINIMUM_SIZE_TRIGGER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_minimum_size_trigger")
        .with_description("The number of times the minimum size was a trigger")
        .build()
});

static PRODUCE_REQUEST_YOUR_TICKET_IS_READY: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_your_ticket_is_ready")
        .with_description("The number of notifications that your ticket was ready while waiting")
        .build()
});

static PRODUCE_REQUEST_TIMEOUT_EXPIRED_TRIGGER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_timeout_expired_trigger")
        .with_description("The number of times the timeout expiry was a trigger")
        .build()
});

static PRODUCE_REQUEST_QUEUED_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_queued")
        .with_description("The number of produce requests queued")
        .build()
});

static PRODUCE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("batch_produce_duration")
        .with_unit("ms")
        .with_description("The batch produce latency in milliseconds")
        .build()
});

#[derive(Clone, Debug)]
struct Ticket<G> {
    id: Uuid,
    batcher: ProduceRequestBatcher<G>,
}

impl<G> Ticket<G> {
    fn new(batcher: ProduceRequestBatcher<G>) -> Self {
        Self {
            id: Uuid::now_v7(),
            batcher,
        }
    }
}

impl<G> AsRef<Uuid> for Ticket<G> {
    fn as_ref(&self) -> &Uuid {
        &self.id
    }
}

impl<G> Future for Ticket<G> {
    type Output = Result<i64, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut responses = self.batcher.responses.lock()?;

        match responses.remove(&self.id) {
            Some(BatchResponse::Response(response)) => {
                BATCH_TICKET_POLL.add(1, &[KeyValue::new("outcome", "ready")]);
                Poll::Ready(Ok(response))
            }
            Some(BatchResponse::Waker(_)) | None => {
                BATCH_TICKET_POLL.add(1, &[KeyValue::new("outcome", "pending")]);
                _ = responses.insert(self.id, BatchResponse::Waker(cx.waker().clone()));
                Poll::Pending
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TopitionProducerId {
    topition: Topition,
    producer_id: i64,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchRequest {
    id: Uuid,
    batch: deflated::Batch,
}

#[derive(Clone, Debug)]
enum BatchResponse {
    Waker(Waker),
    Response(i64),
}

#[derive(Clone, Debug)]
pub(crate) struct ProduceRequestBatcher<G> {
    storage: G,
    maximum_delay: Option<Duration>,
    minimum_size: Option<usize>,

    requests: Arc<Mutex<BTreeMap<TopitionProducerId, Vec<BatchRequest>>>>,
    responses: Arc<Mutex<BTreeMap<Uuid, BatchResponse>>>,
}

impl<G> ProduceRequestBatcher<G> {
    fn update_metrics(&self) -> Result<()> {
        self.requests
            .lock()
            .map_err(Into::into)
            .map(|requests| requests.values().map(|queue| queue.len() as u64).sum())
            .map(|length| BATCH_REQUESTS_LENGTH.record(length, &[]))
            .and(
                self.responses
                    .lock()
                    .map_err(Into::into)
                    .map(|responses| responses.len() as u64)
                    .map(|length| BATCH_RESPONSES_LENGTH.record(length, &[])),
            )
    }
}

impl<G> ProduceRequestBatcher<G>
where
    G: Storage,
{
    pub(crate) fn new(storage: G) -> Self {
        Self {
            storage,
            minimum_size: Default::default(),
            maximum_delay: Default::default(),

            requests: Arc::new(Mutex::new(BTreeMap::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn with_minimum_size(self, minimum_size: Option<usize>) -> Self {
        Self {
            minimum_size,
            ..self
        }
    }

    pub(crate) fn with_maximum_delay(self, maximum_delay: Option<Duration>) -> Self {
        Self {
            maximum_delay,
            ..self
        }
    }
}
