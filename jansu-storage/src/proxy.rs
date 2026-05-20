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

//! [`SemaphoreProxy`]: a [`Storage`] adapter serialising calls behind a
//! single-permit semaphore.

use std::{
    fmt::Debug,
    sync::{Arc, LazyLock},
    time::SystemTime,
};

use opentelemetry::{KeyValue, metrics::Histogram};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::{METER, Result};

mod operations;
mod storage_impl;

static SEMAPHORE_ACQUIRE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_storage_proxy_semaphore_acquire_duration")
        .with_boundaries(
            [
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0,
                1000.0,
            ]
            .into(),
        )
        .with_unit("ms")
        .with_description("Storage proxy semaphore acquisition duration in ms")
        .build()
});

#[derive(Clone, Debug)]
pub(crate) struct SemaphoreProxy<G> {
    storage: G,
    semaphore: Arc<Semaphore>,
}

impl<G> SemaphoreProxy<G> {
    pub(crate) fn new(storage: G) -> Self {
        Self {
            storage,
            semaphore: Arc::new(Semaphore::new(1)),
        }
    }

    /// Acquire the single semaphore permit, recording acquisition latency for
    /// `operation`.
    async fn permit(&self, operation: &'static str) -> Result<SemaphorePermit<'_>> {
        let start = SystemTime::now();
        self.semaphore
            .acquire()
            .await
            .inspect(|_| {
                SEMAPHORE_ACQUIRE_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", operation)],
                );
            })
            .map_err(Into::into)
    }
}

fn elapsed_millis(start: SystemTime) -> u64 {
    start
        .elapsed()
        .map_or(0, |duration| duration.as_millis() as u64)
}
