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

pub(super) static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_redlinedb_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
        .build()
});

pub(super) static CONNECT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_redlinedb_connect_duration")
        .with_unit("ms")
        .with_description("The connection latencies in milliseconds")
        .build()
});

pub(super) static PRODUCE_IN_TX_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_redlinedb_produce_in_tx_duration")
        .with_unit("ms")
        .with_description("The produce in TX latencies in milliseconds")
        .build()
});

pub(super) static TRANSACTION_COMMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_redlinedb_transaction_commit_duration")
        .with_unit("ms")
        .with_description("The transaction commit latencies in milliseconds")
        .build()
});

pub(super) static ENGINE_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_redlinedb_engine_request_duration")
        .with_unit("ms")
        .with_description("The engine latencies in milliseconds")
        .build()
});

pub(super) static DELEGATE_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_redlinedb_delegate_request_duration")
        .with_boundaries(
            [
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0,
                1000.0,
            ]
            .into(),
        )
        .with_unit("ms")
        .with_description("The engine latencies in milliseconds")
        .build()
});

pub(super) static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_redlinedb_requests")
        .with_description("The number of SQL requests made")
        .build()
});

pub(super) static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_redlinedb_error")
        .with_description("The SQL error count")
        .build()
});

pub(super) fn elapsed_millis(start: SystemTime) -> u64 {
    start
        .elapsed()
        .map_or(0, |duration| duration.as_millis() as u64)
}

use ::redlinedb::metrics::{MetricResult, Metrics};

pub(super) struct JansuMetrics;

impl Metrics for JansuMetrics {
    fn on_query(&self, _sql: &str, duration: Duration, result: MetricResult) {
        SQL_DURATION.record(duration.as_millis() as u64, &[]);
        SQL_REQUESTS.add(1, &[]);
        if result == MetricResult::Err {
            SQL_ERROR.add(1, &[]);
        }
    }

    fn on_execute(
        &self,
        _sql: &str,
        duration: Duration,
        _rows_affected: u64,
        result: MetricResult,
    ) {
        SQL_DURATION.record(duration.as_millis() as u64, &[]);
        SQL_REQUESTS.add(1, &[]);
        if result == MetricResult::Err {
            SQL_ERROR.add(1, &[]);
        }
    }

    fn on_commit(&self, duration: Duration, _result: MetricResult) {
        TRANSACTION_COMMIT_DURATION.record(duration.as_millis() as u64, &[]);
    }

    fn on_pool_acquire(&self, duration: Duration, _result: MetricResult) {
        CONNECT_DURATION.record(duration.as_millis() as u64, &[]);
    }
}
