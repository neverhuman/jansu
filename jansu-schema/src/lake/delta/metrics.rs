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

//! Delta Lake OpenTelemetry metrics

use std::sync::LazyLock;

use opentelemetry::metrics::{Counter, Histogram};

use crate::METER;

pub(super) static RECORD_BATCH_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_record_batch_rows")
        .with_description("The row count of records written in a batch")
        .build()
});

pub(super) static WRITE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_write_duration")
        .with_unit("ms")
        .with_description("The Delta Lake write latencies in milliseconds")
        .build()
});

pub(super) static FLUSH_AND_COMMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_flush_and_commit_duration")
        .with_unit("ms")
        .with_description("Delta Lake record batch flush and commit latency in milliseconds")
        .build()
});

pub(super) static WRITE_WITH_DATAFUSION_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_write_with_datafusion_duration")
        .with_unit("ms")
        .with_description("The Delta Lake write with datafusion latencies in milliseconds")
        .build()
});

pub(super) static RATE_LIMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_rate_limit_duration")
        .with_unit("ms")
        .with_description("Delta Lake Rate limit latencies in milliseconds")
        .build()
});

pub(super) static OPTIMIZE_NUM_FILES_ADDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_num_files_added")
        .with_description("Number of optimized files added")
        .build()
});

pub(super) static OPTIMIZE_NUM_FILES_REMOVED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_num_files_removed")
        .with_description("Number of unoptimized files removed")
        .build()
});

pub(super) static OPTIMIZE_PARTITIONS_OPTIMIZED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_partitions_optimized")
        .with_description("Number of partitions that had at least one file optimized")
        .build()
});

pub(super) static OPTIMIZE_NUM_BATCHES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_num_batches")
        .with_description("The number of batches written")
        .build()
});

pub(super) static OPTIMIZE_TOTAL_CONSIDERED_FILES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_total_considered_files")
        .with_description("How many files were considered during optimization. Not every file considered is optimized")
        .build()
});

pub(super) static OPTIMIZE_TOTAL_FILES_SKIPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_total_files_skipped")
        .with_description("How many files were considered for optimization but were skipped")
        .build()
});
