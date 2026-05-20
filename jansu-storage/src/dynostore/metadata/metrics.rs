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

use std::sync::LazyLock;

use opentelemetry::metrics::Counter;

use crate::METER;

pub(super) static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_objectstore_cache_requests")
        .with_description("object_store cache requests")
        .build()
});

pub(super) static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_objectstore_cache_errors")
        .with_description("object_store cache errors")
        .build()
});

pub(super) static OUTCOMES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_objectstore_cache_outcomes")
        .with_description("object_store cache outcomes")
        .build()
});

pub(super) static ENTRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_objectstore_cache_entries")
        .with_description("object_store cache entries")
        .build()
});
