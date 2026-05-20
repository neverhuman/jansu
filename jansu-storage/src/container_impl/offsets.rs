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

//! Consumer-offset dispatchers for [`StorageContainer`].

use std::{collections::BTreeMap, time::Duration};

use jansu_sans_io::ErrorCode;
use opentelemetry::KeyValue;
use tracing::instrument;

use crate::{
    OffsetCommitRequest, OffsetFetchRecord, Result, STORAGE_CONTAINER_ERRORS,
    STORAGE_CONTAINER_REQUESTS, Storage, StorageContainer, Topition,
};

#[instrument(skip_all)]
pub(super) async fn offset_fetch_records(
    container: &StorageContainer,
    group_id: Option<&str>,
    topics: &[Topition],
    require_stable: Option<bool>,
) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
    let attributes = [KeyValue::new("method", "offset_fetch_records")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.offset_fetch_records(group_id, topics, require_stable)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine.offset_fetch_records(group_id, topics, require_stable)
        }

        StorageContainer::Null(engine) => {
            engine.offset_fetch_records(group_id, topics, require_stable)
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.offset_fetch_records(group_id, topics, require_stable)
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine.offset_fetch_records(group_id, topics, require_stable)
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine.offset_fetch_records(group_id, topics, require_stable)
        }
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn offset_commit(
    container: &StorageContainer,
    group_id: &str,
    retention_time_ms: Option<Duration>,
    offsets: &[(Topition, OffsetCommitRequest)],
) -> Result<Vec<(Topition, ErrorCode)>> {
    let attributes = [KeyValue::new("method", "offset_commit")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.offset_commit(group_id, retention_time_ms, offsets)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine.offset_commit(group_id, retention_time_ms, offsets)
        }

        StorageContainer::Null(engine) => {
            engine.offset_commit(group_id, retention_time_ms, offsets)
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.offset_commit(group_id, retention_time_ms, offsets)
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine.offset_commit(group_id, retention_time_ms, offsets)
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine.offset_commit(group_id, retention_time_ms, offsets)
        }
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn committed_offset_topitions(
    container: &StorageContainer,
    group_id: &str,
) -> Result<BTreeMap<Topition, i64>> {
    let attributes = [KeyValue::new("method", "committed_offset_topitions")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.committed_offset_topitions(group_id),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.committed_offset_topitions(group_id),

        StorageContainer::Null(engine) => engine.committed_offset_topitions(group_id),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.committed_offset_topitions(group_id),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.committed_offset_topitions(group_id),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.committed_offset_topitions(group_id),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn offset_fetch(
    container: &StorageContainer,
    group_id: Option<&str>,
    topics: &[Topition],
    require_stable: Option<bool>,
) -> Result<BTreeMap<Topition, i64>> {
    let attributes = [KeyValue::new("method", "offset_fetch")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.offset_fetch(group_id, topics, require_stable)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.offset_fetch(group_id, topics, require_stable),

        StorageContainer::Null(engine) => engine.offset_fetch(group_id, topics, require_stable),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.offset_fetch(group_id, topics, require_stable),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.offset_fetch(group_id, topics, require_stable),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.offset_fetch(group_id, topics, require_stable),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}
