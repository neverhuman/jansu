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

//! Record produce/fetch and offset-stage dispatchers for [`StorageContainer`].

use std::time::Duration;

use jansu_sans_io::{IsolationLevel, ListOffset, record::deflated};
use opentelemetry::KeyValue;
use tracing::instrument;

use crate::{
    AbortedTransactionRange, LeaderEpochRecord, ListOffsetResponse, OffsetStage, Result,
    STORAGE_CONTAINER_ERRORS, STORAGE_CONTAINER_REQUESTS, Storage, StorageContainer, Topition,
};

#[instrument(skip_all)]
pub(super) async fn produce(
    container: &StorageContainer,
    transaction_id: Option<&str>,
    topition: &Topition,
    batch: deflated::Batch,
) -> Result<i64> {
    let attributes = [KeyValue::new("method", "produce")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.produce(transaction_id, topition, batch),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.produce(transaction_id, topition, batch),

        StorageContainer::Null(engine) => engine.produce(transaction_id, topition, batch),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.produce(transaction_id, topition, batch),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.produce(transaction_id, topition, batch),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.produce(transaction_id, topition, batch),
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
pub(super) async fn fetch(
    container: &StorageContainer,
    topition: &'_ Topition,
    offset: i64,
    min_bytes: u32,
    max_bytes: u32,
    isolation: IsolationLevel,
) -> Result<Vec<deflated::Batch>> {
    let attributes = [KeyValue::new("method", "fetch")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
        }

        StorageContainer::Null(engine) => {
            engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
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
pub(super) async fn aborted_transaction_ranges(
    container: &StorageContainer,
    topition: &Topition,
) -> Result<Vec<AbortedTransactionRange>> {
    let attributes = [KeyValue::new("method", "aborted_transaction_ranges")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.aborted_transaction_ranges(topition),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.aborted_transaction_ranges(topition),

        StorageContainer::Null(engine) => engine.aborted_transaction_ranges(topition),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.aborted_transaction_ranges(topition),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.aborted_transaction_ranges(topition),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.aborted_transaction_ranges(topition),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub(super) async fn fetch_wait(
    container: &StorageContainer,
    topition: &'_ Topition,
    offset: i64,
    min_bytes: u32,
    max_bytes: u32,
    isolation: IsolationLevel,
    max_wait: Duration,
) -> Result<Vec<deflated::Batch>> {
    let attributes = [KeyValue::new("method", "fetch_wait")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
        }

        StorageContainer::Null(engine) => {
            engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
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
pub(super) async fn offset_stage(
    container: &StorageContainer,
    topition: &Topition,
) -> Result<OffsetStage> {
    let attributes = [KeyValue::new("method", "offset_stage")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.offset_stage(topition),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.offset_stage(topition),

        StorageContainer::Null(engine) => engine.offset_stage(topition),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.offset_stage(topition),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.offset_stage(topition),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.offset_stage(topition),
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
pub(super) async fn list_offsets(
    container: &StorageContainer,
    isolation_level: IsolationLevel,
    offsets: &[(Topition, ListOffset)],
) -> Result<Vec<(Topition, ListOffsetResponse)>> {
    let attributes = [KeyValue::new("method", "list_offsets")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.list_offsets(isolation_level, offsets),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.list_offsets(isolation_level, offsets),

        StorageContainer::Null(engine) => engine.list_offsets(isolation_level, offsets),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.list_offsets(isolation_level, offsets),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.list_offsets(isolation_level, offsets),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.list_offsets(isolation_level, offsets),
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
pub(super) async fn offset_for_leader_epoch(
    container: &StorageContainer,
    topition: &Topition,
    leader_epoch: i32,
) -> Result<Option<(i32, i64)>> {
    let attributes = [KeyValue::new("method", "offset_for_leader_epoch")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.offset_for_leader_epoch(topition, leader_epoch)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

        StorageContainer::Null(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.offset_for_leader_epoch(topition, leader_epoch)
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),
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
pub(super) async fn leader_epoch_history(
    container: &StorageContainer,
    topition: &Topition,
) -> Result<Vec<LeaderEpochRecord>> {
    let attributes = [KeyValue::new("method", "leader_epoch_history")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.leader_epoch_history(topition),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.leader_epoch_history(topition),

        StorageContainer::Null(engine) => engine.leader_epoch_history(topition),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.leader_epoch_history(topition),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.leader_epoch_history(topition),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.leader_epoch_history(topition),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}
