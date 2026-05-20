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

//! Producer and transaction dispatchers for [`StorageContainer`].

use jansu_sans_io::{ErrorCode, txn_offset_commit_response::TxnOffsetCommitResponseTopic};
use opentelemetry::KeyValue;
use tracing::instrument;

use crate::{
    ProducerIdResponse, Result, STORAGE_CONTAINER_ERRORS, STORAGE_CONTAINER_REQUESTS, Storage,
    StorageContainer, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
};

#[instrument(skip_all)]
pub(super) async fn init_producer(
    container: &StorageContainer,
    transaction_id: Option<&str>,
    transaction_timeout_ms: i32,
    producer_id: Option<i64>,
    producer_epoch: Option<i16>,
) -> Result<ProducerIdResponse> {
    let attributes = [KeyValue::new("method", "init_producer")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.init_producer(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        ),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.init_producer(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        ),

        StorageContainer::Null(engine) => engine.init_producer(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        ),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.init_producer(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        ),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.init_producer(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        ),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.init_producer(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        ),
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
pub(super) async fn txn_add_offsets(
    container: &StorageContainer,
    transaction_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
) -> Result<ErrorCode> {
    let attributes = [KeyValue::new("method", "txn_add_offsets")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
        }

        StorageContainer::Null(engine) => {
            engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
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
pub(super) async fn txn_add_partitions(
    container: &StorageContainer,
    partitions: TxnAddPartitionsRequest,
) -> Result<TxnAddPartitionsResponse> {
    let attributes = [KeyValue::new("method", "txn_add_partitions")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.txn_add_partitions(partitions),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.txn_add_partitions(partitions),

        StorageContainer::Null(engine) => engine.txn_add_partitions(partitions),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.txn_add_partitions(partitions),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.txn_add_partitions(partitions),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.txn_add_partitions(partitions),
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
pub(super) async fn txn_offset_commit(
    container: &StorageContainer,
    offsets: TxnOffsetCommitRequest,
) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
    let attributes = [KeyValue::new("method", "txn_offset_commit")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.txn_offset_commit(offsets),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.txn_offset_commit(offsets),

        StorageContainer::Null(engine) => engine.txn_offset_commit(offsets),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.txn_offset_commit(offsets),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.txn_offset_commit(offsets),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.txn_offset_commit(offsets),
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
pub(super) async fn txn_end(
    container: &StorageContainer,
    transaction_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> Result<ErrorCode> {
    let attributes = [KeyValue::new("method", "txn_end")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
        }

        StorageContainer::Null(engine) => {
            engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
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
