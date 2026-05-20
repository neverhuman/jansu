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

//! Broker registration, lifecycle and identity dispatchers for [`StorageContainer`].

use std::time::SystemTime;

use jansu_sans_io::describe_cluster_response::DescribeClusterBroker;
use opentelemetry::KeyValue;
use tracing::{debug, instrument};
use url::Url;

use crate::{
    BrokerRegistrationRequest, Result, STORAGE_CONTAINER_ERRORS, STORAGE_CONTAINER_REQUESTS,
    Storage, StorageCapabilities, StorageContainer,
};

pub(super) fn capabilities(container: &StorageContainer) -> StorageCapabilities {
    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(_) => StorageCapabilities::phase06_dynostore(),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(_) => StorageCapabilities::phase06_lite(),

        StorageContainer::Null(_) => StorageCapabilities::phase06_null(),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(_) => StorageCapabilities::phase06_postgres(),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(_) => StorageCapabilities::phase06_slatedb(),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(_) => StorageCapabilities::phase06_turso(),
    }
}

#[instrument(skip_all)]
pub(super) async fn register_broker(
    container: &StorageContainer,
    broker_registration: BrokerRegistrationRequest,
) -> Result<()> {
    let attributes = [KeyValue::new("method", "register_broker")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.register_broker(broker_registration),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.register_broker(broker_registration),

        StorageContainer::Null(engine) => engine.register_broker(broker_registration),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.register_broker(broker_registration),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.register_broker(broker_registration),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.register_broker(broker_registration),
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
pub(super) async fn brokers(container: &StorageContainer) -> Result<Vec<DescribeClusterBroker>> {
    let attributes = [KeyValue::new("method", "brokers")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.brokers(),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.brokers(),

        StorageContainer::Null(engine) => engine.brokers(),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.brokers(),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.brokers(),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.brokers(),
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
pub(super) async fn maintain(container: &StorageContainer, now: SystemTime) -> Result<()> {
    let attributes = [KeyValue::new("method", "maintain")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.maintain(now),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.maintain(now),

        StorageContainer::Null(engine) => engine.maintain(now),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.maintain(now),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.maintain(now),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.maintain(now),
    }
    .await
    .inspect(|maintain| {
        debug!(?maintain);
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|err| {
        debug!(?err);
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn cluster_id(container: &StorageContainer) -> Result<String> {
    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.cluster_id().await,

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.cluster_id().await,

        StorageContainer::Null(engine) => engine.cluster_id().await,

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.cluster_id().await,

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.cluster_id().await,

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.cluster_id().await,
    }
}

#[instrument(skip_all)]
pub(super) async fn node(container: &StorageContainer) -> Result<i32> {
    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.node().await,

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.node().await,

        StorageContainer::Null(engine) => engine.node().await,

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.node().await,

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.node().await,

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.node().await,
    }
}

#[instrument(skip_all)]
pub(super) async fn advertised_listener(container: &StorageContainer) -> Result<Url> {
    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.advertised_listener().await,

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.advertised_listener().await,

        StorageContainer::Null(engine) => engine.advertised_listener().await,

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.advertised_listener().await,

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.advertised_listener().await,

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.advertised_listener().await,
    }
}

#[instrument(skip_all)]
pub(super) async fn ping(container: &StorageContainer) -> Result<()> {
    let attributes = [KeyValue::new("method", "ping")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.ping(),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.ping(),

        StorageContainer::Null(engine) => engine.ping(),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.ping(),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.ping(),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.ping(),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}
