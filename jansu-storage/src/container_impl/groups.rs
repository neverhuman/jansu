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

//! Consumer-group dispatchers for [`StorageContainer`].

use jansu_sans_io::{
    delete_groups_response::DeletableGroupResult, list_groups_response::ListedGroup,
};
use opentelemetry::KeyValue;
use tracing::instrument;

use crate::{
    GroupDetail, NamedGroupDetail, Result, STORAGE_CONTAINER_ERRORS, STORAGE_CONTAINER_REQUESTS,
    Storage, StorageContainer, UpdateError, Version,
};

#[instrument(skip_all)]
pub(super) async fn list_groups(
    container: &StorageContainer,
    states_filter: Option<&[String]>,
) -> Result<Vec<ListedGroup>> {
    let attributes = [KeyValue::new("method", "list_groups")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.list_groups(states_filter),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.list_groups(states_filter),

        StorageContainer::Null(engine) => engine.list_groups(states_filter),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.list_groups(states_filter),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.list_groups(states_filter),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.list_groups(states_filter),
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
pub(super) async fn delete_groups(
    container: &StorageContainer,
    group_ids: Option<&[String]>,
) -> Result<Vec<DeletableGroupResult>> {
    let attributes = [KeyValue::new("method", "delete_groups")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.delete_groups(group_ids),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.delete_groups(group_ids),

        StorageContainer::Null(engine) => engine.delete_groups(group_ids),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.delete_groups(group_ids),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.delete_groups(group_ids),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.delete_groups(group_ids),
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
pub(super) async fn describe_groups(
    container: &StorageContainer,
    group_ids: Option<&[String]>,
    include_authorized_operations: bool,
) -> Result<Vec<NamedGroupDetail>> {
    let attributes = [KeyValue::new("method", "describe_groups")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.describe_groups(group_ids, include_authorized_operations)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine.describe_groups(group_ids, include_authorized_operations)
        }

        StorageContainer::Null(engine) => {
            engine.describe_groups(group_ids, include_authorized_operations)
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.describe_groups(group_ids, include_authorized_operations)
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine.describe_groups(group_ids, include_authorized_operations)
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine.describe_groups(group_ids, include_authorized_operations)
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
pub(super) async fn update_group(
    container: &StorageContainer,
    group_id: &str,
    detail: GroupDetail,
    version: Option<Version>,
) -> Result<Version, UpdateError<GroupDetail>> {
    let attributes = [KeyValue::new("method", "update_group")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.update_group(group_id, detail, version),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.update_group(group_id, detail, version),

        StorageContainer::Null(engine) => engine.update_group(group_id, detail, version),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.update_group(group_id, detail, version),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.update_group(group_id, detail, version),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.update_group(group_id, detail, version),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}
