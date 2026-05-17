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

//! Dispatch helpers for StorageContainer (methods C):
//! capabilities, cluster_id, node, advertised_listener,
//! delete_user_scram_credential, upsert_user_scram_credential, user_scram_credential.

use jansu_sans_io::ScramMechanism;
use url::Url;

use crate::{
    Result, ScramCredential, Storage, StorageContainer, capabilities::StorageCapabilities,
};

impl StorageContainer {
    pub(super) fn dispatch_capabilities(&self) -> StorageCapabilities {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(_) => StorageCapabilities::phase06_dynostore(),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(_) => StorageCapabilities::phase06_redlinedb(),

            Self::Null(_) => StorageCapabilities::phase06_null(),

            #[cfg(feature = "postgres")]
            Self::Postgres(_) => StorageCapabilities::phase06_postgres(),

            #[cfg(feature = "slatedb")]
            Self::Slate(_) => StorageCapabilities::phase06_slatedb(),
        }
    }

    pub(super) async fn dispatch_cluster_id(&self) -> Result<String> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.cluster_id().await,

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.cluster_id().await,

            Self::Null(engine) => engine.cluster_id().await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.cluster_id().await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.cluster_id().await,
        }
    }

    pub(super) async fn dispatch_node(&self) -> Result<i32> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.node().await,

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.node().await,

            Self::Null(engine) => engine.node().await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.node().await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.node().await,
        }
    }

    pub(super) async fn dispatch_advertised_listener(&self) -> Result<Url> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.advertised_listener().await,

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.advertised_listener().await,

            Self::Null(engine) => engine.advertised_listener().await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.advertised_listener().await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.advertised_listener().await,
        }
    }

    pub(super) async fn dispatch_delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            Self::Null(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_user_scram_credential(user, mechanism).await,
        }
    }

    pub(super) async fn dispatch_upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            Self::Null(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }
        }
    }

    pub(super) async fn dispatch_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.user_scram_credential(user, mechanism).await,

            Self::Null(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.user_scram_credential(user, mechanism).await,
        }
    }
}
