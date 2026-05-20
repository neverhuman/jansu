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

//! SCRAM credential dispatchers for [`StorageContainer`].

use jansu_sans_io::ScramMechanism;

use crate::{Result, ScramCredential, Storage, StorageContainer};

pub(super) async fn delete_user_scram_credential(
    container: &StorageContainer,
    user: &str,
    mechanism: ScramMechanism,
) -> Result<()> {
    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.delete_user_scram_credential(user, mechanism).await
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine.delete_user_scram_credential(user, mechanism).await
        }

        StorageContainer::Null(engine) => {
            engine.delete_user_scram_credential(user, mechanism).await
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.delete_user_scram_credential(user, mechanism).await
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine.delete_user_scram_credential(user, mechanism).await
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine.delete_user_scram_credential(user, mechanism).await
        }
    }
}

pub(super) async fn upsert_user_scram_credential(
    container: &StorageContainer,
    user: &str,
    mechanism: ScramMechanism,
    credential: ScramCredential,
) -> Result<()> {
    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine
                .upsert_user_scram_credential(user, mechanism, credential)
                .await
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine
                .upsert_user_scram_credential(user, mechanism, credential)
                .await
        }

        StorageContainer::Null(engine) => {
            engine
                .upsert_user_scram_credential(user, mechanism, credential)
                .await
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine
                .upsert_user_scram_credential(user, mechanism, credential)
                .await
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine
                .upsert_user_scram_credential(user, mechanism, credential)
                .await
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine
                .upsert_user_scram_credential(user, mechanism, credential)
                .await
        }
    }
}

pub(super) async fn user_scram_credential(
    container: &StorageContainer,
    user: &str,
    mechanism: ScramMechanism,
) -> Result<Option<ScramCredential>> {
    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.user_scram_credential(user, mechanism).await,

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.user_scram_credential(user, mechanism).await,

        StorageContainer::Null(engine) => engine.user_scram_credential(user, mechanism).await,

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.user_scram_credential(user, mechanism).await,

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.user_scram_credential(user, mechanism).await,

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.user_scram_credential(user, mechanism).await,
    }
}
