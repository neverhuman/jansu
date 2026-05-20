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

//! The runtime-dispatched [`StorageContainer`] enum and its [`Builder`].

use std::{
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    sync::Arc,
};

#[cfg(feature = "dynostore")]
use dynostore::DynoStore;
#[cfg(feature = "postgres")]
use pg::Postgres;

use console::Emoji;
use indicatif::{ProgressBar, ProgressStyle};
use jansu_schema::{Registry, lake::House};
use tokio_util::sync::CancellationToken;
use url::Url;

#[cfg(feature = "dynostore")]
use crate::dynostore;
#[cfg(feature = "turso")]
use crate::limbo;
#[cfg(feature = "libsql")]
use crate::lite;
#[cfg(feature = "postgres")]
use crate::pg;
#[cfg(feature = "slatedb")]
use crate::slate;
use crate::{Error, Result, Storage, null};

mod build_backend;
mod builder;

/// Storage Container
#[derive(Clone)]
#[cfg_attr(
    not(any(
        feature = "dynostore",
        feature = "libsql",
        feature = "postgres",
        feature = "slatedb",
        feature = "turso"
    )),
    allow(missing_copy_implementations)
)]
pub enum StorageContainer {
    Null(null::Engine),

    #[cfg(feature = "postgres")]
    Postgres(Postgres),

    #[cfg(feature = "dynostore")]
    DynoStore(DynoStore),

    #[cfg(feature = "libsql")]
    Lite(lite::Engine),

    #[cfg(feature = "slatedb")]
    Slate(slate::Engine),

    #[cfg(feature = "turso")]
    Turso(limbo::Engine),
}

impl Debug for StorageContainer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null(_) => f.debug_tuple(stringify!(StorageContainer::Null)).finish(),

            #[cfg(feature = "postgres")]
            Self::Postgres(_) => f
                .debug_tuple(stringify!(StorageContainer::Postgres))
                .finish(),

            #[cfg(feature = "dynostore")]
            Self::DynoStore(_) => f
                .debug_tuple(stringify!(StorageContainer::DynoStore))
                .finish(),

            #[cfg(feature = "libsql")]
            Self::Lite(_) => f.debug_tuple(stringify!(StorageContainer::Lite)).finish(),

            #[cfg(feature = "slatedb")]
            Self::Slate(_) => f.debug_tuple(stringify!(StorageContainer::Slate)).finish(),

            #[cfg(feature = "turso")]
            Self::Turso(_) => f.debug_tuple(stringify!(StorageContainer::Turso)).finish(),
        }
    }
}

impl StorageContainer {
    pub fn builder() -> PhantomBuilder {
        PhantomBuilder::default()
    }
}

/// A [`StorageContainer`] builder
#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, A, S> {
    pub(crate) node_id: N,
    pub(crate) cluster_id: C,
    pub(crate) advertised_listener: A,
    pub(crate) storage: S,
    pub(crate) schema_registry: Option<Registry>,
    pub(crate) lake_house: Option<House>,
    pub(crate) silent: bool,

    pub(crate) cancellation: CancellationToken,
}

type PhantomBuilder =
    Builder<PhantomData<i32>, PhantomData<String>, PhantomData<Url>, PhantomData<Url>>;

impl Builder<i32, String, Url, Url> {
    pub async fn build(self) -> Result<Arc<Box<dyn Storage>>> {
        let storage = self.build_storage().await?;

        let pb = if self.silent {
            None
        } else {
            let pb = ProgressBar::new(1);
            pb.set_style(
                ProgressStyle::with_template("[{elapsed}] {bar:40.cyan/blue} {msg}")
                    .unwrap()
                    .progress_chars("##-"),
            );

            pb.set_message("connecting to storage");

            Some(pb)
        };

        storage.ping().await?;

        if let Some(pb) = pb {
            pb.inc(1);
            pb.finish_with_message(format!("{} connected to storage", Emoji("✅", ""),));
        }

        Ok(storage)
    }

    async fn build_storage(&self) -> Result<Arc<Box<dyn Storage>>> {
        match self.storage.scheme() {
            #[cfg(feature = "postgres")]
            "postgres" | "postgresql" => Postgres::builder(self.storage.to_string().as_str())
                .map(|builder| builder.cluster(self.cluster_id.as_str()))
                .map(|builder| builder.node(self.node_id))
                .map(|builder| builder.advertised_listener(self.advertised_listener.clone()))
                .map(|builder| builder.schemas(self.schema_registry.clone()))
                .map(|builder| builder.lake(self.lake_house.clone()))
                .map(|builder| builder.build())
                .map(|storage| Box::new(StorageContainer::Postgres(storage)) as Box<dyn Storage>)
                .map(Arc::new),

            #[cfg(not(feature = "postgres"))]
            "postgres" | "postgresql" => Err(Error::FeatureNotEnabled {
                feature: "postgres".into(),
                message: self.storage.to_string(),
            }),

            #[cfg(feature = "dynostore")]
            "s3" => self.build_s3(),

            #[cfg(feature = "dynostore")]
            "gs" => self.build_gs(),

            #[cfg(feature = "dynostore")]
            "memory" => Ok(DynoStore::new(
                self.cluster_id.as_str(),
                self.node_id,
                object_store::memory::InMemory::new(),
            )
            .advertised_listener(self.advertised_listener.clone())
            .schemas(self.schema_registry.clone())
            .lake(self.lake_house.clone()))
            .map(|storage| Box::new(StorageContainer::DynoStore(storage)) as Box<dyn Storage>)
            .map(Arc::new),

            #[cfg(not(feature = "dynostore"))]
            "s3" | "memory" => Err(Error::FeatureNotEnabled {
                feature: "dynostore".into(),
                message: self.storage.to_string(),
            }),

            #[cfg(feature = "libsql")]
            "sqlite" => {
                lite::Engine::builder()
                    .storage(self.storage.clone())
                    .node(self.node_id)
                    .cluster(self.cluster_id.clone())
                    .advertised_listener(self.advertised_listener.clone())
                    .schemas(self.schema_registry.clone())
                    .lake(self.lake_house.clone())
                    .cancellation(self.cancellation.clone())
                    .build()
                    .await
            }

            #[cfg(not(feature = "libsql"))]
            "sqlite" => Err(Error::FeatureNotEnabled {
                feature: "libsql".into(),
                message: self.storage.to_string(),
            }),

            #[cfg(feature = "slatedb")]
            "slatedb" => self.build_slatedb().await,

            #[cfg(not(feature = "slatedb"))]
            "slatedb" => Err(Error::FeatureNotEnabled {
                feature: "slatedb".into(),
                message: self.storage.to_string(),
            }),

            #[cfg(feature = "turso")]
            "turso" => limbo::Engine::builder()
                .storage(self.storage.clone())
                .node(self.node_id)
                .cluster(self.cluster_id.clone())
                .advertised_listener(self.advertised_listener.clone())
                .schemas(self.schema_registry.clone())
                .lake(self.lake_house.clone())
                .build()
                .await
                .map(|storage| Box::new(StorageContainer::Turso(storage)) as Box<dyn Storage>)
                .map(Arc::new),

            #[cfg(not(feature = "turso"))]
            "turso" => Err(Error::FeatureNotEnabled {
                feature: "turso".into(),
                message: self.storage.to_string(),
            }),

            "null" => Ok(null::Engine::new(
                self.cluster_id.clone(),
                self.node_id,
                self.advertised_listener.clone(),
            ))
            .map(|storage| Box::new(StorageContainer::Null(storage)) as Box<dyn Storage>)
            .map(Arc::new),

            #[cfg(not(any(
                feature = "dynostore",
                feature = "libsql",
                feature = "postgres",
                feature = "slatedb",
                feature = "turso"
            )))]
            _storage => Ok(null::Engine::new(
                self.cluster_id.clone(),
                self.node_id,
                self.advertised_listener.clone(),
            ))
            .map(|storage| Box::new(StorageContainer::Null(storage)) as Box<dyn Storage>)
            .map(Arc::new),

            #[cfg(any(
                feature = "dynostore",
                feature = "libsql",
                feature = "postgres",
                feature = "slatedb",
                feature = "turso"
            ))]
            _unsupported => Err(Error::UnsupportedStorageUrl(self.storage.clone())),
        }
    }
}
