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

use console::Emoji;
#[cfg(feature = "dynostore")]
use crate::dynostore::DynoStore;
use indicatif::{ProgressBar, ProgressStyle};
#[cfg(feature = "dynostore")]
use object_store::memory::InMemory;
#[cfg(feature = "dynostore")]
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
#[cfg(feature = "postgres")]
use crate::pg::Postgres;
use jansu_schema::{Registry, lake::House};
use std::{
    marker::PhantomData,
    str::FromStr,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;
#[cfg(feature = "dynostore")]
use tracing::warn;
use url::Url;

use crate::{
    Error, Result, Storage, StorageContainer,
    null,
};

#[cfg(feature = "libsql")]
use crate::lite;

#[cfg(feature = "slatedb")]
use crate::slate;

#[cfg(feature = "turso")]
use crate::limbo;

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

pub type PhantomBuilder =
    Builder<PhantomData<i32>, PhantomData<String>, PhantomData<Url>, PhantomData<Url>>;

impl<N, C, A, S> Builder<N, C, A, S> {
    pub fn node_id(self, node_id: i32) -> Builder<i32, C, A, S> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, A, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn advertised_listener(self, advertised_listener: impl Into<Url>) -> Builder<N, C, Url, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn storage(self, storage: Url) -> Builder<N, C, A, Url> {
        debug!(%storage);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Registry>) -> Self {
        _ = schema_registry
            .as_ref()
            .inspect(|schema_registry| debug!(?schema_registry));

        Self {
            schema_registry,
            ..self
        }
    }

    pub fn lake_house(self, lake_house: Option<House>) -> Self {
        _ = lake_house
            .as_ref()
            .inspect(|lake_house| debug!(?lake_house));

        Self { lake_house, ..self }
    }

    pub fn cancellation(self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self
        }
    }

    pub fn silent(self, silent: bool) -> Self {
        Self { silent, ..self }
    }
}

impl Builder<i32, String, Url, Url> {
    pub async fn build(self) -> Result<Arc<Box<dyn Storage>>> {
        let storage = match self.storage.scheme() {
            #[cfg(feature = "postgres")]
            "postgres" | "postgresql" => Postgres::builder(self.storage.to_string().as_str())
                .map(|builder| builder.cluster(self.cluster_id.as_str()))
                .map(|builder| builder.node(self.node_id))
                .map(|builder| builder.advertised_listener(self.advertised_listener.clone()))
                .map(|builder| builder.schemas(self.schema_registry))
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
            "s3" => {
                use crate::batch::ProduceRequestBatcher;

                let bucket_name = self.storage.host_str().unwrap_or("jansu");

                let minimum_size = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_min_size" {
                        human_units::Size::from_str(v.as_ref())
                            .map(|size| size.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                            .and_then(|size| usize::try_from(size).ok())
                    } else {
                        None
                    }
                });

                let maximum_delay = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_max_delay" {
                        human_units::Duration::from_str(v.as_ref())
                            .map(|duration| duration.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                    } else {
                        None
                    }
                });

                debug!(?minimum_size, ?maximum_delay);

                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()
                    .map(|object_store| {
                        DynoStore::new(self.cluster_id.as_str(), self.node_id, object_store)
                            .advertised_listener(self.advertised_listener.clone())
                            .schemas(self.schema_registry)
                            .lake(self.lake_house.clone())
                    })
                    .map(|storage| {
                        ProduceRequestBatcher::new(StorageContainer::DynoStore(storage))
                            .with_minimum_size(minimum_size)
                            .with_maximum_delay(maximum_delay)
                    })
                    .map(|storage| Box::new(storage) as Box<dyn Storage>)
                    .map(Arc::new)
                    .map_err(Into::into)
            }

            #[cfg(feature = "dynostore")]
            "gs" => {
                use std::num::NonZeroU32;

                use object_store::gcp::GoogleCloudStorageBuilder;

                use crate::{batch::ProduceRequestBatcher, gcs::limit::PutRateLimiter};

                let bucket_name = self.storage.host_str().unwrap_or("jansu");

                let minimum_size = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_min_size" {
                        human_units::Size::from_str(v.as_ref())
                            .map(|size| size.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                            .and_then(|size| usize::try_from(size).ok())
                    } else {
                        None
                    }
                });

                let maximum_delay = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_max_delay" {
                        human_units::Duration::from_str(v.as_ref())
                            .map(|duration| duration.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                    } else {
                        None
                    }
                });

                GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket_name)
                    .build()
                    .map(|object_store| {
                        PutRateLimiter::new(object_store, std::time::Duration::from_mins(5))
                            .with_rate_per_second(NonZeroU32::new(1))
                            .with_jitter(Some(std::time::Duration::from_millis(50)))
                    })
                    .map(|object_store| {
                        DynoStore::new(self.cluster_id.as_str(), self.node_id, object_store)
                            .advertised_listener(self.advertised_listener.clone())
                            .schemas(self.schema_registry)
                            .lake(self.lake_house.clone())
                    })
                    .map(|storage| {
                        ProduceRequestBatcher::new(StorageContainer::DynoStore(storage))
                            .with_minimum_size(minimum_size)
                            .with_maximum_delay(maximum_delay)
                    })
                    .map(|storage| Box::new(storage) as Box<dyn Storage>)
                    .map(Arc::new)
                    .map_err(Into::into)
            }

            #[cfg(feature = "dynostore")]
            "memory" => Ok(
                DynoStore::new(self.cluster_id.as_str(), self.node_id, InMemory::new())
                    .advertised_listener(self.advertised_listener.clone())
                    .schemas(self.schema_registry)
                    .lake(self.lake_house.clone()),
            )
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
                    .schemas(self.schema_registry)
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
            "slatedb" => {
                use slatedb::Db;
                use slatedb::object_store::{
                    ObjectStore as SlateObjectStore,
                    aws::{
                        AmazonS3Builder as SlateS3Builder,
                        S3ConditionalPut as SlateS3ConditionalPut,
                    },
                    memory::InMemory as SlateInMemory,
                };

                let host = self.storage.host_str().unwrap_or("jansu");
                let db_path = format!("jansu-{}.slatedb", self.cluster_id);

                // Support memory backend for testing: slatedb://memory
                let object_store: Arc<dyn SlateObjectStore> = if host == "memory" {
                    Arc::new(SlateInMemory::new())
                } else {
                    // Use S3 backend with host as bucket name
                    SlateS3Builder::from_env()
                        .with_bucket_name(host)
                        .with_conditional_put(SlateS3ConditionalPut::ETagMatch)
                        .build()
                        .map(Arc::new)
                        .map_err(|e| Error::Message(e.to_string()))?
                };

                Db::open(db_path, object_store)
                    .await
                    .map(Arc::new)
                    .map(|db| {
                        slate::Engine::builder()
                            .cluster(self.cluster_id.clone())
                            .node(self.node_id)
                            .advertised_listener(self.advertised_listener.clone())
                            .db(db)
                            .schemas(self.schema_registry)
                            .lake(self.lake_house)
                            .build()
                    })
                    .map(|storage| Box::new(StorageContainer::Slate(storage)) as Box<dyn Storage>)
                    .map(Arc::new)
                    .map_err(Into::into)
            }

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
                .schemas(self.schema_registry)
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
        }?;

        let pb = if self.silent {
            None
        } else {
            let pb = ProgressBar::new(1);
            pb.set_style(
                ProgressStyle::with_template("[{elapsed}] {bar:40.cyan/blue} {msg}")
                    .expect("invariant: progress bar template is valid")
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
}
