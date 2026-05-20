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

//! Object-store backend construction for the [`StorageContainer`] [`Builder`].

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
use std::sync::Arc;

use url::Url;

#[cfg(feature = "dynostore")]
use dynostore::DynoStore;

use super::{Builder, StorageContainer};
#[cfg(feature = "dynostore")]
use crate::dynostore;
#[cfg(any(feature = "dynostore", feature = "slatedb"))]
use crate::{Result, Storage};

impl Builder<i32, String, Url, Url> {
    #[cfg(feature = "dynostore")]
    pub(super) fn batching_params(&self) -> (Option<usize>, Option<std::time::Duration>) {
        use std::str::FromStr;

        use tracing::{debug, warn};

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

        (minimum_size, maximum_delay)
    }

    #[cfg(feature = "dynostore")]
    pub(super) fn build_s3(&self) -> Result<Arc<Box<dyn Storage>>> {
        use object_store::aws::{AmazonS3Builder, S3ConditionalPut};

        use crate::batch::ProduceRequestBatcher;

        let bucket_name = self.storage.host_str().unwrap_or("jansu");
        let (minimum_size, maximum_delay) = self.batching_params();

        AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .map(|object_store| {
                DynoStore::new(self.cluster_id.as_str(), self.node_id, object_store)
                    .advertised_listener(self.advertised_listener.clone())
                    .schemas(self.schema_registry.clone())
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
    pub(super) fn build_gs(&self) -> Result<Arc<Box<dyn Storage>>> {
        use std::{num::NonZeroU32, time::Duration};

        use object_store::gcp::GoogleCloudStorageBuilder;

        use crate::{batch::ProduceRequestBatcher, gcs::limit::PutRateLimiter};

        let bucket_name = self.storage.host_str().unwrap_or("jansu");
        let (minimum_size, maximum_delay) = self.batching_params();

        GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket_name)
            .build()
            .map(|object_store| {
                PutRateLimiter::new(object_store, Duration::from_mins(5))
                    .with_rate_per_second(NonZeroU32::new(1))
                    .with_jitter(Some(Duration::from_millis(50)))
            })
            .map(|object_store| {
                DynoStore::new(self.cluster_id.as_str(), self.node_id, object_store)
                    .advertised_listener(self.advertised_listener.clone())
                    .schemas(self.schema_registry.clone())
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

    #[cfg(feature = "slatedb")]
    pub(super) async fn build_slatedb(&self) -> Result<Arc<Box<dyn Storage>>> {
        use slatedb::Db;
        use slatedb::object_store::{
            ObjectStore as SlateObjectStore,
            aws::{AmazonS3Builder as SlateS3Builder, S3ConditionalPut as SlateS3ConditionalPut},
            memory::InMemory as SlateInMemory,
        };

        use crate::{Error, slate};

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
                    .schemas(self.schema_registry.clone())
                    .lake(self.lake_house.clone())
                    .build()
            })
            .map(|storage| Box::new(StorageContainer::Slate(storage)) as Box<dyn Storage>)
            .map(Arc::new)
            .map_err(Into::into)
    }
}
