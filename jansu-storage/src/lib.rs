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
//
//! Jansu Storage Abstraction
//!
//! [`StorageContainer`] provides an abstraction over [`Storage`] and can
//! be configured to use memory, [S3](https://en.wikipedia.org/wiki/Amazon_S3),
//! [SlateDB](https://slatedb.io/) and
//! [RedlineDB](https://github.com/neverhuman/redlineDB/).
//!
//! ## Memory
//!
//! ```
//! # use jansu_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("jansu")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("memory://jansu/")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## S3
//!
//! ```no_run
//! # use jansu_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("jansu")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("s3://jansu/")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## RedlineDB
//!
//! ```no_run
//! # use jansu_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("jansu")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("redlinedb://jansu.redline")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!

use bytes::Bytes;

#[cfg(feature = "dynostore")]
use crate::dynostore::DynoStore;

use opentelemetry::{
    InstrumentationScope, global,
    metrics::{Counter, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;

use std::{
    fmt::{self, Debug, Formatter},
    sync::{Arc, LazyLock},
};

#[cfg(feature = "dynostore")]
mod dynostore;

mod advertised_listener;
pub use advertised_listener::AdvertisedListenerStorage;

mod batch;
mod null;
mod proxy;
mod service;

pub use service::{
    AlterUserScramCredentialsService, ChannelRequestLayer, ChannelRequestService,
    ConsumerGroupDescribeService, CreateAclsService, CreateTopicsService, DeleteGroupsService,
    DeleteRecordsService, DeleteTopicsService, DescribeAclsService, DescribeClusterService,
    DescribeConfigsService, DescribeGroupsService, DescribeTopicPartitionsService,
    DescribeUserScramCredentialsService, FetchService, FindCoordinatorService,
    GetTelemetrySubscriptionsService, IncrementalAlterConfigsService, InitProducerIdService,
    ListGroupsService, ListOffsetsService, ListPartitionReassignmentsService, MetadataService,
    OffsetForLeaderEpochService, ProduceService, Request, RequestChannelService, RequestLayer,
    RequestReceiver, RequestSender, RequestService, RequestStorageService, Response,
    TxnAddOffsetsService, TxnAddPartitionService, TxnOffsetCommitService, bounded_channel,
};

#[cfg(feature = "slatedb")]
pub mod slate;

#[cfg(feature = "redlinedb")]
pub(crate) mod sql;

#[cfg(feature = "redlinedb")]
mod redlinedb;

#[cfg(feature = "dynostore")]
mod gcs;

#[cfg(feature = "dynostore")]
mod os;

mod error;
pub use error::{Error, Result};

mod topition;
pub use topition::{LeaderEpochRecord, Topition, TopitionOffset};

mod offset;
pub(crate) use offset::DEFAULT_OFFSET_RETENTION;
pub use offset::{
    ListOffsetRequest, ListOffsetResponse, OffsetCommitRequest, OffsetFetchRecord, OffsetStage,
};

mod topic;
pub use topic::{BrokerRegistrationRequest, MetadataResponse, TopicId};

#[cfg(feature = "slatedb")]
pub(crate) use jansu_sans_io::NULL_TOPIC_ID;

mod capabilities;
pub use capabilities::{StorageCapabilities, StorageCertification, StorageEngine, StorageFeature};

// Extracted modules
mod group;
pub use group::{
    ConsumerGroupState, GroupDetail, GroupDetailResponse, GroupMember, GroupState, NamedGroupDetail,
};

mod types;
pub use types::{
    PartitionDetail, ProducerIdResponse, TopitionDetail, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState, Version,
};

mod storage_trait;
pub use storage_trait::Storage;

mod storage_ptr_impl;

mod container_builder;
pub use container_builder::{Builder, PhantomBuilder};

mod container_dispatch;
mod container_dispatch_a;
mod container_dispatch_b;
mod container_dispatch_c;

pub type DynStorage = dyn Storage;
pub type ArcDynStorage = Arc<DynStorage>;

/// Conditional Update Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum UpdateError<T> {
    Error(#[from] Error),

    MissingEtag,

    Outdated { current: Box<T>, version: Version },

    SerdeJson(Arc<serde_json::Error>),

    Uuid(#[from] uuid::Error),
}

#[cfg(feature = "redlinedb")]
impl<T> From<::redlinedb::Error> for UpdateError<T> {
    fn from(value: ::redlinedb::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl<T> From<object_store::Error> for UpdateError<T> {
    fn from(value: object_store::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

impl<T> From<serde_json::Error> for UpdateError<T> {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJson(Arc::new(value))
    }
}

/// Storage Container
#[derive(Clone)]
#[cfg_attr(
    not(any(feature = "dynostore", feature = "redlinedb", feature = "slatedb",)),
    allow(missing_copy_implementations)
)]
pub enum StorageContainer {
    Null(null::Engine),

    #[cfg(feature = "dynostore")]
    DynoStore(DynoStore),

    #[cfg(feature = "redlinedb")]
    RedlineDb(redlinedb::Engine),

    #[cfg(feature = "slatedb")]
    Slate(slate::Engine),
}

impl Debug for StorageContainer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null(_) => f.debug_tuple(stringify!(StorageContainer::Null)).finish(),

            #[cfg(feature = "dynostore")]
            Self::DynoStore(_) => f
                .debug_tuple(stringify!(StorageContainer::DynoStore))
                .finish(),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(_) => f
                .debug_tuple(stringify!(StorageContainer::RedlineDb))
                .finish(),

            #[cfg(feature = "slatedb")]
            Self::Slate(_) => f.debug_tuple(stringify!(StorageContainer::Slate)).finish(),
        }
    }
}

impl StorageContainer {
    pub fn builder() -> PhantomBuilder {
        PhantomBuilder::default()
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

pub(crate) static STORAGE_CONTAINER_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_storage_container_requests")
        .with_description("jansu storage container requests")
        .build()
});

pub(crate) static STORAGE_CONTAINER_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_storage_container_errors")
        .with_description("jansu storage container errors")
        .build()
});

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScramCredential {
    pub salt: Bytes,
    pub iterations: i32,
    pub stored_key: Bytes,
    pub server_key: Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn topition_from_str() -> Result<()> {
        let topition = Topition::from_str("qwerty-2147483647")?;
        assert_eq!("qwerty", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }

    #[test]
    fn topic_with_dashes_in_name() -> Result<()> {
        let topition = Topition::from_str("test-topic-0000000-eFC79C8-2147483647")?;
        assert_eq!("test-topic-0000000-eFC79C8", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }
}
