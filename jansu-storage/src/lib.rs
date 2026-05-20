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
//! [PostgreSQL](https://postgresql.org/),
//! [libSQL](https://github.com/tursodatabase/libsql) and
//! [Turso](https://github.com/tursodatabase/turso) (alpha: currently feature locked).
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
//! ## PostgreSQL
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
//!     .storage(Url::parse("postgres://postgres:postgres@localhost")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## libSQL (SQLite)
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
//!     .storage(Url::parse("sqlite://jansu.db")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Turso
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
//!     .storage(Url::parse("turso://jansu.db")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!

#[cfg(feature = "dynostore")]
mod dynostore;

#[cfg(feature = "postgres")]
mod pg;

mod batch;
mod capabilities;
mod container;
mod container_impl;
mod error;
mod groups;
mod metrics;
mod null;
mod offsets;
mod producer;
mod proxy;
mod service;
mod storage_arc;
mod storage_box;
mod storage_trait;
mod topition;
mod update_error;

pub use capabilities::{StorageCapabilities, StorageCertification, StorageEngine, StorageFeature};
pub use container::{Builder, StorageContainer};
pub use error::{AgentFriendly, Error, RepairHint, Result};
pub use groups::{
    ConsumerGroupState, GroupDetail, GroupDetailResponse, GroupMember, GroupState, NamedGroupDetail,
};
pub use offsets::{
    ListOffsetRequest, ListOffsetResponse, OffsetCommitRequest, OffsetFetchRecord, OffsetStage,
};
pub use producer::{
    BrokerRegistrationRequest, MetadataResponse, ProducerIdResponse, ScramCredential,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
};
pub use storage_trait::{ArcDynStorage, DynStorage, Storage};
pub use topition::{
    AbortedTransactionRange, LeaderEpochRecord, PartitionDetail, TopicId, Topition, TopitionDetail,
    TopitionOffset, Version,
};
pub use update_error::UpdateError;

pub(crate) use metrics::{METER, STORAGE_CONTAINER_ERRORS, STORAGE_CONTAINER_REQUESTS};
pub(crate) use offsets::{DEFAULT_OFFSET_RETENTION, append_config_tokens, subtract_config_tokens};

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

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
pub(crate) mod sql;

#[cfg(feature = "libsql")]
mod lite;

#[cfg(feature = "dynostore")]
mod gcs;

#[cfg(feature = "dynostore")]
mod os;

#[cfg(feature = "turso")]
mod limbo;

#[cfg(test)]
mod tests;
