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

//! LibSQL/SQLite storage engine.
//!
//! This module root re-exports the items used across the engine submodules; the
//! engine logic itself is split into sibling modules by concern.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Debug,
    marker::PhantomData,
    ops::Deref,
    path::PathBuf,
    result,
    str::FromStr,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use crate::{
    BrokerRegistrationRequest, ChannelRequestLayer, DEFAULT_OFFSET_RETENTION, Error, GroupDetail,
    LeaderEpochRecord, ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetFetchRecord, OffsetStage, ProducerIdResponse, RequestChannelService,
    RequestStorageService, Result, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version, bounded_channel,
    proxy::SemaphoreProxy,
    sql::{Cache, default_hash, idempotent_sequence_check, remove_comments},
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDateTime;
use deadpool::managed;
use jansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Header, Record, deflated, inflated},
    to_system_time, to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use jansu_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use libsql::{
    Connection, Database, Row, Rows, Statement, Transaction, TransactionBehavior, Value,
    ffi::SQLITE_CONSTRAINT_UNIQUE, params::IntoParams,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rama::{Context, Layer as _, Service as _};
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use tokio::{fs::rename, sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, warn};
use url::Url;
use uuid::Uuid;

/// Reads a SQL file at compile time, stripping comments.
///
/// Defined here (rather than in `cache`) so the `#[cfg(test)] mod tests` child,
/// declared later in this file, can also use it.
macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

mod builder;
mod builder_def;
mod cache;
mod connection;
mod delegate;
mod delegate_compaction;
mod delegate_end;
mod delegate_produce;
mod delegate_produce_tx;
mod delegate_topic;
mod engine;
mod engine_dispatch_a;
mod engine_dispatch_b;
mod metrics;
mod storage;
mod storage_broker;
mod storage_config;
mod storage_describe_tp;
mod storage_fetch;
mod storage_groups;
mod storage_list_offsets;
mod storage_metadata;
mod storage_offsets;
mod storage_produce;
mod storage_producer;
mod storage_topics;
mod storage_txn;
mod timestamp;
mod txn_row;

#[cfg(test)]
mod tests;

use builder::CompactionMode;
use builder_def::Builder;
use cache::DDL;
pub(crate) use cache::SQL;
#[cfg(test)]
use cache::fix_parameters;
use connection::Pool;
use delegate::{
    ConnectionManager, Delegate, PoolConnection, is_unique_constraint, unique_constraint,
    value_to_system_time,
};
use metrics::{
    CONNECT_DURATION, DELEGATE_REQUEST_DURATION, ENGINE_REQUEST_DURATION, PRODUCE_IN_TX_DURATION,
    SQL_DURATION, SQL_ERROR, SQL_REQUESTS, TRANSACTION_COMMIT_DURATION,
    TRANSACTION_WITH_BEHAVIOR_DURATION, elapsed_millis,
};
use timestamp::LiteTimestamp;
use txn_row::Txn;

/// LibSQL/SQLite channel-fronted storage engine.
#[derive(Clone, Debug)]
pub struct Engine {
    #[allow(dead_code)]
    pub(super) server: Arc<JoinSet<Result<(), Error>>>,
    pub(super) inner: RequestChannelService,
}
