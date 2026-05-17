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
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rama::{Context, Layer as _, Service as _};
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use tokio::{
    fs::rename,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, warn};
use url::Url;
use uuid::Uuid;

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

mod metrics;
use metrics::*;

mod redline;
use redline::{
    Connection, Database, IntoParams, OpenOptions, RedlineError, RedlineErrorCode, Row, Rows,
    Transaction, Value, ValueExt,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Txn {
    name: String,
    producer_id: i64,
    producer_epoch: i16,
    status: TxnState,
}

impl TryFrom<Row> for Txn {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let name = row.get::<String>(0).inspect_err(|err| error!(?err))?;
        let producer_id = row.get::<i64>(1).inspect_err(|err| error!(?err))?;
        let producer_epoch = row.get::<i32>(2).inspect_err(|err| error!(?err))? as i16;
        let status = row
            .get::<Option<String>>(3)
            .map_err(Into::into)
            .and_then(|status| status.map_or(Ok(TxnState::Begin), TxnState::try_from))
            .inspect_err(|err| error!(?err))?;

        Ok(Self {
            name,
            producer_id,
            producer_epoch,
            status,
        })
    }
}

fn is_unique_constraint(error: &RedlineError) -> bool {
    error.code() == RedlineErrorCode::Constraint
}

fn value_to_system_time(value: Value) -> Result<Option<SystemTime>> {
    match value {
        Value::Null => Ok(None),
        other => RedlineTimestamp::try_from(other)
            .map(SystemTime::from)
            .map(Some),
    }
}

/// RedlineDB storage engine
///
#[derive(Clone, Debug)]
pub(crate) struct Delegate {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    pool: Pool,

    schemas: Option<Registry>,

    lake: Option<House>,

    vacuum_into: Option<PathBuf>,

    maintenance: Arc<Semaphore>,
    compaction: CompactionMode,
}

#[derive(Clone)]
pub(crate) struct ConnectionManager {
    db: Database,
    busy_timeout: Duration,
}

impl Debug for ConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager")
            .field("busy_timeout", &self.busy_timeout)
            .finish_non_exhaustive()
    }
}

pub(crate) struct PoolConnection {
    connection: Arc<Mutex<Connection>>,
    _permit: OwnedSemaphorePermit,
}

impl Debug for PoolConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolConnection")
            .field("connection", &self.connection)
            .finish()
    }
}

mod pool;
pub(crate) use pool::Pool;

mod redline_timestamp;
pub(super) use redline_timestamp::RedlineTimestamp;

mod builder;
pub(crate) use builder::Builder;
pub(crate) use builder::SQL;
use builder::{CompactionMode, unique_constraint};

mod delegate_compaction;
mod delegate_helpers;
mod delegate_produce_core;
mod storage_admin;
mod storage_describe;
mod storage_groups;
mod storage_list_offsets;
mod storage_metadata;
mod storage_misc;
mod storage_offsets;
mod storage_produce;
mod storage_producer;
mod storage_transactions;

mod engine;
pub(crate) use engine::Engine;

#[async_trait]
impl Storage for Delegate {
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.delegate_register_broker(broker_registration).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.delegate_brokers().await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.delegate_create_topic(topic, validate_only).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.delegate_delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.delegate_delete_topic(topic).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.delegate_incremental_alter_resource(resource).await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        self.delegate_produce(transaction_id, topition, deflated)
            .await
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        self.delegate_fetch(topition, offset, min_bytes, max_bytes, isolation_level)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.delegate_offset_stage(topition).await
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.delegate_offset_commit(group, retention, offsets).await
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        self.delegate_committed_offset_topitions(group_id).await
    }

    async fn offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        self.delegate_offset_fetch_records(group_id, topics, require_stable)
            .await
    }

    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        self.delegate_offset_for_leader_epoch(topition, leader_epoch)
            .await
    }

    async fn leader_epoch_history(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
        self.delegate_leader_epoch_history(topition).await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.delegate_offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.delegate_list_offsets(isolation_level, offsets).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.delegate_metadata(topics).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.delegate_describe_config(name, resource, keys).await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.delegate_describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.delegate_list_groups(states_filter).await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.delegate_delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.delegate_describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.delegate_update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.delegate_init_producer(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        )
        .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.delegate_txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.delegate_txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.delegate_txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.delegate_txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.delegate_maintain(now).await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.delegate_cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.delegate_node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.delegate_advertised_listener().await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.delegate_delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.delegate_upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.delegate_user_scram_credential(user, mechanism).await
    }

    async fn ping(&self) -> Result<()> {
        self.delegate_ping().await
    }
}
