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
    collections::BTreeMap,
    env,
    fmt::Debug,
    marker::PhantomData,
    ops::Deref,
    result,
    str::FromStr as _,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, LeaderEpochRecord, ListOffsetRequest,
    ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetFetchRecord, OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, TopicId,
    Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version,
    sql::{Cache, default_hash, idempotent_sequence_check, remove_comments},
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDateTime;
use jansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsTopicResult, DeleteRecordsPartitionResult},
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
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use tracing::{debug, error};
use turso::{
    Connection, Database, Row, Value, params::IntoParams, transaction::Transaction,
    transaction::TransactionBehavior,
};
use url::Url;
use uuid::Uuid;

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sqlite_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
        .build()
});

static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_sqlite_requests")
        .with_description("The number of SQL requests made")
        .build()
});

static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_sqlite_error")
        .with_description("The SQL error count")
        .build()
});

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
        let name = row
            .get_value(0)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let producer_id = row
            .get_value(1)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let producer_epoch = row
            .get_value(2)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .map(|value| value as i16)
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let status = row
            .get_value(3)
            .map(|value| value.as_text().cloned())
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

/// Turso storage engine
///
#[derive(Clone, Debug)]
pub struct Engine {
    pub(super) cluster: String,
    pub(super) node: i32,
    pub(super) advertised_listener: Url,
    pub(super) db: Arc<Mutex<Database>>,
    pub(super) schemas: Option<Registry>,
    pub(super) lake: Option<House>,
}

impl Engine {
    pub fn builder()
    -> builder::Builder<PhantomData<String>, PhantomData<i32>, PhantomData<Url>, PhantomData<Url>>
    {
        builder::Builder::default()
    }
}

mod builder;
mod engine;
mod produce_txn;
mod storage_broker;
mod storage_describe;
mod storage_end;
mod storage_fetch;
mod storage_groups;
mod storage_metadata;
mod storage_offsets;
mod storage_txn;
mod timestamp;
#[cfg(test)]
mod tests;

use builder::{sql_lookup, unique_constraint};
#[cfg(test)]
use builder::fix_parameters;
use timestamp::LiteTimestamp;

#[async_trait]
impl Storage for Engine {
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        storage_broker::register_broker(self, broker_registration).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        storage_broker::brokers(self).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        storage_broker::create_topic(self, topic, validate_only).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        storage_broker::delete_records(self, topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        storage_broker::delete_topic(self, topic).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        storage_broker::incremental_alter_resource(self, resource).await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        storage_fetch::produce(self, transaction_id, topition, deflated).await
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        storage_fetch::fetch(self, topition, offset, min_bytes, max_bytes, isolation_level).await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        storage_fetch::offset_stage(self, topition).await
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        storage_offsets::offset_commit(self, group, retention, offsets).await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        storage_offsets::committed_offset_topitions(self, group_id).await
    }

    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        storage_offsets::offset_for_leader_epoch(self, topition, leader_epoch).await
    }

    async fn leader_epoch_history(
        &self,
        topition: &Topition,
    ) -> Result<Vec<LeaderEpochRecord>> {
        storage_offsets::leader_epoch_history(self, topition).await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        storage_offsets::offset_fetch(self, group_id, topics, require_stable).await
    }

    async fn offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        storage_offsets::offset_fetch_records(self, group_id, topics, require_stable).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        storage_offsets::list_offsets(self, isolation_level, offsets).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        storage_metadata::metadata(self, topics).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        storage_metadata::describe_config(self, name, resource, keys).await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        storage_describe::describe_topic_partitions(self, topics, partition_limit, cursor).await
    }

    async fn list_groups(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        storage_groups::list_groups(self, states_filter).await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        storage_groups::delete_groups(self, group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        storage_groups::describe_groups(self, group_ids, include_authorized_operations).await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        storage_groups::update_group(self, group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        storage_txn::init_producer(
            self,
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
        storage_txn::txn_add_offsets(self, transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        storage_txn::txn_add_partitions(self, partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        storage_end::txn_offset_commit(self, offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        storage_end::txn_end(self, transaction_id, producer_id, producer_epoch, committed).await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        storage_end::maintain(self, now).await
    }

    async fn cluster_id(&self) -> Result<String> {
        storage_end::cluster_id(self).await
    }

    async fn node(&self) -> Result<i32> {
        storage_end::node(self).await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        storage_end::advertised_listener(self).await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        storage_end::delete_user_scram_credential(self, user, mechanism).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        storage_end::upsert_user_scram_credential(self, user, mechanism, credential).await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        storage_end::user_scram_credential(self, user, mechanism).await
    }

    async fn ping(&self) -> Result<()> {
        storage_end::ping(self).await
    }
}
