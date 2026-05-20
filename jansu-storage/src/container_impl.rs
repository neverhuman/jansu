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

//! Storage trait dispatchers for [`StorageContainer`].
//!
//! The [`Storage`] trait implementation for [`StorageContainer`] is kept as
//! one block; each method delegates to a free function grouped by request
//! family in the [`container_impl`](self) submodules.

use std::{collections::BTreeMap, time::SystemTime};

use async_trait::async_trait;
use jansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic, delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup, record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use url::Url;
use uuid::Uuid;

use crate::{
    AbortedTransactionRange, BrokerRegistrationRequest, GroupDetail, LeaderEpochRecord,
    ListOffsetResponse, MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetFetchRecord,
    OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, StorageCapabilities,
    StorageContainer, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};

mod broker;
mod groups;
mod offsets;
mod records;
mod scram;
mod topics;
mod txn;

#[async_trait]
impl Storage for StorageContainer {
    fn capabilities(&self) -> StorageCapabilities {
        broker::capabilities(self)
    }

    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        broker::register_broker(self, broker_registration).await
    }

    async fn offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        offsets::offset_fetch_records(self, group_id, topics, require_stable).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        topics::incremental_alter_resource(self, resource).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        topics::create_topic(self, topic, validate_only).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        topics::delete_records(self, topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        topics::delete_topic(self, topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        broker::brokers(self).await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        records::produce(self, transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        records::fetch(self, topition, offset, min_bytes, max_bytes, isolation).await
    }

    async fn aborted_transaction_ranges(
        &self,
        topition: &Topition,
    ) -> Result<Vec<AbortedTransactionRange>> {
        records::aborted_transaction_ranges(self, topition).await
    }

    async fn fetch_wait(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: std::time::Duration,
    ) -> Result<Vec<deflated::Batch>> {
        records::fetch_wait(
            self, topition, offset, min_bytes, max_bytes, isolation, max_wait,
        )
        .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        records::offset_stage(self, topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        records::list_offsets(self, isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<std::time::Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        offsets::offset_commit(self, group_id, retention_time_ms, offsets).await
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        offsets::committed_offset_topitions(self, group_id).await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        offsets::offset_fetch(self, group_id, topics, require_stable).await
    }

    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        records::offset_for_leader_epoch(self, topition, leader_epoch).await
    }

    async fn leader_epoch_history(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
        records::leader_epoch_history(self, topition).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        topics::metadata(self, topics).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        topics::describe_config(self, name, resource, keys).await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        topics::describe_topic_partitions(self, topics, partition_limit, cursor).await
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        groups::list_groups(self, states_filter).await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        groups::delete_groups(self, group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        groups::describe_groups(self, group_ids, include_authorized_operations).await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        groups::update_group(self, group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        txn::init_producer(
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
        txn::txn_add_offsets(self, transaction_id, producer_id, producer_epoch, group_id).await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        txn::txn_add_partitions(self, partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        txn::txn_offset_commit(self, offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        txn::txn_end(self, transaction_id, producer_id, producer_epoch, committed).await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        broker::maintain(self, now).await
    }

    async fn cluster_id(&self) -> Result<String> {
        broker::cluster_id(self).await
    }

    async fn node(&self) -> Result<i32> {
        broker::node(self).await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        broker::advertised_listener(self).await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        scram::delete_user_scram_credential(self, user, mechanism).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        scram::upsert_user_scram_credential(self, user, mechanism, credential).await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        scram::user_scram_credential(self, user, mechanism).await
    }

    async fn ping(&self) -> Result<()> {
        broker::ping(self).await
    }
}
