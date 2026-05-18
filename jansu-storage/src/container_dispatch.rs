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

//! Thin `impl Storage for StorageContainer` that delegates to dispatch helpers.

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
use opentelemetry::KeyValue;
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};
use tracing::instrument;
use url::Url;
use uuid::Uuid;

use crate::{
    GroupDetail, LeaderEpochRecord, ListOffsetResponse, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetFetchRecord, OffsetStage, ProducerIdResponse, Result,
    STORAGE_CONTAINER_ERRORS, STORAGE_CONTAINER_REQUESTS, ScramCredential, Storage,
    StorageContainer, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version, capabilities::StorageCapabilities,
    topic::BrokerRegistrationRequest,
};

#[async_trait]
impl Storage for StorageContainer {
    fn capabilities(&self) -> StorageCapabilities {
        self.dispatch_capabilities()
    }

    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let a = [KeyValue::new("method", "register_broker")];
        self.dispatch_register_broker(broker_registration)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        let a = [KeyValue::new("method", "offset_fetch_records")];
        self.dispatch_offset_fetch_records(group_id, topics, require_stable)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let a = [KeyValue::new("method", "incremental_alter_resource")];
        self.dispatch_incremental_alter_resource(resource)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let a = [KeyValue::new("method", "create_topic")];
        self.dispatch_create_topic(topic, validate_only)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let a = [KeyValue::new("method", "delete_records")];
        self.dispatch_delete_records(topics)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let a = [KeyValue::new("method", "delete_topic")];
        self.dispatch_delete_topic(topic)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let a = [KeyValue::new("method", "brokers")];
        self.dispatch_brokers()
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let a = [KeyValue::new("method", "produce")];
        self.dispatch_produce(transaction_id, topition, batch)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let a = [KeyValue::new("method", "fetch")];
        self.dispatch_fetch(topition, offset, min_bytes, max_bytes, isolation)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn fetch_wait(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let a = [KeyValue::new("method", "fetch_wait")];
        self.dispatch_fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let a = [KeyValue::new("method", "offset_stage")];
        self.dispatch_offset_stage(topition)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let a = [KeyValue::new("method", "list_offsets")];
        self.dispatch_list_offsets(isolation_level, offsets)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let a = [KeyValue::new("method", "offset_commit")];
        self.dispatch_offset_commit(group_id, retention_time_ms, offsets)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let a = [KeyValue::new("method", "committed_offset_topitions")];
        self.dispatch_committed_offset_topitions(group_id)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let a = [KeyValue::new("method", "offset_fetch")];
        self.dispatch_offset_fetch(group_id, topics, require_stable)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        let a = [KeyValue::new("method", "offset_for_leader_epoch")];
        self.dispatch_offset_for_leader_epoch(topition, leader_epoch)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn leader_epoch_history(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
        let a = [KeyValue::new("method", "leader_epoch_history")];
        self.dispatch_leader_epoch_history(topition)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let a = [KeyValue::new("method", "metadata")];
        self.dispatch_metadata(topics)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let a = [KeyValue::new("method", "describe_config")];
        self.dispatch_describe_config(name, resource, keys)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let a = [KeyValue::new("method", "describe_topic_partitions")];
        self.dispatch_describe_topic_partitions(topics, partition_limit, cursor)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let a = [KeyValue::new("method", "list_groups")];
        self.dispatch_list_groups(states_filter)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let a = [KeyValue::new("method", "delete_groups")];
        self.dispatch_delete_groups(group_ids)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let a = [KeyValue::new("method", "describe_groups")];
        self.dispatch_describe_groups(group_ids, include_authorized_operations)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let a = [KeyValue::new("method", "update_group")];
        self.dispatch_update_group(group_id, detail, version)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let a = [KeyValue::new("method", "init_producer")];
        self.dispatch_init_producer(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        )
        .await
        .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
        .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let a = [KeyValue::new("method", "txn_add_offsets")];
        self.dispatch_txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let a = [KeyValue::new("method", "txn_add_partitions")];
        self.dispatch_txn_add_partitions(partitions)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let a = [KeyValue::new("method", "txn_offset_commit")];
        self.dispatch_txn_offset_commit(offsets)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let a = [KeyValue::new("method", "txn_end")];
        self.dispatch_txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let a = [KeyValue::new("method", "maintain")];
        self.dispatch_maintain(now)
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        self.dispatch_cluster_id().await
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        self.dispatch_node().await
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        self.dispatch_advertised_listener().await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.dispatch_delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.dispatch_upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.dispatch_user_scram_credential(user, mechanism).await
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let a = [KeyValue::new("method", "ping")];
        self.dispatch_ping()
            .await
            .inspect(|_| STORAGE_CONTAINER_REQUESTS.add(1, &a))
            .inspect_err(|_| STORAGE_CONTAINER_ERRORS.add(1, &a))
    }
}
