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

//! Storage trait dispatchers for the PostgreSQL backend.
//!
//! Extracted from `pg.rs` to keep that file under the workspace shape budget.
//! Each method delegates to an inherent `Postgres` method in the `dispatch`
//! submodules; this keeps the single trait-impl block thin.

use super::*;

#[async_trait]
impl Storage for Postgres {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.register_broker_dispatch(broker_registration).await
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.brokers_dispatch().await
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.create_topic_dispatch(topic, validate_only).await
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.delete_records_dispatch(topics).await
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.delete_topic_dispatch(topic).await
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.incremental_alter_resource_dispatch(resource).await
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        self.produce_dispatch(transaction_id, topition, deflated)
            .await
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        self.fetch_dispatch(topition, offset, min_bytes, max_bytes, isolation_level)
            .await
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.offset_stage_dispatch(topition).await
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.offset_commit_dispatch(group, retention, offsets).await
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        self.committed_offset_topitions_dispatch(group_id).await
    }

    #[instrument(skip_all)]
    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        self.offset_for_leader_epoch_dispatch(topition, leader_epoch)
            .await
    }

    async fn leader_epoch_history(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
        self.leader_epoch_history_dispatch(topition).await
    }

    #[instrument(skip_all)]
    async fn offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        self.offset_fetch_records_dispatch(group_id, topics, require_stable)
            .await
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.offset_fetch_dispatch(group_id, topics, require_stable)
            .await
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.list_offsets_dispatch(isolation_level, offsets).await
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.metadata_dispatch(topics).await
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.describe_config_dispatch(name, resource, keys).await
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.describe_topic_partitions_dispatch(topics, partition_limit, cursor)
            .await
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.list_groups_dispatch(states_filter).await
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.delete_groups_dispatch(group_ids).await
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.describe_groups_dispatch(group_ids, include_authorized_operations)
            .await
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.update_group_dispatch(group_id, detail, version).await
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.init_producer_dispatch(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        )
        .await
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.txn_add_offsets_dispatch(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.txn_add_partitions_dispatch(partitions).await
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.txn_offset_commit_dispatch(offsets).await
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.txn_end_dispatch(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.maintain_dispatch(now).await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.delete_user_scram_credential_dispatch(user, mechanism)
            .await
    }

    async fn upsert_user_scram_credential(
        &self,
        username: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.upsert_user_scram_credential_dispatch(username, mechanism, credential)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.user_scram_credential_dispatch(user, mechanism).await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.cluster_id_dispatch().await
    }

    async fn node(&self) -> Result<i32> {
        self.node_dispatch().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.advertised_listener_dispatch().await
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        self.ping_dispatch().await
    }
}
