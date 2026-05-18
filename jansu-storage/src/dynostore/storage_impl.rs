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

//! `impl Storage for DynoStore` — thin delegators to `*_inner` methods.

use super::*;

#[async_trait]
impl Storage for DynoStore {
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.register_broker_inner(_broker_registration).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.incremental_alter_resource_inner(resource).await
    }

    #[instrument(skip_all, fields(topic = %topic.name))]
    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        self.create_topic_inner(topic, _validate_only).await
    }

    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.delete_records_inner(_topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.delete_topic_inner(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.brokers_inner().await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        self.fetch_inner(topition, offset, min_bytes, max_bytes, isolation_level)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.offset_stage_inner(topition).await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let outcome = self.produce_inner(transaction_id, topition, deflated).await;

        // Wake any `fetch_wait` callers blocked on this topition. Only signal
        // on a successful produce — transactional or idempotent failures
        // (`UnknownProducerId`, `ProducerFenced`, `OutOfOrderSequenceNumber`,
        // `DuplicateSequenceNumber`) leave the log untouched and would cause
        // a waiter to spuriously re-poll for nothing.
        if outcome.is_ok() {
            self.produce_notifier(topition).notify_waiters();
        }

        outcome
    }

    async fn fetch_wait(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        self.fetch_wait_inner(
            topition,
            offset,
            min_bytes,
            max_bytes,
            isolation_level,
            max_wait,
        )
        .await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.list_offsets_inner(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.offset_commit_inner(group_id, retention_time_ms, offsets)
            .await
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        self.committed_offset_topitions_inner(group_id).await
    }

    async fn offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        self.offset_fetch_records_inner(group_id, topics, require_stable)
            .await
    }

    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        self.offset_for_leader_epoch_inner(topition, leader_epoch)
            .await
    }

    async fn leader_epoch_history(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
        self.leader_epoch_history_inner(topition).await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.offset_fetch_inner(group_id, topics, _require_stable)
            .await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.metadata_inner(topics).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.describe_config_inner(name, resource, _keys).await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.describe_topic_partitions_inner(topics, partition_limit, cursor)
            .await
    }

    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.list_groups_inner(_states_filter).await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.delete_groups_inner(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.describe_groups_inner(group_ids, _include_authorized_operations)
            .await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.update_group_inner(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.init_producer_inner(
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
        self.txn_add_offsets_inner(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        request: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.txn_add_partitions_inner(request).await
    }

    async fn txn_offset_commit(
        &self,
        request: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.txn_offset_commit_inner(request).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.txn_end_inner(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.maintain_inner(now).await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.cluster_id_inner().await
    }

    async fn node(&self) -> Result<i32> {
        self.node_inner().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.advertised_listener_inner().await
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        self.delete_user_scram_credential_inner(_user, _mechanism)
            .await
    }

    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        self.upsert_user_scram_credential_inner(_user, _mechanism, _credential)
            .await
    }

    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.user_scram_credential_inner(_user, _mechanism).await
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        self.ping_inner().await
    }
}
