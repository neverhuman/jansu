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

//! Permit-guarded storage operations for [`SemaphoreProxy`].

use std::{collections::BTreeMap, time::Duration};

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
use uuid::Uuid;

use super::SemaphoreProxy;
use crate::{
    BrokerRegistrationRequest, GroupDetail, ListOffsetResponse, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage,
    TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    UpdateError, Version,
};

impl<G> SemaphoreProxy<G>
where
    G: Storage + Clone,
{
    pub(super) async fn proxy_register_broker(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        let _permit = self.permit("register_broker").await?;
        self.storage.register_broker(broker_registration).await
    }

    pub(super) async fn proxy_create_topic(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        let _permit = self.permit("create_topic").await?;
        self.storage.create_topic(topic, validate_only).await
    }

    pub(super) async fn proxy_incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let _permit = self.permit("incremental_alter_resource").await?;
        self.storage.incremental_alter_resource(resource).await
    }

    pub(super) async fn proxy_delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let _permit = self.permit("delete_records").await?;
        self.storage.delete_records(topics).await
    }

    pub(super) async fn proxy_delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let _permit = self.permit("delete_topic").await?;
        self.storage.delete_topic(topic).await
    }

    pub(super) async fn proxy_brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let _permit = self.permit("brokers").await?;
        self.storage.brokers().await
    }

    pub(super) async fn proxy_produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let _permit = self.permit("produce").await?;
        self.storage
            .produce(transaction_id, topition, deflated)
            .await
    }

    pub(super) async fn proxy_fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let _permit = self.permit("fetch").await?;
        self.storage
            .fetch(topition, offset, min_bytes, max_bytes, isolation)
            .await
    }

    pub(super) async fn proxy_fetch_wait(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        // Hold a single permit for the whole long-poll window — `fetch_wait`
        // is logically one client request, even if the inner engine wakes
        // multiple times via `Notify`.
        let _permit = self.permit("fetch_wait").await?;
        self.storage
            .fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    pub(super) async fn proxy_offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let _permit = self.permit("offset_stage").await?;
        self.storage.offset_stage(topition).await
    }

    pub(super) async fn proxy_list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let _permit = self.permit("list_offsets").await?;
        self.storage.list_offsets(isolation_level, offsets).await
    }

    pub(super) async fn proxy_offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let _permit = self.permit("offset_commit").await?;
        self.storage
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    pub(super) async fn proxy_offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let _permit = self.permit("offset_fetch").await?;
        self.storage
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    pub(super) async fn proxy_committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        let _permit = self.permit("committed_offset_topitions").await?;
        self.storage.committed_offset_topitions(group_id).await
    }

    pub(super) async fn proxy_metadata(
        &self,
        topics: Option<&[TopicId]>,
    ) -> Result<MetadataResponse> {
        let _permit = self.permit("metadata").await?;
        self.storage.metadata(topics).await
    }

    pub(super) async fn proxy_upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let _permit = self.permit("upsert_user_scram_credential").await?;
        self.storage
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    pub(super) async fn proxy_delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let _permit = self.permit("delete_user_scram_credential").await?;
        self.storage
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    pub(super) async fn proxy_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let _permit = self.permit("user_scram_credential").await?;
        self.storage.user_scram_credential(user, mechanism).await
    }

    pub(super) async fn proxy_describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let _permit = self.permit("describe_config").await?;
        self.storage.describe_config(name, resource, keys).await
    }

    pub(super) async fn proxy_list_groups(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        let _permit = self.permit("list_groups").await?;
        self.storage.list_groups(states_filter).await
    }

    pub(super) async fn proxy_delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let _permit = self.permit("delete_groups").await?;
        self.storage.delete_groups(group_ids).await
    }

    pub(super) async fn proxy_describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let _permit = self.permit("describe_groups").await?;
        self.storage
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    pub(super) async fn proxy_describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let _permit = self.permit("describe_topic_partitions").await?;
        self.storage
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    pub(super) async fn proxy_update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let _permit = self
            .permit("update_group")
            .await
            .map_err(UpdateError::Error)?;
        self.storage.update_group(group_id, detail, version).await
    }

    pub(super) async fn proxy_init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let _permit = self.permit("init_producer").await?;
        self.storage
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    pub(super) async fn proxy_txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let _permit = self.permit("txn_add_offsets").await?;
        self.storage
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    pub(super) async fn proxy_txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let _permit = self.permit("txn_add_partitions").await?;
        self.storage.txn_add_partitions(partitions).await
    }

    pub(super) async fn proxy_txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let _permit = self.permit("txn_offset_commit").await?;
        self.storage.txn_offset_commit(offsets).await
    }

    pub(super) async fn proxy_txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let _permit = self.permit("txn_end").await?;
        self.storage
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }
}
