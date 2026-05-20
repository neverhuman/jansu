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

//! A blocking [`Storage`] test double for the storage service tests.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

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
use tokio::sync::Notify;
use url::Url;
use uuid::Uuid;

use crate::{
    AbortedTransactionRange, BrokerRegistrationRequest, GroupDetail, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetFetchRecord, OffsetStage,
    ProducerIdResponse, Result, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
    Version,
};

#[derive(Clone, Debug, Default)]
pub(super) struct BlockingFetchStorage {
    pub(super) gate: Arc<Notify>,
    pub(super) aborted_transaction_ranges: Vec<AbortedTransactionRange>,
}

#[async_trait]
impl Storage for BlockingFetchStorage {
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        todo!()
    }

    async fn create_topic(&self, _topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        todo!()
    }

    async fn incremental_alter_resource(
        &self,
        _resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        todo!()
    }

    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        todo!()
    }

    async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
        todo!()
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        todo!()
    }

    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        _topition: &Topition,
        _batch: deflated::Batch,
    ) -> Result<i64> {
        todo!()
    }

    async fn fetch(
        &self,
        _topition: &Topition,
        _offset: i64,
        _min_bytes: u32,
        _max_bytes: u32,
        _isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        self.gate.notified().await;
        Ok(vec![])
    }

    async fn aborted_transaction_ranges(
        &self,
        _topition: &Topition,
    ) -> Result<Vec<AbortedTransactionRange>> {
        Ok(self.aborted_transaction_ranges.clone())
    }

    async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
        todo!()
    }

    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        _offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        todo!()
    }

    async fn offset_commit(
        &self,
        _group_id: &str,
        _retention_time_ms: Option<Duration>,
        _offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        todo!()
    }

    async fn offset_for_leader_epoch(
        &self,
        _topition: &Topition,
        _leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        todo!()
    }

    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        _topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        todo!()
    }

    async fn offset_fetch_records(
        &self,
        _group_id: Option<&str>,
        _topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        todo!()
    }

    async fn committed_offset_topitions(&self, _group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        todo!()
    }

    async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        todo!()
    }

    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        todo!()
    }

    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        todo!()
    }

    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        todo!()
    }

    async fn describe_config(
        &self,
        _name: &str,
        _resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        todo!()
    }

    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        todo!()
    }

    async fn delete_groups(
        &self,
        _group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        todo!()
    }

    async fn describe_groups(
        &self,
        _group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        todo!()
    }

    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        todo!()
    }

    async fn update_group(
        &self,
        _group_id: &str,
        _detail: GroupDetail,
        _version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        todo!()
    }

    async fn init_producer(
        &self,
        _transaction_id: Option<&str>,
        _transaction_timeout_ms: i32,
        _producer_id: Option<i64>,
        _producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        todo!()
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        todo!()
    }

    async fn txn_add_partitions(
        &self,
        _partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        todo!()
    }

    async fn txn_offset_commit(
        &self,
        _offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        todo!()
    }

    async fn txn_end(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> Result<ErrorCode> {
        todo!()
    }

    async fn cluster_id(&self) -> Result<String> {
        todo!()
    }

    async fn node(&self) -> Result<i32> {
        todo!()
    }

    async fn advertised_listener(&self) -> Result<Url> {
        todo!()
    }

    async fn ping(&self) -> Result<()> {
        todo!()
    }
}
