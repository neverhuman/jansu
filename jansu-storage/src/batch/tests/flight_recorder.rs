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

//! In-memory [`Storage`] test double recording produced batches.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use jansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    record::{deflated, inflated},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};

#[derive(Clone, Debug, Default)]
pub(super) struct FlightRecorder {
    produced: Arc<Mutex<BTreeMap<Topition, Vec<deflated::Batch>>>>,
}

impl FlightRecorder {
    pub(super) fn new() -> Self {
        Self {
            produced: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(super) fn produced(&self, topition: &Topition) -> Result<Option<Vec<inflated::Batch>>> {
        self.produced
            .as_ref()
            .lock()
            .map_err(Into::into)
            .and_then(|produced| {
                produced
                    .get(topition)
                    .map(|produced| {
                        produced
                            .iter()
                            .map(|deflated| inflated::Batch::try_from(deflated).map_err(Into::into))
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()
            })
    }
}

fn flight_recorder_error(operation: &str) -> Error {
    Error::Message(format!("FlightRecorder cannot service {operation}"))
}

#[async_trait]
impl Storage for FlightRecorder {
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        Err(flight_recorder_error("register_broker"))
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        Err(flight_recorder_error("brokers"))
    }

    async fn create_topic(&self, _topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        Err(flight_recorder_error("create_topic"))
    }

    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        Err(flight_recorder_error("delete_records"))
    }

    async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
        Err(flight_recorder_error("delete_topic"))
    }

    async fn incremental_alter_resource(
        &self,
        _resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        Err(flight_recorder_error("incremental_alter_resource"))
    }

    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        self.produced
            .lock()
            .map(|mut produced| {
                _ = produced
                    .entry(topition.to_owned())
                    .or_default()
                    .push(deflated);

                0
            })
            .map_err(Into::into)
    }

    async fn fetch(
        &self,
        _topition: &Topition,
        _offset: i64,
        _min_bytes: u32,
        _max_bytes: u32,
        _isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        Err(flight_recorder_error("fetch"))
    }

    async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
        Err(flight_recorder_error("offset_stage"))
    }

    async fn offset_commit(
        &self,
        _group: &str,
        _retention: Option<Duration>,
        _offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        Err(flight_recorder_error("offset_commit"))
    }

    async fn committed_offset_topitions(&self, _group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        Err(flight_recorder_error("committed_offset_topitions"))
    }

    async fn offset_for_leader_epoch(
        &self,
        _topition: &Topition,
        _leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        Err(flight_recorder_error("offset_for_leader_epoch"))
    }

    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        _topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        Err(flight_recorder_error("offset_fetch"))
    }

    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        _offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        Err(flight_recorder_error("list_offsets"))
    }

    async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        Err(flight_recorder_error("metadata"))
    }

    async fn describe_config(
        &self,
        _name: &str,
        _resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        Err(flight_recorder_error("describe_config"))
    }

    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        Err(flight_recorder_error("describe_topic_partitions"))
    }

    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        Err(flight_recorder_error("list_groups"))
    }

    async fn delete_groups(
        &self,
        _group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        Err(flight_recorder_error("delete_groups"))
    }

    async fn describe_groups(
        &self,
        _group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        Err(flight_recorder_error("describe_groups"))
    }

    async fn update_group(
        &self,
        _group_id: &str,
        _detail: GroupDetail,
        _version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        Err(UpdateError::Error(flight_recorder_error("update_group")))
    }

    async fn init_producer(
        &self,
        _transaction_id: Option<&str>,
        _transaction_timeout_ms: i32,
        _producer_id: Option<i64>,
        _producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        Err(flight_recorder_error("init_producer"))
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Err(flight_recorder_error("txn_add_offsets"))
    }

    async fn txn_add_partitions(
        &self,
        _partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        Err(flight_recorder_error("txn_add_partitions"))
    }

    async fn txn_offset_commit(
        &self,
        _offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        Err(flight_recorder_error("txn_offset_commit"))
    }

    async fn txn_end(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> Result<ErrorCode> {
        Err(flight_recorder_error("txn_end"))
    }

    async fn maintain(&self, _now: SystemTime) -> Result<()> {
        Err(flight_recorder_error("maintain"))
    }

    async fn cluster_id(&self) -> Result<String> {
        Err(flight_recorder_error("cluster_id"))
    }

    async fn node(&self) -> Result<i32> {
        Err(flight_recorder_error("node"))
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Err(flight_recorder_error("advertised_listener"))
    }

    async fn ping(&self) -> Result<()> {
        Err(flight_recorder_error("ping"))
    }

    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Err(flight_recorder_error("delete_user_scram_credential"))
    }

    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Err(flight_recorder_error("upsert_user_scram_credential"))
    }

    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Err(flight_recorder_error("user_scram_credential"))
    }
}
