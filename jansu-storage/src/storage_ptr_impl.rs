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
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use url::Url;
use uuid::Uuid;

use crate::{
    GroupDetail, LeaderEpochRecord, ListOffsetResponse, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetFetchRecord, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version, capabilities::StorageCapabilities,
    topic::BrokerRegistrationRequest,
};

macro_rules! impl_storage_for_ptr {
    ($Ptr:ident) => {
        #[async_trait]
        impl<T> Storage for $Ptr<T>
        where
            T: Storage + ?Sized,
        {
            fn capabilities(&self) -> StorageCapabilities {
                self.as_ref().capabilities()
            }

            async fn register_broker(
                &self,
                broker_registration: BrokerRegistrationRequest,
            ) -> Result<()> {
                self.as_ref().register_broker(broker_registration).await
            }

            async fn create_topic(
                &self,
                topic: CreatableTopic,
                validate_only: bool,
            ) -> Result<Uuid> {
                self.as_ref().create_topic(topic, validate_only).await
            }

            async fn incremental_alter_resource(
                &self,
                resource: AlterConfigsResource,
            ) -> Result<AlterConfigsResourceResponse> {
                self.as_ref().incremental_alter_resource(resource).await
            }

            async fn delete_records(
                &self,
                topics: &[DeleteRecordsTopic],
            ) -> Result<Vec<DeleteRecordsTopicResult>> {
                self.as_ref().delete_records(topics).await
            }

            async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
                self.as_ref().delete_topic(topic).await
            }

            async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
                self.as_ref().brokers().await
            }

            async fn produce(
                &self,
                transaction_id: Option<&str>,
                topition: &Topition,
                batch: deflated::Batch,
            ) -> Result<i64> {
                self.as_ref().produce(transaction_id, topition, batch).await
            }

            async fn fetch(
                &self,
                topition: &'_ Topition,
                offset: i64,
                min_bytes: u32,
                max_bytes: u32,
                isolation: IsolationLevel,
            ) -> Result<Vec<deflated::Batch>> {
                self.as_ref()
                    .fetch(topition, offset, min_bytes, max_bytes, isolation)
                    .await
            }

            async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
                self.as_ref().offset_stage(topition).await
            }

            async fn list_offsets(
                &self,
                isolation_level: IsolationLevel,
                offsets: &[(Topition, ListOffset)],
            ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
                self.as_ref().list_offsets(isolation_level, offsets).await
            }

            async fn offset_commit(
                &self,
                group_id: &str,
                retention_time_ms: Option<Duration>,
                offsets: &[(Topition, OffsetCommitRequest)],
            ) -> Result<Vec<(Topition, ErrorCode)>> {
                self.as_ref()
                    .offset_commit(group_id, retention_time_ms, offsets)
                    .await
            }

            async fn offset_fetch(
                &self,
                group_id: Option<&str>,
                topics: &[Topition],
                require_stable: Option<bool>,
            ) -> Result<BTreeMap<Topition, i64>> {
                self.as_ref()
                    .offset_fetch(group_id, topics, require_stable)
                    .await
            }

            async fn offset_fetch_records(
                &self,
                group_id: Option<&str>,
                topics: &[Topition],
                require_stable: Option<bool>,
            ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
                self.as_ref()
                    .offset_fetch_records(group_id, topics, require_stable)
                    .await
            }

            async fn offset_for_leader_epoch(
                &self,
                topition: &Topition,
                leader_epoch: i32,
            ) -> Result<Option<(i32, i64)>> {
                self.as_ref()
                    .offset_for_leader_epoch(topition, leader_epoch)
                    .await
            }

            async fn leader_epoch_history(
                &self,
                topition: &Topition,
            ) -> Result<Vec<LeaderEpochRecord>> {
                self.as_ref().leader_epoch_history(topition).await
            }

            async fn committed_offset_topitions(
                &self,
                group_id: &str,
            ) -> Result<BTreeMap<Topition, i64>> {
                self.as_ref().committed_offset_topitions(group_id).await
            }

            async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
                self.as_ref().metadata(topics).await
            }

            async fn upsert_user_scram_credential(
                &self,
                user: &str,
                mechanism: ScramMechanism,
                credential: ScramCredential,
            ) -> Result<()> {
                self.as_ref()
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            async fn delete_user_scram_credential(
                &self,
                user: &str,
                mechanism: ScramMechanism,
            ) -> Result<()> {
                self.as_ref()
                    .delete_user_scram_credential(user, mechanism)
                    .await
            }

            async fn user_scram_credential(
                &self,
                user: &str,
                mechanism: ScramMechanism,
            ) -> Result<Option<ScramCredential>> {
                self.as_ref().user_scram_credential(user, mechanism).await
            }

            async fn describe_config(
                &self,
                name: &str,
                resource: ConfigResource,
                keys: Option<&[String]>,
            ) -> Result<DescribeConfigsResult> {
                self.as_ref().describe_config(name, resource, keys).await
            }

            async fn list_groups(
                &self,
                states_filter: Option<&[String]>,
            ) -> Result<Vec<ListedGroup>> {
                self.as_ref().list_groups(states_filter).await
            }

            async fn delete_groups(
                &self,
                group_ids: Option<&[String]>,
            ) -> Result<Vec<DeletableGroupResult>> {
                self.as_ref().delete_groups(group_ids).await
            }

            async fn describe_groups(
                &self,
                group_ids: Option<&[String]>,
                include_authorized_operations: bool,
            ) -> Result<Vec<NamedGroupDetail>> {
                self.as_ref()
                    .describe_groups(group_ids, include_authorized_operations)
                    .await
            }

            async fn describe_topic_partitions(
                &self,
                topics: Option<&[TopicId]>,
                partition_limit: i32,
                cursor: Option<Topition>,
            ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
                self.as_ref()
                    .describe_topic_partitions(topics, partition_limit, cursor)
                    .await
            }

            async fn update_group(
                &self,
                group_id: &str,
                detail: GroupDetail,
                version: Option<Version>,
            ) -> Result<Version, UpdateError<GroupDetail>> {
                self.as_ref().update_group(group_id, detail, version).await
            }

            async fn init_producer(
                &self,
                transaction_id: Option<&str>,
                transaction_timeout_ms: i32,
                producer_id: Option<i64>,
                producer_epoch: Option<i16>,
            ) -> Result<ProducerIdResponse> {
                self.as_ref()
                    .init_producer(
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
                self.as_ref()
                    .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
                    .await
            }

            async fn txn_add_partitions(
                &self,
                partitions: TxnAddPartitionsRequest,
            ) -> Result<TxnAddPartitionsResponse> {
                self.as_ref().txn_add_partitions(partitions).await
            }

            async fn txn_offset_commit(
                &self,
                offsets: TxnOffsetCommitRequest,
            ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
                self.as_ref().txn_offset_commit(offsets).await
            }

            async fn txn_end(
                &self,
                transaction_id: &str,
                producer_id: i64,
                producer_epoch: i16,
                committed: bool,
            ) -> Result<ErrorCode> {
                self.as_ref()
                    .txn_end(transaction_id, producer_id, producer_epoch, committed)
                    .await
            }

            async fn maintain(&self, now: SystemTime) -> Result<()> {
                self.as_ref().maintain(now).await
            }

            async fn cluster_id(&self) -> Result<String> {
                self.as_ref().cluster_id().await
            }

            async fn node(&self) -> Result<i32> {
                self.as_ref().node().await
            }

            async fn advertised_listener(&self) -> Result<Url> {
                self.as_ref().advertised_listener().await
            }

            async fn ping(&self) -> Result<()> {
                self.as_ref().ping().await
            }
        }
    };
}

impl_storage_for_ptr!(Arc);
impl_storage_for_ptr!(Box);
