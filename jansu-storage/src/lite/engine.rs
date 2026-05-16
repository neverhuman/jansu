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
    marker::PhantomData,
    sync::Arc,
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
    record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use opentelemetry::KeyValue;
use tokio::task::JoinSet;
use tracing::instrument;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, LeaderEpochRecord, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    RequestChannelService, Result, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
    Version,
};

use super::Builder;
use super::metrics::{ENGINE_REQUEST_DURATION, elapsed_millis};

#[derive(Clone, Debug)]
pub struct Engine {
    #[allow(dead_code)]
    pub(super) server: Arc<JoinSet<Result<(), Error>>>,
    pub(super) inner: RequestChannelService,
}

impl Engine {
    pub fn builder()
    -> Builder<PhantomData<String>, PhantomData<i32>, PhantomData<Url>, PhantomData<Url>> {
        Builder::default()
    }
}

#[async_trait]
impl Storage for Engine {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .register_broker(broker_registration)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "register_broker")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();
        self.inner.brokers().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "brokers")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let start = SystemTime::now();
        self.inner
            .create_topic(topic, validate_only)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "create_topic")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let start = SystemTime::now();
        self.inner.delete_records(topics).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_records")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner.delete_topic(topic).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_topic")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let start = SystemTime::now();
        self.inner
            .incremental_alter_resource(resource)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "incremental_alter_resource")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let start = SystemTime::now();
        self.inner
            .produce(transaction_id, topition, deflated)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "produce")],
                )
            })
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
        let start = SystemTime::now();
        self.inner
            .fetch(topition, offset, min_bytes, max_bytes, isolation_level)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "fetch")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();
        self.inner.offset_stage(topition).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_stage")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let start = SystemTime::now();
        self.inner
            .offset_commit(group, retention, offsets)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "offset_commit")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();
        self.inner
            .committed_offset_topitions(group_id)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "committed_offset_topitions")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        let start = SystemTime::now();
        self.inner
            .offset_for_leader_epoch(topition, leader_epoch)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "offset_for_leader_epoch")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn leader_epoch_history(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
        let start = SystemTime::now();
        self.inner
            .leader_epoch_history(topition)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "leader_epoch_history")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();
        self.inner
            .offset_fetch(group_id, topics, require_stable)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "offset_fetch")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();
        self.inner
            .list_offsets(isolation_level, offsets)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "list_offsets")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let start = SystemTime::now();
        self.inner.metadata(topics).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "metadata")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let start = SystemTime::now();
        self.inner
            .describe_config(name, resource, keys)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_config")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let start = SystemTime::now();
        self.inner
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_topic_partitions")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let start = SystemTime::now();
        self.inner.list_groups(states_filter).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "list_groups")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let start = SystemTime::now();
        self.inner.delete_groups(group_ids).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_groups")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();
        self.inner
            .describe_groups(group_ids, include_authorized_operations)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_groups")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let start = SystemTime::now();
        self.inner
            .update_group(group_id, detail, version)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "update_group")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let start = SystemTime::now();
        self.inner
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "init_producer")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "txn_add_offsets")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let start = SystemTime::now();
        self.inner
            .txn_add_partitions(partitions)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "txn_add_partitions")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let start = SystemTime::now();
        self.inner.txn_offset_commit(offsets).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_offset_commit")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "txn_end")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let start = SystemTime::now();
        self.inner.maintain(now).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "maintain")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        let start = SystemTime::now();
        self.inner.cluster_id().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "cluster_id")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        let start = SystemTime::now();
        self.inner.node().await.inspect(|_| {
            ENGINE_REQUEST_DURATION
                .record(elapsed_millis(start), &[KeyValue::new("operation", "node")])
        })
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        let start = SystemTime::now();
        self.inner.advertised_listener().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "advertised_listener")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .delete_user_scram_credential(user, mechanism)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "delete_user_scram_credential")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "upsert_user_scram_credential")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let start = SystemTime::now();
        self.inner
            .user_scram_credential(user, mechanism)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "user_scram_credential")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let start = SystemTime::now();
        self.inner.ping().await.inspect(|_| {
            ENGINE_REQUEST_DURATION
                .record(elapsed_millis(start), &[KeyValue::new("operation", "ping")])
        })
    }
}
