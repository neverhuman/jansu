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

use super::*;

#[async_trait]
impl Storage for StorageContainer {
    fn capabilities(&self) -> StorageCapabilities {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(_) => StorageCapabilities::phase06_dynostore(),

            #[cfg(feature = "libsql")]
            Self::Lite(_) => StorageCapabilities::phase06_lite(),

            Self::Null(_) => StorageCapabilities::phase06_null(),

            #[cfg(feature = "postgres")]
            Self::Postgres(_) => StorageCapabilities::phase06_postgres(),

            #[cfg(feature = "slatedb")]
            Self::Slate(_) => StorageCapabilities::phase06_slatedb(),

            #[cfg(feature = "turso")]
            Self::Turso(_) => StorageCapabilities::phase06_turso(),
        }
    }

    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let attributes = [KeyValue::new("method", "register_broker")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.register_broker(broker_registration),

            Self::Null(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.register_broker(broker_registration),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        let attributes = [KeyValue::new("method", "offset_fetch_records")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.offset_fetch_records(group_id, topics, require_stable)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.offset_fetch_records(group_id, topics, require_stable),

            Self::Null(engine) => engine.offset_fetch_records(group_id, topics, require_stable),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.offset_fetch_records(group_id, topics, require_stable),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_fetch_records(group_id, topics, require_stable),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.offset_fetch_records(group_id, topics, require_stable),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let attributes = [KeyValue::new("method", "incremental_alter_resource")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.incremental_alter_resource(resource),

            Self::Null(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.incremental_alter_resource(resource),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let attributes = [KeyValue::new("method", "create_topic")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.create_topic(topic, validate_only),

            Self::Null(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.create_topic(topic, validate_only),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let attributes = [KeyValue::new("method", "delete_records")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_records(topics),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.delete_records(topics),

            Self::Null(engine) => engine.delete_records(topics),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.delete_records(topics),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_records(topics),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.delete_records(topics),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let attributes = [KeyValue::new("method", "delete_topic")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_topic(topic),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.delete_topic(topic),

            Self::Null(engine) => engine.delete_topic(topic),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.delete_topic(topic),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_topic(topic),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.delete_topic(topic),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let attributes = [KeyValue::new("method", "brokers")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.brokers(),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.brokers(),

            Self::Null(engine) => engine.brokers(),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.brokers(),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.brokers(),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.brokers(),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let attributes = [KeyValue::new("method", "produce")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.produce(transaction_id, topition, batch),

            Self::Null(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.produce(transaction_id, topition, batch),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
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
        let attributes = [KeyValue::new("method", "fetch")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.fetch(topition, offset, min_bytes, max_bytes, isolation),

            Self::Null(engine) => engine.fetch(topition, offset, min_bytes, max_bytes, isolation),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.fetch(topition, offset, min_bytes, max_bytes, isolation),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.fetch(topition, offset, min_bytes, max_bytes, isolation),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn aborted_transaction_ranges(
        &self,
        topition: &Topition,
    ) -> Result<Vec<AbortedTransactionRange>> {
        let attributes = [KeyValue::new("method", "aborted_transaction_ranges")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.aborted_transaction_ranges(topition),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.aborted_transaction_ranges(topition),

            Self::Null(engine) => engine.aborted_transaction_ranges(topition),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.aborted_transaction_ranges(topition),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.aborted_transaction_ranges(topition),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.aborted_transaction_ranges(topition),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
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
        let attributes = [KeyValue::new("method", "fetch_wait")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => {
                engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            Self::Null(engine) => {
                engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine.fetch_wait(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let attributes = [KeyValue::new("method", "offset_stage")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_stage(topition),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.offset_stage(topition),

            Self::Null(engine) => engine.offset_stage(topition),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.offset_stage(topition),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_stage(topition),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.offset_stage(topition),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let attributes = [KeyValue::new("method", "list_offsets")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.list_offsets(isolation_level, offsets),

            Self::Null(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.list_offsets(isolation_level, offsets),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let attributes = [KeyValue::new("method", "offset_commit")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            Self::Null(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let attributes = [KeyValue::new("method", "committed_offset_topitions")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.committed_offset_topitions(group_id),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.committed_offset_topitions(group_id),

            Self::Null(engine) => engine.committed_offset_topitions(group_id),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.committed_offset_topitions(group_id),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.committed_offset_topitions(group_id),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.committed_offset_topitions(group_id),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let attributes = [KeyValue::new("method", "offset_fetch")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.offset_fetch(group_id, topics, require_stable),

            Self::Null(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.offset_fetch(group_id, topics, require_stable),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        let attributes = [KeyValue::new("method", "offset_for_leader_epoch")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

            Self::Null(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn leader_epoch_history(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
        let attributes = [KeyValue::new("method", "leader_epoch_history")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.leader_epoch_history(topition),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.leader_epoch_history(topition),

            Self::Null(engine) => engine.leader_epoch_history(topition),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.leader_epoch_history(topition),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.leader_epoch_history(topition),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.leader_epoch_history(topition),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let attributes = [KeyValue::new("method", "metadata")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.metadata(topics),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.metadata(topics),

            Self::Null(engine) => engine.metadata(topics),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.metadata(topics),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.metadata(topics),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.metadata(topics),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let attributes = [KeyValue::new("method", "describe_config")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.describe_config(name, resource, keys),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.describe_config(name, resource, keys),

            Self::Null(engine) => engine.describe_config(name, resource, keys),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.describe_config(name, resource, keys),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.describe_config(name, resource, keys),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.describe_config(name, resource, keys),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let attributes = [KeyValue::new("method", "describe_topic_partitions")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.describe_topic_partitions(topics, partition_limit, cursor)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.describe_topic_partitions(topics, partition_limit, cursor),

            Self::Null(engine) => engine.describe_topic_partitions(topics, partition_limit, cursor),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.describe_topic_partitions(topics, partition_limit, cursor)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine.describe_topic_partitions(topics, partition_limit, cursor)
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine.describe_topic_partitions(topics, partition_limit, cursor)
            }
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let attributes = [KeyValue::new("method", "list_groups")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.list_groups(states_filter),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.list_groups(states_filter),

            Self::Null(engine) => engine.list_groups(states_filter),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.list_groups(states_filter),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.list_groups(states_filter),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.list_groups(states_filter),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let attributes = [KeyValue::new("method", "delete_groups")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_groups(group_ids),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.delete_groups(group_ids),

            Self::Null(engine) => engine.delete_groups(group_ids),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.delete_groups(group_ids),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_groups(group_ids),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.delete_groups(group_ids),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let attributes = [KeyValue::new("method", "describe_groups")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.describe_groups(group_ids, include_authorized_operations)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.describe_groups(group_ids, include_authorized_operations),

            Self::Null(engine) => engine.describe_groups(group_ids, include_authorized_operations),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.describe_groups(group_ids, include_authorized_operations)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.describe_groups(group_ids, include_authorized_operations),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.describe_groups(group_ids, include_authorized_operations),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let attributes = [KeyValue::new("method", "update_group")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.update_group(group_id, detail, version),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.update_group(group_id, detail, version),

            Self::Null(engine) => engine.update_group(group_id, detail, version),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.update_group(group_id, detail, version),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.update_group(group_id, detail, version),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.update_group(group_id, detail, version),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
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
        let attributes = [KeyValue::new("method", "init_producer")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            Self::Null(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
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
        let attributes = [KeyValue::new("method", "txn_add_offsets")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            Self::Null(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let attributes = [KeyValue::new("method", "txn_add_partitions")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.txn_add_partitions(partitions),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.txn_add_partitions(partitions),

            Self::Null(engine) => engine.txn_add_partitions(partitions),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.txn_add_partitions(partitions),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.txn_add_partitions(partitions),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.txn_add_partitions(partitions),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let attributes = [KeyValue::new("method", "txn_offset_commit")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.txn_offset_commit(offsets),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.txn_offset_commit(offsets),

            Self::Null(engine) => engine.txn_offset_commit(offsets),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.txn_offset_commit(offsets),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.txn_offset_commit(offsets),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.txn_offset_commit(offsets),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
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
        let attributes = [KeyValue::new("method", "txn_end")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            Self::Null(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let attributes = [KeyValue::new("method", "maintain")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.maintain(now),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.maintain(now),

            Self::Null(engine) => engine.maintain(now),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.maintain(now),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.maintain(now),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.maintain(now),
        }
        .await
        .inspect(|maintain| {
            debug!(?maintain);
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|err| {
            debug!(?err);
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.cluster_id().await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.cluster_id().await,

            Self::Null(engine) => engine.cluster_id().await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.cluster_id().await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.cluster_id().await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.cluster_id().await,
        }
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.node().await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.node().await,

            Self::Null(engine) => engine.node().await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.node().await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.node().await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.node().await,
        }
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.advertised_listener().await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.advertised_listener().await,

            Self::Null(engine) => engine.advertised_listener().await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.advertised_listener().await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.advertised_listener().await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.advertised_listener().await,
        }
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            Self::Null(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.delete_user_scram_credential(user, mechanism).await,
        }
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            Self::Null(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }
        }
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.user_scram_credential(user, mechanism).await,

            Self::Null(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.user_scram_credential(user, mechanism).await,
        }
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let attributes = [KeyValue::new("method", "ping")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.ping(),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.ping(),

            Self::Null(engine) => engine.ping(),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.ping(),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.ping(),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.ping(),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }
}
