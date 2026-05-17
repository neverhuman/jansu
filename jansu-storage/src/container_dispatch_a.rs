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

//! Dispatch helpers for StorageContainer (methods A): register_broker through leader_epoch_history.

use jansu_sans_io::{
    ErrorCode, IsolationLevel, ListOffset, create_topics_request::CreatableTopic,
    delete_records_request::DeleteRecordsTopic, delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse, record::deflated,
};
use std::{collections::BTreeMap, time::Duration};
use uuid::Uuid;

use crate::{
    LeaderEpochRecord, ListOffsetResponse, OffsetCommitRequest, OffsetFetchRecord, OffsetStage,
    Result, Storage, StorageContainer, Topition, topic::BrokerRegistrationRequest,
};

impl StorageContainer {
    pub(super) async fn dispatch_register_broker(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.register_broker(broker_registration),

            Self::Null(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.register_broker(broker_registration),
        }
        .await
    }

    pub(super) async fn dispatch_create_topic(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.create_topic(topic, validate_only),

            Self::Null(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.create_topic(topic, validate_only),
        }
        .await
    }

    pub(super) async fn dispatch_incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.incremental_alter_resource(resource),

            Self::Null(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.incremental_alter_resource(resource),
        }
        .await
    }

    pub(super) async fn dispatch_delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_records(topics),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.delete_records(topics),

            Self::Null(engine) => engine.delete_records(topics),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_records(topics),
        }
        .await
    }

    pub(super) async fn dispatch_delete_topic(&self, topic: &crate::TopicId) -> Result<ErrorCode> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_topic(topic),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.delete_topic(topic),

            Self::Null(engine) => engine.delete_topic(topic),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_topic(topic),
        }
        .await
    }

    pub(super) async fn dispatch_brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.brokers(),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.brokers(),

            Self::Null(engine) => engine.brokers(),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.brokers(),
        }
        .await
    }

    pub(super) async fn dispatch_produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.produce(transaction_id, topition, batch),

            Self::Null(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.produce(transaction_id, topition, batch),
        }
        .await
    }

    pub(super) async fn dispatch_fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
            }

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation)
            }

            Self::Null(engine) => engine.fetch(topition, offset, min_bytes, max_bytes, isolation),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.fetch(topition, offset, min_bytes, max_bytes, isolation),
        }
        .await
    }

    pub(super) async fn dispatch_offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_stage(topition),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.offset_stage(topition),

            Self::Null(engine) => engine.offset_stage(topition),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_stage(topition),
        }
        .await
    }

    pub(super) async fn dispatch_list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.list_offsets(isolation_level, offsets),

            Self::Null(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.list_offsets(isolation_level, offsets),
        }
        .await
    }

    pub(super) async fn dispatch_offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            Self::Null(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),
        }
        .await
    }

    pub(super) async fn dispatch_offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.offset_fetch(group_id, topics, require_stable),

            Self::Null(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_fetch(group_id, topics, require_stable),
        }
        .await
    }

    pub(super) async fn dispatch_offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.offset_fetch_records(group_id, topics, require_stable)
            }

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => {
                engine.offset_fetch_records(group_id, topics, require_stable)
            }

            Self::Null(engine) => engine.offset_fetch_records(group_id, topics, require_stable),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_fetch_records(group_id, topics, require_stable),
        }
        .await
    }

    pub(super) async fn dispatch_offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

            Self::Null(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_for_leader_epoch(topition, leader_epoch),
        }
        .await
    }

    pub(super) async fn dispatch_leader_epoch_history(
        &self,
        topition: &Topition,
    ) -> Result<Vec<LeaderEpochRecord>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.leader_epoch_history(topition),

            #[cfg(feature = "redlinedb")]
            Self::RedlineDb(engine) => engine.leader_epoch_history(topition),

            Self::Null(engine) => engine.leader_epoch_history(topition),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.leader_epoch_history(topition),
        }
        .await
    }
}
