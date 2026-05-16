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

//! Dispatch helpers for StorageContainer (methods B):
//! committed_offset_topitions through ping.

use jansu_sans_io::{
    ConfigResource, ErrorCode,
    delete_groups_response::DeletableGroupResult,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    list_groups_response::ListedGroup,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use std::{
    collections::BTreeMap,
    time::SystemTime,
};
use tracing::debug;

use crate::{
    GroupDetail, MetadataResponse, NamedGroupDetail, ProducerIdResponse, Result, Storage,
    StorageContainer, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};

impl StorageContainer {
    pub(super) async fn dispatch_committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
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
    }

    pub(super) async fn dispatch_metadata(
        &self,
        topics: Option<&[TopicId]>,
    ) -> Result<MetadataResponse> {
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
    }

    pub(super) async fn dispatch_describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
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
    }

    pub(super) async fn dispatch_describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
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
    }

    pub(super) async fn dispatch_list_groups(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
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
    }

    pub(super) async fn dispatch_delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
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
    }

    pub(super) async fn dispatch_describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
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
    }

    pub(super) async fn dispatch_update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
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
    }

    pub(super) async fn dispatch_init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
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
    }

    pub(super) async fn dispatch_txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
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
    }

    pub(super) async fn dispatch_txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
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
    }

    pub(super) async fn dispatch_txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
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
    }

    pub(super) async fn dispatch_txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
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
    }

    pub(super) async fn dispatch_maintain(&self, now: SystemTime) -> Result<()> {
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
        })
        .inspect_err(|err| {
            debug!(?err);
        })
    }

    pub(super) async fn dispatch_ping(&self) -> Result<()> {
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
    }
}
