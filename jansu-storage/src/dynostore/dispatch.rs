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

//! Inherent `DynoStore` dispatch methods, split by request family.
//!
//! Each submodule holds the real bodies behind the thin `Storage` trait
//! delegators in `storage.rs`.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    str::FromStr,
    sync::Mutex,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures::{StreamExt, stream::TryStreamExt};
use jansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Record, deflated, inflated},
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use jansu_schema::lake::LakeHouse as _;
use object_store::{
    Attributes, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    path::Path,
};
use rand::{prelude::*, rng};
use tracing::{debug, error, warn};
use url::Url;
use uuid::Uuid;

use super::{
    DynoStore, Group, Offset, OptiCon, Partition, ProducerDetail, Topic, TopicMetadata, Txn,
    TxnCommitOffset, TxnDetail, TxnId, TxnProduceOffset, Watermark, json_content_type,
};
use crate::{
    AbortedTransactionRange, BrokerRegistrationRequest, DEFAULT_OFFSET_RETENTION, Error,
    GroupDetail, LeaderEpochRecord, ListOffsetResponse, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetFetchRecord, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, TxnState, UpdateError, Version,
};

mod broker;
mod cluster_metadata;
mod describe;
mod fetch;
mod groups;
mod offset_fetch;
mod offsets;
mod produce;
mod producer;
mod topics;
mod txn;
mod txn_commit;
mod txn_end;
