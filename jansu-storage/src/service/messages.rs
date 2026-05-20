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

//! The [`Request`] / [`Response`] message types carried over the storage channel.

use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    time::{Duration, SystemTime},
};

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
use url::Url;
use uuid::Uuid;

use crate::{
    AbortedTransactionRange, BrokerRegistrationRequest, GroupDetail, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetFetchRecord, OffsetStage,
    ProducerIdResponse, Result, ScramCredential, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Request {
    RegisterBroker(BrokerRegistrationRequest),
    IncrementalAlterResource(AlterConfigsResource),
    CreateTopic {
        topic: CreatableTopic,
        validate_only: bool,
    },
    DeleteRecords(Vec<DeleteRecordsTopic>),
    DeleteTopic(TopicId),
    Brokers,
    Produce {
        transaction_id: Option<String>,
        topition: Topition,
        batch: deflated::Batch,
    },
    Fetch {
        topition: Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    },
    AbortedTransactionRanges(Topition),
    OffsetStage(Topition),
    ListOffsets {
        isolation_level: IsolationLevel,
        offsets: Vec<(Topition, ListOffset)>,
    },
    OffsetCommit {
        group_id: String,
        retention_time_ms: Option<Duration>,
        offsets: Vec<(Topition, OffsetCommitRequest)>,
    },
    CommittedOffsetTopitions(String),
    OffsetFetch {
        group_id: Option<String>,
        topics: Vec<Topition>,
        require_stable: Option<bool>,
    },
    OffsetForLeaderEpoch {
        topition: Topition,
        leader_epoch: i32,
    },
    Metadata(Option<Vec<TopicId>>),
    DescribeConfig {
        name: String,
        resource: ConfigResource,
        keys: Option<Vec<String>>,
    },
    DescribeTopicPartitions {
        topics: Option<Vec<TopicId>>,
        partition_limit: i32,
        cursor: Option<Topition>,
    },
    ListGroups(Option<Vec<String>>),
    DeleteGroups(Option<Vec<String>>),
    DescribeGroups {
        group_ids: Option<Vec<String>>,
        include_authorized_operations: bool,
    },
    UpdateGroup {
        group_id: String,
        detail: GroupDetail,
        version: Option<Version>,
    },
    InitProducer {
        transaction_id: Option<String>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    },
    TxnAddOffsets {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        group_id: String,
    },
    TxnAddPartitions(TxnAddPartitionsRequest),
    TxnOffsetCommit(TxnOffsetCommitRequest),
    TxnEnd {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    },
    Maintain(SystemTime),
    ClusterId,
    Node,
    AdvertisedListener,
    DeleteUserScramCredential {
        user: String,
        mechanism: ScramMechanism,
    },
    UpsertUserScramCredential {
        user: String,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    },
    UserScramCredential {
        user: String,
        mechanism: ScramMechanism,
    },
    Ping,
}

impl Display for Request {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdvertisedListener => f.write_str("AdvertisedListener"),
            Self::Brokers => f.write_str("Brokers"),
            Self::ClusterId => f.write_str("ClusterId"),
            Self::CommittedOffsetTopitions(_) => f.write_str("CommittedOffsetTopitions"),
            Self::CreateTopic { .. } => f.write_str("CreateTopic"),
            Self::DeleteGroups(_) => f.write_str("DeleteGroups"),
            Self::DeleteRecords(_) => f.write_str("DeleteRecords"),
            Self::DeleteTopic(_) => f.write_str("DeleteTopic"),
            Self::DescribeConfig { .. } => f.write_str("DescribeConfig"),
            Self::DescribeGroups { .. } => f.write_str("DescribeGroups"),
            Self::DescribeTopicPartitions { .. } => f.write_str("DescribeTopicPartitions"),
            Self::AbortedTransactionRanges(_) => f.write_str("AbortedTransactionRanges"),
            Self::Fetch { .. } => f.write_str("Fetch"),
            Self::IncrementalAlterResource(_) => f.write_str("IncrementalAlterResource"),
            Self::InitProducer { .. } => f.write_str("InitProducer"),
            Self::ListGroups(_) => f.write_str("ListGroups"),
            Self::ListOffsets { .. } => f.write_str("ListOffsets"),
            Self::Maintain(_) => f.write_str("Maintain"),
            Self::Metadata(_) => f.write_str("Metadata"),
            Self::Node => f.write_str("Node"),
            Self::OffsetCommit { .. } => f.write_str("OffsetCommit"),
            Self::OffsetFetch { .. } => f.write_str("OffsetFetch"),
            Self::OffsetForLeaderEpoch { .. } => f.write_str("OffsetForLeaderEpoch"),
            Self::OffsetStage(_) => f.write_str("OffsetStage"),
            Self::Produce { .. } => f.write_str("Produce"),
            Self::RegisterBroker(_) => f.write_str("RegisterBroker"),
            Self::TxnAddOffsets { .. } => f.write_str("TxnAddOffsets"),
            Self::TxnAddPartitions(_) => f.write_str("TxnAddPartitions"),
            Self::TxnEnd { .. } => f.write_str("TxnEnd"),
            Self::TxnOffsetCommit(_) => f.write_str("TxnOffsetCommit"),
            Self::UpdateGroup { .. } => f.write_str("UpdateGroup"),
            Self::DeleteUserScramCredential { .. } => f.write_str("DeleteUserScramCredential"),
            Self::UpsertUserScramCredential { .. } => f.write_str("UpsertUserScramCredential"),
            Self::UserScramCredential { .. } => f.write_str("UserScramCredential"),
            Self::Ping => f.write_str("Ping"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Response {
    AdvertisedListener(Result<Url>),
    Brokers(Result<Vec<DescribeClusterBroker>>),
    ClusterId(Result<String>),
    CommittedOffsetTopitions(Result<BTreeMap<Topition, i64>>),
    CreateTopic(Result<Uuid>),
    DeleteGroups(Result<Vec<DeletableGroupResult>>),
    DeleteRecords(Result<Vec<DeleteRecordsTopicResult>>),
    DeleteTopic(Result<ErrorCode>),
    DeleteUserScramCredential(Result<()>),
    DescribeConfig(Result<DescribeConfigsResult>),
    DescribeGroups(Result<Vec<NamedGroupDetail>>),
    DescribeTopicPartitions(Result<Vec<DescribeTopicPartitionsResponseTopic>>),
    Fetch(Result<Vec<deflated::Batch>>),
    AbortedTransactionRanges(Result<Vec<AbortedTransactionRange>>),
    IncrementalAlterResponse(Result<AlterConfigsResourceResponse>),
    InitProducer(Result<ProducerIdResponse>),
    ListGroups(Result<Vec<ListedGroup>>),
    ListOffsets(Result<Vec<(Topition, ListOffsetResponse)>>),
    Maintain(Result<()>),
    Metadata(Result<MetadataResponse>),
    Node(Result<i32>),
    OffsetCommit(Result<Vec<(Topition, ErrorCode)>>),
    OffsetFetch(Result<BTreeMap<Topition, OffsetFetchRecord>>),
    OffsetForLeaderEpoch(Result<Option<(i32, i64)>>),
    OffsetStage(Result<OffsetStage>),
    Ping(Result<()>),
    Produce(Result<i64>),
    RegisterBroker(Result<()>),
    TxnAddOffsets(Result<ErrorCode>),
    TxnAddPartitions(Result<TxnAddPartitionsResponse>),
    TxnEnd(Result<ErrorCode>),
    TxnOffsetCommit(Result<Vec<TxnOffsetCommitResponseTopic>>),
    UpdateGroup(Result<Version, UpdateError<GroupDetail>>),
    UpsertUserScramCredential(Result<()>),
    UserScramCredential(Result<Option<ScramCredential>>),
}
