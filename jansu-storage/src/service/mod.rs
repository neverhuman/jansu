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

mod alter_user_scram_credentials;
mod consumer_group_describe;
mod create_acls;
mod create_topics;
mod delete_groups;
mod delete_records;
mod delete_topics;
mod describe_acls;
mod describe_cluster;
mod describe_configs;
mod describe_groups;
pub(crate) mod topic_config_defaults;
mod describe_topic_partitions;
mod describe_user_scram_credentials;
mod fetch;
mod find_coordinator;
mod get_telemetry_subscriptions;
mod incremental_alter_configs;
mod init_producer_id;
mod leader_epoch;
mod list_groups;
mod list_offsets;
mod list_partition_reassignments;
mod metadata;
mod offset_for_leader_epoch;
mod produce;
mod txn;

use std::{
    collections::BTreeMap,
    fmt::{self, Debug, Display, Formatter},
    sync::LazyLock,
    time::{Duration, SystemTime},
};

pub use alter_user_scram_credentials::AlterUserScramCredentialsService;
use async_trait::async_trait;
pub use consumer_group_describe::ConsumerGroupDescribeService;
pub use create_acls::CreateAclsService;
pub use create_topics::CreateTopicsService;
pub use delete_groups::DeleteGroupsService;
pub use delete_records::DeleteRecordsService;
pub use delete_topics::DeleteTopicsService;
pub use describe_acls::DescribeAclsService;
pub use describe_cluster::DescribeClusterService;
pub use describe_configs::DescribeConfigsService;
pub use describe_groups::DescribeGroupsService;
pub use describe_topic_partitions::DescribeTopicPartitionsService;
pub use describe_user_scram_credentials::DescribeUserScramCredentialsService;
pub use fetch::FetchService;
pub use find_coordinator::FindCoordinatorService;
pub use get_telemetry_subscriptions::GetTelemetrySubscriptionsService;
pub use incremental_alter_configs::IncrementalAlterConfigsService;
pub use init_producer_id::InitProducerIdService;
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
pub use list_groups::ListGroupsService;
pub use list_offsets::ListOffsetsService;
pub use list_partition_reassignments::ListPartitionReassignmentsService;
pub use metadata::MetadataService;
pub use offset_for_leader_epoch::OffsetForLeaderEpochService;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
pub use produce::ProduceService;
use rama::{Context, Layer, Service};
use tokio::sync::{
    mpsc::{self, error::SendError},
    oneshot,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument};
pub use txn::add_offsets::AddOffsetsService as TxnAddOffsetsService;
pub use txn::add_partitions::AddPartitionService as TxnAddPartitionService;
pub use txn::offset_commit::OffsetCommitService as TxnOffsetCommitService;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, METER, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetFetchRecord, OffsetStage, ProducerIdResponse,
    Result, ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
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

pub type RequestSender = mpsc::Sender<(Request, oneshot::Sender<Response>)>;
pub type RequestReceiver = mpsc::Receiver<(Request, oneshot::Sender<Response>)>;

pub fn bounded_channel(buffer: usize) -> (RequestSender, RequestReceiver) {
    mpsc::channel::<(Request, oneshot::Sender<Response>)>(buffer)
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ServiceError {
    Storage(Error),
    UpdateGroupDetail(UpdateError<GroupDetail>),
}

impl Display for ServiceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<SendError<()>> for ServiceError {
    fn from(_value: SendError<()>) -> Self {
        Self::Storage(Error::UnableToSend)
    }
}

impl From<Error> for ServiceError {
    fn from(value: Error) -> Self {
        Self::Storage(value)
    }
}

impl From<UpdateError<GroupDetail>> for ServiceError {
    fn from(value: UpdateError<GroupDetail>) -> Self {
        Self::UpdateGroupDetail(value)
    }
}

impl From<ServiceError> for Error {
    fn from(value: ServiceError) -> Self {
        if let ServiceError::Storage(error) = value {
            error
        } else {
            unreachable!()
        }
    }
}

impl From<ServiceError> for UpdateError<GroupDetail> {
    fn from(value: ServiceError) -> Self {
        if let ServiceError::UpdateGroupDetail(error) = value {
            error
        } else {
            unreachable!()
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestLayer;

impl<S> Layer<S> for RequestLayer {
    type Service = RequestService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S> {
    inner: S,
}

impl<State, S> Service<State, Request> for RequestService<S>
where
    S: Service<State, Request>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        self.inner.serve(ctx, req).await
    }
}

pub mod channel_request;
pub use channel_request::{ChannelRequestLayer, ChannelRequestService};
pub mod request_channel;
pub use request_channel::RequestChannelService;
pub mod request_storage;
pub use request_storage::RequestStorageService;
