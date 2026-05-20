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
mod channel_storage;
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
mod messages;
mod metadata;
mod offset_for_leader_epoch;
mod produce;
mod storage_service;
mod txn;

#[cfg(test)]
mod tests;

use std::{
    fmt::{self, Display, Formatter},
    sync::LazyLock,
    time::SystemTime,
};

pub use alter_user_scram_credentials::AlterUserScramCredentialsService;
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
pub use list_groups::ListGroupsService;
pub use list_offsets::ListOffsetsService;
pub use list_partition_reassignments::ListPartitionReassignmentsService;
pub use messages::{Request, Response};
pub use metadata::MetadataService;
pub use offset_for_leader_epoch::OffsetForLeaderEpochService;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
pub use produce::ProduceService;
use rama::{Context, Layer, Service};
pub use storage_service::{ChannelRequestLayer, ChannelRequestService, RequestStorageService};
use tokio::sync::{
    mpsc::{self, error::SendError},
    oneshot,
};
use tracing::{debug, error, instrument};
pub use txn::add_offsets::AddOffsetsService as TxnAddOffsetsService;
pub use txn::add_partitions::AddPartitionService as TxnAddPartitionService;
pub use txn::offset_commit::OffsetCommitService as TxnOffsetCommitService;

use crate::{Error, GroupDetail, METER, Result, UpdateError};

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

/// A [`Service`] sending [`Request`]s over a [`RequestSender`] channel
#[derive(Clone, Debug)]
pub struct RequestChannelService {
    tx: RequestSender,
}

impl RequestChannelService {
    pub fn new(tx: RequestSender) -> Self {
        Self { tx }
    }

    fn elapsed_millis(&self, start: SystemTime) -> u64 {
        start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64)
    }
}

static STORAGE_CHANNEL_CAPACITY: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("jansu_storage_channel_capacity")
        .with_description("Storage channel capacity")
        .build()
});

impl<State> Service<State, Request> for RequestChannelService
where
    State: Send + Sync + 'static,
{
    type Response = Response;
    type Error = ServiceError;

    #[instrument(skip_all)]
    async fn serve(
        &self,
        ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        let _ = ctx;
        let (resp_tx, resp_rx) = oneshot::channel();

        let start = SystemTime::now();

        let operation = req.to_string();
        let attributes = [KeyValue::new("operation", operation.clone())];

        let capacity = self.tx.capacity();
        STORAGE_CHANNEL_CAPACITY.record(capacity as u64, &attributes);
        debug!(operation, capacity);

        self.tx
            .reserve()
            .await
            .map(|permit| permit.send((req, resp_tx)))
            .inspect(|_| {
                let permit_elapsed = self.elapsed_millis(start);
                STORAGE_CHANNEL_PERMIT_DURATION.record(permit_elapsed, &attributes);
                debug!(operation, permit_elapsed);
            })
            .inspect_err(|err| {
                error!(operation, ?err);
                STORAGE_CHANNEL_ERROR.add(1, &attributes);
            })?;

        resp_rx
            .await
            .map_err(|_| Error::OneshotRecv.into())
            .inspect(|_| {
                let elapsed_millis = self.elapsed_millis(start);
                STORAGE_CHANNEL_REQUEST_DURATION.record(elapsed_millis, &attributes);
                debug!(operation, elapsed_millis);
            })
            .inspect_err(|err| {
                error!(operation, ?err);
                STORAGE_CHANNEL_ERROR.add(1, &attributes);
            })
    }
}

static STORAGE_CHANNEL_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_storage_channel_request_duration")
        .with_unit("ms")
        .with_description("Storage channel request latency in milliseconds")
        .build()
});

static STORAGE_CHANNEL_PERMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_storage_channel_permit_duration")
        .with_unit("ms")
        .with_description("Storage channel permit latency in milliseconds")
        .build()
});

static STORAGE_CHANNEL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_storage_channel_error")
        .with_description("Storage channel error count")
        .build()
});
