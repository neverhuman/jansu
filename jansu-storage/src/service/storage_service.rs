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

//! Channel and storage-backed [`Service`] implementations for storage requests.

use rama::{Context, Layer, Service};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::{Request, RequestReceiver, Response};
use crate::{Error, Result, Storage};

#[derive(Clone, Debug, Default)]
pub struct ChannelRequestLayer {
    cancellation: CancellationToken,
}

impl ChannelRequestLayer {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }
}

impl<S> Layer<S> for ChannelRequestLayer {
    type Service = ChannelRequestService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            cancellation: self.cancellation.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChannelRequestService<S> {
    inner: S,
    cancellation: CancellationToken,
}

impl<S, State> Service<State, RequestReceiver> for ChannelRequestService<S>
where
    S: Service<State, Request, Response = Response, Error = Error>,
    State: Clone + Send + Sync + 'static,
{
    type Response = ();
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        mut req: RequestReceiver,
    ) -> Result<Self::Response, Self::Error> {
        loop {
            tokio::select! {
                Some((request, tx)) = req.recv() => {
                    self.inner
                    .serve(ctx.clone(), request)
                    .await
                    .and_then(|response| {
                        tx.send(response).map_err(|_unsent| Error::UnableToSend)
                    })?
                }

                cancelled = self.cancellation.cancelled() => {
                    debug!(?cancelled);
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestStorageService<G> {
    storage: G,
}

impl<G> RequestStorageService<G>
where
    G: Storage,
{
    pub fn new(storage: G) -> Self {
        Self { storage }
    }
}

impl<G, State> Service<State, Request> for RequestStorageService<G>
where
    G: Storage,
    State: Clone + Send + Sync + 'static,
{
    type Response = Response;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        match req {
            Request::RegisterBroker(broker_registration) => Ok(Response::RegisterBroker(
                self.storage.register_broker(broker_registration).await,
            )),
            Request::IncrementalAlterResource(alter_configs_resource) => {
                Ok(Response::IncrementalAlterResponse(
                    self.storage
                        .incremental_alter_resource(alter_configs_resource)
                        .await,
                ))
            }
            Request::CreateTopic {
                topic,
                validate_only,
            } => Ok(Response::CreateTopic(
                self.storage.create_topic(topic, validate_only).await,
            )),
            Request::DeleteRecords(delete_records_topics) => Ok(Response::DeleteRecords(
                self.storage
                    .delete_records(&delete_records_topics[..])
                    .await,
            )),
            Request::DeleteTopic(topic_id) => Ok(Response::DeleteTopic(
                self.storage.delete_topic(&topic_id).await,
            )),
            Request::Brokers => Ok(Response::Brokers(self.storage.brokers().await)),
            Request::Produce {
                transaction_id,
                topition,
                batch,
            } => Ok(Response::Produce(
                self.storage
                    .produce(transaction_id.as_deref(), &topition, batch)
                    .await,
            )),
            Request::Fetch {
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation,
            } => {
                let cancellation = ctx.get::<CancellationToken>().cloned();
                let fetch = self
                    .storage
                    .fetch(&topition, offset, min_bytes, max_bytes, isolation);

                let response = if let Some(cancellation) = cancellation.as_ref() {
                    tokio::select! {
                        result = fetch => result,

                        _ = cancellation.cancelled() => {
                            return Err(Error::Cancelled);
                        }
                    }
                } else {
                    fetch.await
                };

                Ok(Response::Fetch(response))
            }
            Request::AbortedTransactionRanges(topition) => Ok(Response::AbortedTransactionRanges(
                self.storage.aborted_transaction_ranges(&topition).await,
            )),
            Request::OffsetStage(topition) => Ok(Response::OffsetStage(
                self.storage.offset_stage(&topition).await,
            )),
            Request::ListOffsets {
                isolation_level,
                offsets,
            } => Ok(Response::ListOffsets(
                self.storage
                    .list_offsets(isolation_level, &offsets[..])
                    .await,
            )),
            Request::OffsetCommit {
                group_id,
                retention_time_ms,
                offsets,
            } => Ok(Response::OffsetCommit(
                self.storage
                    .offset_commit(&group_id, retention_time_ms, &offsets[..])
                    .await,
            )),
            Request::CommittedOffsetTopitions(group_id) => Ok(Response::CommittedOffsetTopitions(
                self.storage.committed_offset_topitions(&group_id).await,
            )),
            Request::OffsetForLeaderEpoch {
                topition,
                leader_epoch,
            } => Ok(Response::OffsetForLeaderEpoch(
                self.storage
                    .offset_for_leader_epoch(&topition, leader_epoch)
                    .await,
            )),
            Request::OffsetFetch {
                group_id,
                topics,
                require_stable,
            } => Ok(Response::OffsetFetch(
                self.storage
                    .offset_fetch_records(group_id.as_deref(), &topics[..], require_stable)
                    .await,
            )),
            Request::Metadata(topic_ids) => Ok(Response::Metadata(
                self.storage.metadata(topic_ids.as_deref()).await,
            )),
            Request::DescribeConfig {
                name,
                resource,
                keys,
            } => Ok(Response::DescribeConfig(
                self.storage
                    .describe_config(&name, resource, keys.as_deref())
                    .await,
            )),
            Request::DescribeTopicPartitions {
                topics,
                partition_limit,
                cursor,
            } => Ok(Response::DescribeTopicPartitions(
                self.storage
                    .describe_topic_partitions(topics.as_deref(), partition_limit, cursor)
                    .await,
            )),
            Request::ListGroups(items) => Ok(Response::ListGroups(
                self.storage.list_groups(items.as_deref()).await,
            )),
            Request::DeleteGroups(items) => Ok(Response::DeleteGroups(
                self.storage.delete_groups(items.as_deref()).await,
            )),
            Request::DescribeGroups {
                group_ids,
                include_authorized_operations,
            } => Ok(Response::DescribeGroups(
                self.storage
                    .describe_groups(group_ids.as_deref(), include_authorized_operations)
                    .await,
            )),
            Request::UpdateGroup {
                group_id,
                detail,
                version,
            } => Ok(Response::UpdateGroup(
                self.storage.update_group(&group_id, detail, version).await,
            )),
            Request::InitProducer {
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            } => Ok(Response::InitProducer(
                self.storage
                    .init_producer(
                        transaction_id.as_deref(),
                        transaction_timeout_ms,
                        producer_id,
                        producer_epoch,
                    )
                    .await,
            )),
            Request::TxnAddOffsets {
                transaction_id,
                producer_id,
                producer_epoch,
                group_id,
            } => Ok(Response::TxnAddOffsets(
                self.storage
                    .txn_add_offsets(&transaction_id, producer_id, producer_epoch, &group_id)
                    .await,
            )),
            Request::TxnAddPartitions(txn_add_partitions_request) => {
                Ok(Response::TxnAddPartitions(
                    self.storage
                        .txn_add_partitions(txn_add_partitions_request)
                        .await,
                ))
            }
            Request::TxnOffsetCommit(txn_offset_commit_request) => Ok(Response::TxnOffsetCommit(
                self.storage
                    .txn_offset_commit(txn_offset_commit_request)
                    .await,
            )),
            Request::TxnEnd {
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
            } => Ok(Response::TxnEnd(
                self.storage
                    .txn_end(&transaction_id, producer_id, producer_epoch, committed)
                    .await,
            )),
            Request::Maintain(now) => Ok(Response::Maintain(self.storage.maintain(now).await)),
            Request::ClusterId => Ok(Response::ClusterId(self.storage.cluster_id().await)),
            Request::Node => Ok(Response::Node(self.storage.node().await)),
            Request::AdvertisedListener => Ok(Response::AdvertisedListener(
                self.storage.advertised_listener().await,
            )),
            Request::DeleteUserScramCredential { user, mechanism } => {
                Ok(Response::DeleteUserScramCredential(
                    self.storage
                        .delete_user_scram_credential(&user[..], mechanism)
                        .await,
                ))
            }
            Request::UpsertUserScramCredential {
                user,
                mechanism,
                credential,
            } => Ok(Response::UpsertUserScramCredential(
                self.storage
                    .upsert_user_scram_credential(&user[..], mechanism, credential)
                    .await,
            )),
            Request::UserScramCredential { user, mechanism } => Ok(Response::UserScramCredential(
                self.storage
                    .user_scram_credential(&user[..], mechanism)
                    .await,
            )),
            Request::Ping => Ok(Response::Ping(self.storage.ping().await)),
        }
    }
}
