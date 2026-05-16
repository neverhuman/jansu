// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{collections::BTreeMap, ops::Deref as _};

use jansu_sans_io::{
    ApiKey, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest, ListOffsetsResponse,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};
use rama::{Context, Service};
use tracing::{debug, error, instrument};

use super::leader_epoch::{
    current_leader_epoch_error, leader_epoch_for_offset, leader_epoch_history,
    leader_epoch_or_unknown,
};
use crate::{Error, LeaderEpochRecord, Result, Storage, Topition};

/// Response skeleton entry: `(topic_name, [(partition_index, Option<response>)])`.
type ResponseTopics = Vec<(String, Vec<(i32, Option<ListOffsetsPartitionResponse>)>)>;

/// A [`Service`] using [`Storage`] as [`Context`] taking [`ListOffsetsRequest`] returning [`ListOffsetsResponse`].
/// ```
/// use rama::{Context, Layer as _, Service, layer::MapStateLayer};
/// use jansu_sans_io::{
///     ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest,
///     list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
/// };
/// use jansu_storage::{Error, ListOffsetsService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
/// const NODE_ID: i32 = 111;
///
/// let storage = StorageContainer::builder()
///     .cluster_id("jansu")
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://jansu/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(ListOffsetsService);
///
/// let topic = "abcba";
///
/// let response = service
///     .serve(
///         Context::default(),
///         ListOffsetsRequest::default()
///             .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
///             .replica_id(NODE_ID)
///             .topics(Some(
///                 [ListOffsetsTopic::default()
///                     .name(topic.into())
///                     .partitions(Some(
///                         [ListOffsetsPartition::default()
///                             .current_leader_epoch(Some(-1))
///                             .max_num_offsets(Some(3))
///                             .partition_index(0)
///                             .timestamp(ListOffset::Earliest.try_into()?)]
///                         .into(),
///                     ))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// let topics = response.topics.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(topic, topics[0].name);
///
/// let partitions = topics[0].partitions.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(0, partitions[0].partition_index);
/// assert!(partitions[0].old_style_offsets.is_none());
/// assert_eq!(
///     ErrorCode::UnknownTopicOrPartition,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// assert_eq!(Some(-1), partitions[0].timestamp);
/// assert_eq!(Some(-1), partitions[0].offset);
/// assert_eq!(Some(-1), partitions[0].leader_epoch);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetsService;

impl ApiKey for ListOffsetsService {
    const KEY: i16 = ListOffsetsRequest::KEY;
}

/// A partition that passed validation and needs a storage lookup.
#[derive(Debug)]
struct PendingListOffset {
    topic_slot: usize,
    partition_slot: usize,
    topition: Topition,
    request: ListOffset,
}

/// Build a per-partition error response with Kafka-compatible defaults.
fn partition_error(partition_index: i32, error_code: ErrorCode) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .partition_index(partition_index)
        .error_code(error_code.into())
        .old_style_offsets(None)
        .timestamp(Some(-1))
        .offset(Some(-1))
        .leader_epoch(Some(-1))
}

impl<G> Service<G, ListOffsetsRequest> for ListOffsetsService
where
    G: Storage,
{
    type Response = ListOffsetsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: ListOffsetsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let throttle_time_ms = Some(0);

        let isolation_level = req
            .isolation_level
            .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation_level| {
                IsolationLevel::try_from(isolation_level)
            })?;

        let topics = if let Some(request_topics) = req.topics {
            // Phase 1: Walk the request in order, validate each partition,
            // and build the response skeleton preserving request topology.
            let mut response_topics: ResponseTopics = Vec::with_capacity(request_topics.len());

            let mut pending = Vec::new();
            let mut histories: BTreeMap<Topition, Vec<LeaderEpochRecord>> = BTreeMap::new();

            for request_topic in request_topics {
                let topic_slot = response_topics.len();
                let topic_name = request_topic.name;
                let request_partitions = request_topic.partitions.unwrap_or_default();

                let mut partition_slots: Vec<_> = request_partitions
                    .iter()
                    .map(|partition| (partition.partition_index, None))
                    .collect();

                for (partition_slot, partition) in request_partitions.iter().enumerate() {
                    let topition = Topition::new(topic_name.clone(), partition.partition_index);

                    // Check partition existence via leader_epoch_history.
                    // Unknown partitions get an immediate error response.
                    let history = match leader_epoch_history(&ctx, &topition).await {
                        Ok(history) => history,
                        Err(Error::Api(ErrorCode::UnknownTopicOrPartition)) => {
                            partition_slots[partition_slot].1 = Some(partition_error(
                                partition.partition_index,
                                ErrorCode::UnknownTopicOrPartition,
                            ));
                            continue;
                        }
                        Err(err) => return Err(err),
                    };

                    // Validate current_leader_epoch fencing (Kafka semantics).
                    if let Some(error_code) =
                        current_leader_epoch_error(partition.current_leader_epoch, &history)
                    {
                        partition_slots[partition_slot].1 =
                            Some(partition_error(partition.partition_index, error_code));
                        continue;
                    }

                    let request = ListOffset::try_from(partition.timestamp)?;

                    _ = histories.insert(topition.clone(), history);
                    pending.push(PendingListOffset {
                        topic_slot,
                        partition_slot,
                        topition,
                        request,
                    });
                }

                response_topics.push((topic_name, partition_slots));
            }

            // Phase 2: Call storage only for valid partitions.
            let storage_requests: Vec<_> = pending
                .iter()
                .map(|pending| (pending.topition.clone(), pending.request))
                .collect();

            let storage_responses = if storage_requests.is_empty() {
                Vec::new()
            } else {
                ctx.state()
                    .list_offsets(isolation_level, storage_requests.deref())
                    .await
                    .inspect(|r| debug!(?r, ?storage_requests))
                    .inspect_err(|err| error!(?err, ?storage_requests))?
            };

            // Index storage responses by topition for lookup.
            let mut by_topition: BTreeMap<Topition, Vec<_>> = BTreeMap::new();
            for (topition, response) in storage_responses {
                by_topition.entry(topition).or_default().push(response);
            }

            // Phase 3: Fill pending partition slots with storage results.
            for pending in pending {
                let storage_offset = by_topition.get_mut(&pending.topition).and_then(|v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.remove(0))
                    }
                });

                let response = match storage_offset {
                    Some(offset) => {
                        let history = histories
                            .get(&pending.topition)
                            .map(Vec::as_slice)
                            .unwrap_or_default();

                        let epoch = if offset.error_code() == ErrorCode::None {
                            offset.offset().map_or_else(
                                || leader_epoch_or_unknown(history),
                                |offset| leader_epoch_for_offset(history, offset),
                            )
                        } else {
                            -1
                        };

                        ListOffsetsPartitionResponse::default()
                            .partition_index(pending.topition.partition())
                            .error_code(offset.error_code().into())
                            .old_style_offsets(None)
                            .timestamp(offset.timestamp().unwrap_or(Some(-1)).or(Some(-1)))
                            .offset(offset.offset().or(Some(-1)))
                            .leader_epoch(Some(epoch))
                    }
                    None => partition_error(
                        pending.topition.partition(),
                        ErrorCode::UnknownTopicOrPartition,
                    ),
                };

                response_topics[pending.topic_slot].1[pending.partition_slot].1 = Some(response);
            }

            // Phase 4: Assemble final response preserving request order.
            Some(
                response_topics
                    .into_iter()
                    .map(|(name, partitions)| {
                        let partitions = partitions
                            .into_iter()
                            .map(|(partition_index, response)| {
                                response.unwrap_or_else(|| {
                                    partition_error(
                                        partition_index,
                                        ErrorCode::UnknownTopicOrPartition,
                                    )
                                })
                            })
                            .collect();

                        ListOffsetsTopicResponse::default()
                            .name(name)
                            .partitions(Some(partitions))
                    })
                    .collect(),
            )
        } else {
            None
        };

        Ok(ListOffsetsResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .topics(topics))
        .inspect(|r| debug!(?r))
    }
}
