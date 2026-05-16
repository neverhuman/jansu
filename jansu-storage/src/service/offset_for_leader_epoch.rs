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

use jansu_sans_io::{
    ApiKey, ErrorCode, OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse,
    offset_for_leader_epoch_response::{EpochEndOffset, OffsetForLeaderTopicResult},
};
use rama::{Context, Service};
use tracing::{debug, error, instrument};

use super::leader_epoch::{current_leader_epoch, leader_epoch_history};
use crate::{Error, Result, Storage, Topition};

/// A [`Service`] using [`Storage`] as [`Context`] taking
/// [`OffsetForLeaderEpochRequest`] returning [`OffsetForLeaderEpochResponse`].
///
/// **Important**: This API (key 23) must NOT be advertised in `ApiVersions`
/// until leader-epoch history semantics are fully verified.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetForLeaderEpochService;

impl ApiKey for OffsetForLeaderEpochService {
    const KEY: i16 = OffsetForLeaderEpochRequest::KEY;
}

impl<G> Service<G, OffsetForLeaderEpochRequest> for OffsetForLeaderEpochService
where
    G: Storage,
{
    type Response = OffsetForLeaderEpochResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: OffsetForLeaderEpochRequest,
    ) -> Result<Self::Response, Self::Error> {
        let throttle_time_ms = Some(0);

        let topics = if let Some(topics) = req.topics {
            let mut results = Vec::with_capacity(topics.len());

            for topic in topics {
                let mut partition_results = Vec::new();

                if let Some(partitions) = topic.partitions {
                    for partition in partitions {
                        let tp = Topition::new(topic.topic.clone(), partition.partition);

                        let history = match leader_epoch_history(&ctx, &tp).await {
                            Ok(history) => history,
                            Err(Error::Api(ErrorCode::UnknownTopicOrPartition)) => {
                                partition_results.push(
                                    EpochEndOffset::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .partition(partition.partition)
                                        .leader_epoch(Some(-1))
                                        .end_offset(-1),
                                );
                                continue;
                            }
                            Err(err) => return Err(err),
                        };

                        let current_epoch = current_leader_epoch(&history);

                        // Validate current_leader_epoch if present (fencing)
                        if let Some(cle) = partition.current_leader_epoch
                            && cle != -1
                            && let Some(current_epoch) = current_epoch
                        {
                            if cle < current_epoch.epoch {
                                partition_results.push(
                                    EpochEndOffset::default()
                                        .error_code(ErrorCode::FencedLeaderEpoch.into())
                                        .partition(partition.partition)
                                        .leader_epoch(Some(-1))
                                        .end_offset(-1),
                                );
                                continue;
                            }
                            if cle > current_epoch.epoch {
                                partition_results.push(
                                    EpochEndOffset::default()
                                        .error_code(ErrorCode::UnknownLeaderEpoch.into())
                                        .partition(partition.partition)
                                        .leader_epoch(Some(-1))
                                        .end_offset(-1),
                                );
                                continue;
                            }
                        }

                        let offset_stage = match ctx.state().offset_stage(&tp).await {
                            Ok(offset_stage) => offset_stage,
                            Err(err) => {
                                error!(
                                    ?err,
                                    ?tp,
                                    leader_epoch = partition.leader_epoch,
                                    "offset stage lookup error"
                                );
                                partition_results.push(
                                    EpochEndOffset::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .partition(partition.partition)
                                        .leader_epoch(Some(-1))
                                        .end_offset(-1),
                                );
                                continue;
                            }
                        };

                        match current_epoch {
                            Some(current_epoch) if partition.leader_epoch > current_epoch.epoch => {
                                partition_results.push(
                                    EpochEndOffset::default()
                                        .error_code(ErrorCode::UnknownLeaderEpoch.into())
                                        .partition(partition.partition)
                                        .leader_epoch(Some(-1))
                                        .end_offset(-1),
                                );
                            }
                            Some(current_epoch)
                                if partition.leader_epoch == current_epoch.epoch =>
                            {
                                partition_results.push(
                                    EpochEndOffset::default()
                                        .error_code(ErrorCode::None.into())
                                        .partition(partition.partition)
                                        .leader_epoch(Some(current_epoch.epoch))
                                        .end_offset(offset_stage.high_watermark()),
                                );
                            }
                            Some(_) => {
                                match history
                                    .iter()
                                    .find(|record| record.epoch > partition.leader_epoch)
                                    .copied()
                                {
                                    Some(next_epoch) => {
                                        debug!(
                                            ?tp,
                                            leader_epoch = partition.leader_epoch,
                                            next_epoch = next_epoch.epoch,
                                            end_offset = next_epoch.start_offset,
                                            "epoch lookup success"
                                        );
                                        partition_results.push(
                                            EpochEndOffset::default()
                                                .error_code(ErrorCode::None.into())
                                                .partition(partition.partition)
                                                .leader_epoch(Some(next_epoch.epoch))
                                                .end_offset(next_epoch.start_offset),
                                        );
                                    }
                                    None => {
                                        partition_results.push(
                                            EpochEndOffset::default()
                                                .error_code(ErrorCode::UnknownLeaderEpoch.into())
                                                .partition(partition.partition)
                                                .leader_epoch(Some(-1))
                                                .end_offset(-1),
                                        );
                                    }
                                }
                            }
                            None => {
                                partition_results.push(
                                    EpochEndOffset::default()
                                        .error_code(ErrorCode::UnknownLeaderEpoch.into())
                                        .partition(partition.partition)
                                        .leader_epoch(Some(-1))
                                        .end_offset(-1),
                                );
                            }
                        }
                    }
                }

                results.push(
                    OffsetForLeaderTopicResult::default()
                        .topic(topic.topic)
                        .partitions(Some(partition_results)),
                );
            }

            Some(results)
        } else {
            None
        };

        Ok(OffsetForLeaderEpochResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .topics(topics))
        .inspect(|r| debug!(?r))
    }
}
