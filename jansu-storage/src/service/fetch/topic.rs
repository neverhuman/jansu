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

//! Per-topic fetch handling and the long-poll fetch loop for [`FetchService`].

use std::time::{Duration, Instant};

use jansu_sans_io::{
    ErrorCode, IsolationLevel,
    fetch_request::FetchTopic,
    fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    },
    metadata_response::MetadataResponseTopic,
};
use rama::Context;
use tokio::time::sleep;
use tracing::{debug, instrument};

use super::FetchService;
use super::byte_size::ByteSize;
use crate::{Error, Result, Storage};

impl FetchService {
    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
            .partitions(fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| {
                        PartitionData::default()
                            .partition_index(partition.partition)
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .high_watermark(0)
                            .last_stable_offset(Some(0))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(Some(
                                EpochEndOffset::default().epoch(-1).end_offset(-1),
                            ))
                            .current_leader(Some(
                                LeaderIdAndEpoch::default().leader_id(-1).leader_epoch(-1),
                            ))
                            .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None)
                    })
                    .collect()
            })))
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn fetch_topic<G>(
        &self,
        ctx: &Context<G>,
        max_wait_ms: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        fetch: &FetchTopic,
        _is_first: bool,
    ) -> Result<FetchableTopicResponse>
    where
        G: Storage,
    {
        let metadata = ctx.state().metadata(Some(&[fetch.into()])).await?;

        if let Some(MetadataResponseTopic {
            topic_id,
            name: Some(name),
            ..
        }) = metadata.topics().first()
        {
            let mut partitions = Vec::new();

            for fetch_partition in fetch.partitions.as_ref().unwrap_or(&Vec::new()) {
                let partition = self
                    .fetch_partition(
                        ctx,
                        max_wait_ms,
                        min_bytes,
                        max_bytes,
                        isolation,
                        name,
                        fetch_partition,
                    )
                    .await?;

                partitions.push(partition);
            }

            Ok(FetchableTopicResponse::default()
                .topic(Some(name.to_owned()))
                .topic_id(topic_id.to_owned())
                .partitions(Some(partitions)))
        } else {
            self.unknown_topic_response(fetch)
        }
    }

    #[instrument(skip(self, ctx, isolation, topics))]
    pub(crate) async fn fetch<G>(
        &self,
        ctx: &Context<G>,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topics: &[FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>>
    where
        G: Storage,
    {
        debug!(?isolation, ?topics);

        if topics.is_empty() {
            Ok(vec![])
        } else {
            let cancellation = Self::cancellation_token(ctx);
            let start = Instant::now();
            let mut responses = vec![];
            let mut iteration = 0;
            let mut elapsed = Duration::from_millis(0);
            let mut bytes = 0;

            while elapsed <= max_wait {
                debug!(?elapsed, ?bytes);

                let enumerate = topics.iter().enumerate();
                responses.clear();

                for (i, fetch) in enumerate {
                    let fetch_response = self
                        .fetch_topic(
                            ctx,
                            max_wait,
                            min_bytes,
                            max_bytes,
                            isolation,
                            fetch,
                            i == 0,
                        )
                        .await?;

                    responses.push(fetch_response);
                }

                bytes = u32::try_from(responses.byte_size())?;

                let now = Instant::now();
                elapsed = now.duration_since(start);
                let remaining = max_wait.saturating_sub(elapsed);

                debug!(
                    ?iteration,
                    ?max_wait,
                    ?elapsed,
                    ?remaining,
                    ?bytes,
                    ?min_bytes
                );

                if bytes >= min_bytes || *max_bytes == 0 || remaining.is_zero() {
                    break;
                }

                let delay = if remaining.as_millis() >= 250 {
                    remaining / 2
                } else {
                    remaining
                };

                if let Some(cancellation) = cancellation.as_ref() {
                    tokio::select! {
                        _ = sleep(delay) => {}

                        _ = cancellation.cancelled() => {
                            debug!(?iteration, event = "cancelled");
                            return Err(Error::Cancelled);
                        }
                    }
                } else {
                    sleep(delay).await;
                }

                iteration += 1;
            }

            Ok(responses)
        }
    }
}
