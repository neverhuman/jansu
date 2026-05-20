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

//! Single topic-partition fetch handling for [`FetchService`].

use std::time::Duration;

use jansu_sans_io::{
    ErrorCode, IsolationLevel,
    fetch_request::FetchPartition,
    fetch_response::AbortedTransaction,
    fetch_response::{EpochEndOffset, LeaderIdAndEpoch, PartitionData, SnapshotId},
    record::deflated::{Batch, Frame},
};
use rama::Context;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument};

use super::FetchService;
use super::byte_size::ByteSize;
use crate::service::leader_epoch::{current_leader_epoch, leader_epoch_history};
use crate::{AbortedTransactionRange, Error, Result, Storage, Topition};

impl FetchService {
    pub(super) fn cancellation_token<G>(ctx: &Context<G>) -> Option<CancellationToken> {
        ctx.get::<CancellationToken>().cloned()
    }

    fn aborted_transactions(
        aborted_transaction_ranges: &[AbortedTransactionRange],
    ) -> Vec<AbortedTransaction> {
        let mut aborted_transactions = aborted_transaction_ranges
            .iter()
            .map(Into::into)
            .collect::<Vec<_>>();

        aborted_transactions.sort_unstable();
        aborted_transactions.dedup();
        aborted_transactions
    }

    fn retain_visible_batches(batches: &mut Vec<Batch>) {
        batches.retain(|batch| !batch.is_control());
    }

    fn retain_visible_read_committed_batches(
        batches: &mut Vec<Batch>,
        aborted_transaction_ranges: &[AbortedTransactionRange],
    ) {
        batches.retain(|batch| {
            if !batch.is_transactional() {
                return true;
            }

            !aborted_transaction_ranges.iter().any(|range| {
                batch.producer_id == range.producer_id
                    && batch.base_offset >= range.offset_start
                    && batch.base_offset < range.offset_end
            })
        });
    }

    fn retain_batches_within_max_bytes(batches: &mut Vec<Batch>, max_bytes: u32) -> Result<u32> {
        if max_bytes == 0 {
            batches.clear();
            return Ok(0);
        }

        let mut bytes = 0u32;
        let mut keep = 0usize;

        for batch in batches.iter() {
            let batch_bytes = u32::try_from(batch.byte_size())?;

            if keep == 0 {
                bytes = bytes.saturating_add(batch_bytes);
                keep = 1;
                if bytes >= max_bytes {
                    break;
                }
                continue;
            }

            if bytes.saturating_add(batch_bytes) > max_bytes {
                break;
            }

            bytes = bytes.saturating_add(batch_bytes);
            keep += 1;
        }

        batches.truncate(keep);
        Ok(bytes)
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, ctx, _max_wait_ms, min_bytes, max_bytes, isolation, fetch_partition), fields(partition = fetch_partition.partition))]
    pub(super) async fn fetch_partition<G>(
        &self,
        ctx: &Context<G>,
        _max_wait_ms: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topic: &str,
        fetch_partition: &FetchPartition,
    ) -> Result<PartitionData>
    where
        G: Storage,
    {
        let partition_index = fetch_partition.partition;
        let tp = Topition::new(topic, partition_index);

        let history = leader_epoch_history(ctx, &tp).await?;
        let current_epoch = current_leader_epoch(&history);
        let leader_id = ctx.state().node().await?;

        if let Some(requested_epoch) = fetch_partition.current_leader_epoch
            && requested_epoch != -1
            && let Some(current_epoch) = current_epoch
        {
            if requested_epoch < current_epoch.epoch {
                return Ok(PartitionData::default()
                    .partition_index(partition_index)
                    .error_code(ErrorCode::FencedLeaderEpoch.into())
                    .high_watermark(0)
                    .last_stable_offset(Some(0))
                    .log_start_offset(Some(-1))
                    .diverging_epoch(Some(EpochEndOffset::default().epoch(-1).end_offset(-1)))
                    .current_leader(Some(
                        LeaderIdAndEpoch::default()
                            .leader_id(leader_id)
                            .leader_epoch(current_epoch.epoch),
                    ))
                    .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                    .aborted_transactions(Some([].into()))
                    .preferred_read_replica(Some(-1))
                    .records(None));
            }
            if requested_epoch > current_epoch.epoch {
                return Ok(PartitionData::default()
                    .partition_index(partition_index)
                    .error_code(ErrorCode::UnknownLeaderEpoch.into())
                    .high_watermark(0)
                    .last_stable_offset(Some(0))
                    .log_start_offset(Some(-1))
                    .diverging_epoch(Some(EpochEndOffset::default().epoch(-1).end_offset(-1)))
                    .current_leader(Some(
                        LeaderIdAndEpoch::default()
                            .leader_id(leader_id)
                            .leader_epoch(current_epoch.epoch),
                    ))
                    .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                    .aborted_transactions(Some([].into()))
                    .preferred_read_replica(Some(-1))
                    .records(None));
            }
        }

        let cancellation = Self::cancellation_token(ctx);
        let aborted_transaction_ranges = if isolation == IsolationLevel::ReadCommitted {
            ctx.state().aborted_transaction_ranges(&tp).await?
        } else {
            vec![]
        };

        let mut batches = Vec::new();

        let mut offset = fetch_partition.fetch_offset;
        let mut partition_max_bytes = u32::try_from(fetch_partition.partition_max_bytes)?;

        loop {
            let fetch_max_bytes = (*max_bytes).min(partition_max_bytes);

            if fetch_max_bytes == 0 {
                break;
            }

            debug!(offset);

            let mut fetched = if let Some(cancellation) = cancellation.as_ref() {
                tokio::select! {
                    result = ctx.state().fetch(&tp, offset, min_bytes, fetch_max_bytes, isolation) => {
                        result
                            .inspect(|r| debug!(?tp, ?offset, ?r))
                            .inspect_err(|error| error!(?tp, ?error))?
                    }

                    _ = cancellation.cancelled() => {
                        debug!(?tp, ?offset, event = "cancelled");
                        return Err(Error::Cancelled);
                    }
                }
            } else {
                ctx.state()
                    .fetch(&tp, offset, min_bytes, fetch_max_bytes, isolation)
                    .await
                    .inspect(|r| debug!(?tp, ?offset, ?r))
                    .inspect_err(|error| error!(?tp, ?error))?
            };
            let raw_latest = fetched
                .iter()
                .map(|batch| batch.base_offset + batch.record_count as i64)
                .max();

            Self::retain_visible_batches(&mut fetched);

            if isolation == IsolationLevel::ReadCommitted {
                Self::retain_visible_read_committed_batches(
                    &mut fetched,
                    &aborted_transaction_ranges,
                );
            }

            let fetched_bytes =
                Self::retain_batches_within_max_bytes(&mut fetched, fetch_max_bytes)?;
            *max_bytes = max_bytes.saturating_sub(fetched_bytes);
            partition_max_bytes = partition_max_bytes.saturating_sub(fetched_bytes);

            if let Some(latest) = raw_latest {
                offset = latest;
            }

            debug!(?offset, ?fetched);

            if fetched.is_empty() {
                if raw_latest.is_some() {
                    continue;
                }

                break;
            }

            if fetched.first().is_some_and(|batch| batch.record_count == 0) {
                break;
            }

            batches.append(&mut fetched);

            if batches.byte_size() < u64::from(min_bytes) && raw_latest.is_some() {
                continue;
            }
        }

        let offset_stage = ctx
            .state()
            .offset_stage(&tp)
            .await
            .inspect_err(|error| error!(?error, ?tp))?;

        let current_leader = current_epoch.map_or(
            LeaderIdAndEpoch::default().leader_id(-1).leader_epoch(-1),
            |current_epoch| {
                LeaderIdAndEpoch::default()
                    .leader_id(leader_id)
                    .leader_epoch(current_epoch.epoch)
            },
        );

        Ok(PartitionData::default()
            .partition_index(partition_index)
            .error_code(ErrorCode::None.into())
            .high_watermark(offset_stage.high_watermark())
            .last_stable_offset(Some(offset_stage.last_stable()))
            .log_start_offset(Some(offset_stage.log_start()))
            .diverging_epoch(None)
            .current_leader(Some(current_leader))
            .snapshot_id(None)
            .aborted_transactions(Some(Self::aborted_transactions(
                &aborted_transaction_ranges,
            )))
            .preferred_read_replica(Some(-1))
            .records(if batches.is_empty() {
                None
            } else {
                Some(Frame { batches })
            }))
        .inspect(|r| debug!(?r))
    }
}
