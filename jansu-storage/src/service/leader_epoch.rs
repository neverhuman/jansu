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

use jansu_sans_io::ErrorCode;
use rama::Context;

use crate::{LeaderEpochRecord, Result, Storage, Topition};

pub(crate) async fn leader_epoch_history<G>(
    ctx: &Context<G>,
    topition: &Topition,
) -> Result<Vec<LeaderEpochRecord>>
where
    G: Storage,
{
    let mut history = ctx.state().leader_epoch_history(topition).await?;
    history.sort_unstable();
    Ok(history)
}

pub(crate) fn current_leader_epoch(history: &[LeaderEpochRecord]) -> Option<LeaderEpochRecord> {
    history.last().copied()
}

pub(crate) fn leader_epoch_or_unknown(history: &[LeaderEpochRecord]) -> i32 {
    current_leader_epoch(history).map_or(-1, |record| record.epoch)
}

/// Return the leader epoch that was active for `offset`.
///
/// Kafka ListOffsets v4+ includes the leader epoch associated with the
/// returned offset. That is not always the current epoch.
pub(crate) fn leader_epoch_for_offset(history: &[LeaderEpochRecord], offset: i64) -> i32 {
    if history.is_empty() {
        return -1;
    }

    history
        .iter()
        .rev()
        .find(|record| record.start_offset <= offset)
        .or(history.first())
        .map_or(-1, |record| record.epoch)
}

/// Apply Kafka current_leader_epoch fencing semantics.
///
/// - `None` or `-1` means no fencing.
/// - Lower than current → `FencedLeaderEpoch`.
/// - Higher than current → `UnknownLeaderEpoch`.
/// - Equal → OK (returns `None`).
pub(crate) fn current_leader_epoch_error(
    requested: Option<i32>,
    history: &[LeaderEpochRecord],
) -> Option<ErrorCode> {
    let requested = requested?;

    if requested == -1 {
        return None;
    }

    let Some(current) = current_leader_epoch(history) else {
        return Some(ErrorCode::UnknownLeaderEpoch);
    };

    if requested < current.epoch {
        Some(ErrorCode::FencedLeaderEpoch)
    } else if requested > current.epoch {
        Some(ErrorCode::UnknownLeaderEpoch)
    } else {
        None
    }
}
