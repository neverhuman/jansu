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

//! Notify-driven long-poll fetch support for the DynoStore backend.

use std::{sync::Arc, time::Duration};

use jansu_sans_io::{IsolationLevel, record::deflated};
use tokio::sync::Notify;

use super::DynoStore;
use crate::{Result, Storage, Topition};

impl DynoStore {
    /// Get-or-create the produce notifier for a given topic-partition. Cheap
    /// (one mutex acquire, one `BTreeMap` lookup); the result is shared so
    /// every waiter receives every `notify_waiters` wake.
    pub(super) fn produce_notifier(&self, topition: &Topition) -> Arc<Notify> {
        self.produce_notify
            .lock()
            .expect("produce_notify mutex poisoned")
            .entry(topition.to_owned())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Notify-driven long-poll fetch used by `Storage::fetch_wait`. Returns
    /// as soon as new data lands at or after `offset`, or `max_wait`
    /// elapses — whichever comes first. Falls back to a single immediate
    /// `fetch` if `max_wait.is_zero()`.
    ///
    /// Implements Kafka's `fetch.max.wait.ms` semantics by subscribing to
    /// the per-topition `tokio::sync::Notify` *before* the initial fetch
    /// (so a produce that races between the fetch and the notified wait
    /// still wakes us), then awaiting either the notification or the
    /// deadline. On wake we re-fetch and return whatever is available.
    pub(super) async fn fetch_wait_notify(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        if max_wait.is_zero() {
            return <Self as Storage>::fetch(
                self,
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation_level,
            )
            .await;
        }

        let deadline = tokio::time::Instant::now() + max_wait;
        let notifier = self.produce_notifier(topition);

        loop {
            // Subscribe to the next notify *before* we sample the log, so a
            // concurrent produce that lands between the fetch and the await
            // is still observed (the `Notified` future is armed at the
            // point we call `notified()`).
            let notified = notifier.notified();
            tokio::pin!(notified);

            let batches = <Self as Storage>::fetch(
                self,
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation_level,
            )
            .await?;

            if !batches.is_empty() {
                return Ok(batches);
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(batches);
            }

            let remaining = deadline - now;

            tokio::select! {
                () = &mut notified => {
                    // produce woke us — loop and try fetching again.
                    continue;
                }
                () = tokio::time::sleep(remaining) => {
                    // deadline expired without a produce. Return whatever
                    // is currently in the log (likely empty).
                    return <Self as Storage>::fetch(
                        self,
                        topition,
                        offset,
                        min_bytes,
                        max_bytes,
                        isolation_level,
                    )
                    .await;
                }
            }
        }
    }
}
