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

//! Regression: Consumer::next() must advance `self.offset` on every pop,
//! not just the post-fetch one. Previously the early-return path (when the
//! buffer still held records from a prior fetch) returned a record without
//! bumping `self.offset`, so when the buffer drained the next fetch
//! re-fetched the same batch — downstream consumers saw duplicate records
//! at batch boundaries. Flagged by jeryu's
//! `jansu_consumer_resumes_after_restart` integration which observed
//! offsets `[2, 3, 4, 3, 4, 4]` instead of `[2, 3, 4]`. Tracking ID: J-4.

#![allow(unused_results)]

use jansu_embedded::EmbeddedBroker;

#[tokio::test]
async fn consumer_drains_batch_without_redelivery() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    broker.create_topic("test", 1).await.expect("create topic");

    for i in 0..5_i32 {
        let payload = format!("r{i}");
        broker
            .send("test", 0, None, payload.as_bytes())
            .await
            .expect("produce");
    }

    let mut consumer = broker.consumer("test", 0, 0);
    let mut observed = Vec::new();
    // Hard cap on iterations to keep the test bounded even if the bug
    // resurfaces (a regression would loop forever otherwise).
    for _ in 0..32 {
        match consumer.next().await.expect("fetch ok") {
            Some(record) => observed.push(record.offset),
            None => break,
        }
    }

    assert_eq!(
        observed,
        vec![0, 1, 2, 3, 4],
        "consumer must yield each offset exactly once across batch drain; \
         got {observed:?}",
    );
}
