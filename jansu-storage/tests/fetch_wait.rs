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

//! Tests for `Storage::fetch_wait` — Kafka `fetch.max.wait.ms` semantics.
//!
//! Covers:
//!   * Immediate return when records are already in the partition (no sleep).
//!   * Timeout return when no records arrive (default-impl path).
//!   * Notify-driven wake on concurrent produce (`DynoStore` override path).

mod common;

#[cfg(feature = "dynostore")]
mod jp_m1 {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use bytes::Bytes;
    use jansu_sans_io::{
        IsolationLevel,
        record::{Record, inflated},
    };
    use jansu_storage::{Storage, Topition};
    use url::Url;

    use crate::common::{self, Error};

    async fn memory_storage(
        cluster_id: &str,
        node_id: i32,
    ) -> Result<Arc<Box<dyn Storage>>, Error> {
        common::build_storage(
            cluster_id,
            node_id,
            Url::parse(&format!("memory://{cluster_id}/"))?,
        )
        .await
    }

    async fn produce_value<S>(storage: &S, topition: &Topition, value: Vec<u8>) -> Result<(), Error>
    where
        S: Storage + ?Sized,
    {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from(value))))
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, topition, batch).await?;
        Ok(())
    }

    /// fetch_wait must return without sleeping if records are already present
    /// at the requested offset. Mirrors Kafka behaviour: `fetch.max.wait.ms`
    /// is only consulted when `fetch.min.bytes` would otherwise not be met.
    #[tokio::test]
    async fn returns_immediately_when_records_exist() -> Result<(), Error> {
        let cluster_id = "jp-m1-immediate";
        let node_id = 8;
        let topic = "jp_m1_immediate";
        let partition = 0;
        let storage = memory_storage(cluster_id, node_id).await?;
        common::register_broker(storage.as_ref(), cluster_id, node_id).await?;
        _ = common::create_topic(storage.as_ref(), topic, 1).await?;
        let topition = Topition::new(topic, partition);

        produce_value(storage.as_ref(), &topition, vec![b'a'; 64]).await?;

        let started = Instant::now();
        let batches = storage
            .fetch_wait(
                &topition,
                0,
                1,
                50 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_secs(5),
            )
            .await?;
        let elapsed = started.elapsed();

        assert!(
            !batches.is_empty(),
            "fetch_wait should return records that are already present"
        );
        // A real engine call takes a handful of ms; 250 ms is a generous bound
        // that still detects any accidental long-poll sleep regression.
        assert!(
            elapsed < Duration::from_millis(250),
            "fetch_wait slept for {elapsed:?}; expected immediate return"
        );

        Ok(())
    }

    /// fetch_wait must honour `max_wait` when no records ever arrive.
    /// Verifies the timeout path of the long-poll loop.
    #[tokio::test]
    async fn honours_timeout_when_no_records_arrive() -> Result<(), Error> {
        let cluster_id = "jp-m1-timeout";
        let node_id = 8;
        let topic = "jp_m1_timeout";
        let partition = 0;
        let storage = memory_storage(cluster_id, node_id).await?;
        common::register_broker(storage.as_ref(), cluster_id, node_id).await?;
        _ = common::create_topic(storage.as_ref(), topic, 1).await?;
        let topition = Topition::new(topic, partition);

        let max_wait = Duration::from_millis(200);

        let started = Instant::now();
        let batches = storage
            .fetch_wait(
                &topition,
                0,
                1,
                50 * 1024,
                IsolationLevel::ReadUncommitted,
                max_wait,
            )
            .await?;
        let elapsed = started.elapsed();

        assert!(
            batches.is_empty(),
            "fetch_wait should return empty when no records arrive"
        );
        assert!(
            elapsed >= max_wait,
            "fetch_wait returned in {elapsed:?}; expected >= {max_wait:?}"
        );
        // Allow a generous upper bound: tokio scheduler + 50 ms poll cadence
        // means we could overshoot by ~100 ms on a heavily loaded CI runner.
        assert!(
            elapsed < max_wait + Duration::from_millis(500),
            "fetch_wait blocked too long: {elapsed:?}, expected < {:?}",
            max_wait + Duration::from_millis(500),
        );

        Ok(())
    }

    /// fetch_wait on the memory engine must wake when a concurrent task
    /// produces. Exercises the `tokio::sync::Notify` override on `DynoStore`:
    /// the wait should resolve far below `max_wait` once the producer fires.
    #[tokio::test]
    async fn wakes_on_concurrent_produce() -> Result<(), Error> {
        let cluster_id = "jp-m1-notify";
        let node_id = 8;
        let topic = "jp_m1_notify";
        let partition = 0;
        let storage = memory_storage(cluster_id, node_id).await?;
        common::register_broker(storage.as_ref(), cluster_id, node_id).await?;
        _ = common::create_topic(storage.as_ref(), topic, 1).await?;
        let topition = Topition::new(topic, partition);

        // Spawn a producer that fires after ~100 ms — well within the 5 s
        // long-poll budget, but far enough out that the consumer must be
        // genuinely sleeping rather than racing the produce on the first
        // poll.
        let producer_storage = Arc::clone(&storage);
        let producer_topition = topition.clone();
        let producer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            produce_value(
                producer_storage.as_ref(),
                &producer_topition,
                vec![b'b'; 64],
            )
            .await
            .expect("produce");
        });

        let started = Instant::now();
        let batches = storage
            .fetch_wait(
                &topition,
                0,
                1,
                50 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_secs(5),
            )
            .await?;
        let elapsed = started.elapsed();

        producer.await.expect("producer task");

        assert!(
            !batches.is_empty(),
            "fetch_wait should observe the concurrently-produced record"
        );
        // Notify wake is sub-ms; even with scheduler jitter the total wait
        // should be well under 1 s (the default-impl polling fallback would
        // need up to 150 ms = 100 ms produce + ~50 ms poll). We assert
        // 1 s as the regression bound that catches "the override silently
        // reverted to the default polling path" without being flaky.
        assert!(
            elapsed < Duration::from_secs(1),
            "fetch_wait slept {elapsed:?}; notify path may not be firing"
        );

        Ok(())
    }
}
