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

//! Embedded in-process Jansu broker for single-binary deployments.
//!
//! `jansu-broker::Broker::listen()` binds a TCP socket. That's the right shape
//! for a deployment where producers/consumers live in separate processes, but
//! it's overkill (and a footgun: surprise port-bind, surprise SO_REUSEADDR
//! collisions, surprise TLS) for the case where a single binary just wants a
//! durable in-process event log: e.g. a control-plane daemon dispatching its
//! own webhook events.
//!
//! This crate is that thinner shape. It wraps the [`jansu_storage::Storage`]
//! trait directly: produce → `Storage::produce()`, consume → `Storage::fetch()`.
//! The broker registration handshake is automated. No TCP, no `rama` service
//! tower, no advertised listener nonsense.
//!
//! # Example
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! let broker = jansu_embedded::EmbeddedBroker::new().await?;
//! broker.create_topic("events", 1).await?;
//! broker.send("events", 0, None, b"hello").await?;
//!
//! let mut consumer = broker.consumer("events", 0, 0);
//! if let Some(record) = consumer.next().await? {
//!     assert_eq!(record.payload, b"hello");
//! }
//! # Ok(())
//! # }
//! ```

#![allow(unused_results)]
#![warn(missing_docs)]

use std::sync::Arc;

use bytes::Bytes;
use jansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, inflated},
};
use jansu_storage::{BrokerRegistrationRequest, Storage, StorageContainer, Topition};
use tracing::debug;
use url::Url;
use uuid::Uuid;

/// Re-export for callers that want to construct `Storage` impls directly.
pub use jansu_storage;

/// Error type for the embedded broker.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Underlying storage error.
    #[error("storage error: {0}")]
    Storage(#[from] jansu_storage::Error),
    /// Schema / codec error.
    #[error("codec error: {0}")]
    Codec(#[from] jansu_sans_io::Error),
    /// URL parse error (when configuring custom storage backends).
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    /// Misuse — argument out of bounds, missing required field, etc.
    #[error("misuse: {0}")]
    Misuse(String),
}

/// Convenience `Result` alias.
pub type Result<T> = std::result::Result<T, Error>;

const DEFAULT_CLUSTER_ID: &str = "jansu-embedded";
const DEFAULT_BROKER_ID: i32 = 1;
const DEFAULT_REPLICATION_FACTOR: i16 = 0;
const DEFAULT_TRANSACTION_TIMEOUT_MS: i32 = 60_000;
const DEFAULT_FETCH_MIN_BYTES: u32 = 1;
const DEFAULT_FETCH_MAX_BYTES: u32 = 1024 * 1024;

// ---------------------------------------------------------------------------
// EmbeddedRecord — owned record returned by Consumer::next
// ---------------------------------------------------------------------------

/// One record materialized from a fetched batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedRecord {
    /// Topic the record was produced to.
    pub topic: String,
    /// Partition the record was produced to.
    pub partition: i32,
    /// Absolute offset within the partition.
    pub offset: i64,
    /// Optional Kafka message key (typically used for idempotency hashing).
    pub key: Option<Vec<u8>>,
    /// Record payload bytes.
    pub payload: Vec<u8>,
}

// ---------------------------------------------------------------------------
// EmbeddedBroker — main entrypoint
// ---------------------------------------------------------------------------

/// Embedded broker handle. Cheap to `Clone`; clones share the same storage.
#[derive(Clone)]
pub struct EmbeddedBroker {
    storage: Arc<Box<dyn Storage>>,
    cluster_id: String,
    broker_id: i32,
}

impl std::fmt::Debug for EmbeddedBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedBroker")
            .field("cluster_id", &self.cluster_id)
            .field("broker_id", &self.broker_id)
            .finish()
    }
}

impl EmbeddedBroker {
    /// Start an in-memory broker with default settings.
    pub async fn new() -> Result<Self> {
        Self::builder().build().await
    }

    /// Configure an embedded broker.
    #[must_use]
    pub fn builder() -> EmbeddedBrokerBuilder {
        EmbeddedBrokerBuilder::default()
    }

    /// Create a topic with `num_partitions` partitions. Idempotent — calling
    /// this twice with the same name returns Ok on the second call (the
    /// underlying storage returns the existing topic id).
    pub async fn create_topic(&self, name: &str, num_partitions: i32) -> Result<()> {
        if num_partitions < 1 {
            return Err(Error::Misuse(format!(
                "num_partitions must be >= 1, got {num_partitions}"
            )));
        }
        let topic_id = self
            .storage
            .create_topic(
                CreatableTopic::default()
                    .name(name.to_string())
                    .num_partitions(num_partitions)
                    .replication_factor(DEFAULT_REPLICATION_FACTOR)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;
        debug!(?topic_id, %name, num_partitions, "created topic");
        Ok(())
    }

    /// Produce a single record to `(topic, partition)`. Returns the assigned
    /// offset.
    pub async fn send(
        &self,
        topic: &str,
        partition: i32,
        key: Option<&[u8]>,
        payload: &[u8],
    ) -> Result<i64> {
        let producer = self
            .storage
            .init_producer(None, DEFAULT_TRANSACTION_TIMEOUT_MS, Some(-1), Some(-1))
            .await?;
        let mut record = Record::builder().value(Bytes::copy_from_slice(payload).into());
        if let Some(k) = key {
            record = record.key(Some(Bytes::copy_from_slice(k)));
        }
        let batch = inflated::Batch::builder()
            .record(record)
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .build()
            .and_then(TryInto::try_into)?;
        let topition = Topition::new(topic.to_string(), partition);
        let offset = self.storage.produce(None, &topition, batch).await?;
        Ok(offset)
    }

    /// Build a consumer that yields records from `(topic, partition)` starting
    /// at `start_offset`. The consumer is single-use; spawn a fresh one per
    /// task that needs to read.
    #[must_use]
    pub fn consumer(&self, topic: &str, partition: i32, start_offset: i64) -> Consumer {
        Consumer {
            storage: Arc::clone(&self.storage),
            topition: Topition::new(topic.to_string(), partition),
            topic: topic.to_string(),
            partition,
            offset: start_offset,
            buffer: Vec::new(),
            min_bytes: DEFAULT_FETCH_MIN_BYTES,
            max_bytes: DEFAULT_FETCH_MAX_BYTES,
            isolation: IsolationLevel::ReadUncommitted,
        }
    }

    /// Borrow the underlying Storage (escape hatch for advanced Kafka API
    /// calls not covered by the embedded surface).
    #[must_use]
    pub fn storage(&self) -> &Arc<Box<dyn Storage>> {
        &self.storage
    }
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

/// Sequential consumer for a single (topic, partition).
///
/// `next()` returns one record at a time. Internally fetches batches from
/// storage as needed and decodes them into [`EmbeddedRecord`]s.
pub struct Consumer {
    storage: Arc<Box<dyn Storage>>,
    topition: Topition,
    topic: String,
    partition: i32,
    offset: i64,
    buffer: Vec<EmbeddedRecord>,
    min_bytes: u32,
    max_bytes: u32,
    isolation: IsolationLevel,
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("next_offset", &self.offset)
            .field("buffered", &self.buffer.len())
            .finish()
    }
}

impl Consumer {
    /// Return the next record, or `Ok(None)` if no records are available at
    /// the current offset.
    pub async fn next(&mut self) -> Result<Option<EmbeddedRecord>> {
        if let Some(record) = self.buffer.pop() {
            self.offset = record.offset + 1;
            return Ok(Some(record));
        }
        let batches = self
            .storage
            .fetch(
                &self.topition,
                self.offset,
                self.min_bytes,
                self.max_bytes,
                self.isolation,
            )
            .await?;
        for batch in batches {
            let inflated = inflated::Batch::try_from(batch)?;
            let base = inflated.base_offset;
            for record in inflated.records {
                let abs_offset = base + i64::from(record.offset_delta);
                if abs_offset < self.offset {
                    continue;
                }
                self.buffer.push(EmbeddedRecord {
                    topic: self.topic.clone(),
                    partition: self.partition,
                    offset: abs_offset,
                    key: record.key.as_ref().map(|b| b.to_vec()),
                    payload: record.value().map(|b| b.to_vec()).unwrap_or_default(),
                });
            }
        }
        // Buffer is now newest-first; pop returns the oldest first if we
        // reverse it.
        self.buffer.reverse();
        if let Some(record) = self.buffer.pop() {
            self.offset = record.offset + 1;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    /// Current offset (next to read).
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Skip ahead to a specific offset (e.g. after restart from a committed
    /// offset). Empties the local buffer.
    pub fn seek(&mut self, offset: i64) {
        self.offset = offset;
        self.buffer.clear();
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Fluent builder for [`EmbeddedBroker`].
#[derive(Debug, Clone)]
pub struct EmbeddedBrokerBuilder {
    cluster_id: String,
    broker_id: i32,
    storage_url: Url,
    advertised_listener: Url,
    incarnation_id: Option<Uuid>,
}

impl Default for EmbeddedBrokerBuilder {
    fn default() -> Self {
        Self {
            cluster_id: DEFAULT_CLUSTER_ID.into(),
            broker_id: DEFAULT_BROKER_ID,
            // Defaults to ObjectStore::InMemory (the `memory://` scheme is
            // mapped to a process-local in-memory store inside jansu-storage).
            storage_url: Url::parse("memory://").expect("static URL"),
            // The broker is in-process; the advertised listener is irrelevant
            // for embed use but must be a parseable URL.
            advertised_listener: Url::parse("tcp://127.0.0.1:0").expect("static URL"),
            incarnation_id: None,
        }
    }
}

impl EmbeddedBrokerBuilder {
    /// Override the cluster id. Defaults to `"jansu-embedded"`.
    #[must_use]
    pub fn cluster_id(mut self, id: impl Into<String>) -> Self {
        self.cluster_id = id.into();
        self
    }

    /// Override the broker id. Defaults to 1.
    #[must_use]
    pub fn broker_id(mut self, id: i32) -> Self {
        self.broker_id = id;
        self
    }

    /// Use a different storage backend. Defaults to `memory://` (in-memory,
    /// ephemeral). For persistent storage callers can pass e.g.
    /// `Url::parse("slatedb://memory")?`. See `jansu-storage` for the full
    /// list of supported schemes.
    #[must_use]
    pub fn storage(mut self, url: Url) -> Self {
        self.storage_url = url;
        self
    }

    /// Pre-seed the broker incarnation id (otherwise a fresh v7 UUID is used).
    #[must_use]
    pub fn incarnation_id(mut self, id: Uuid) -> Self {
        self.incarnation_id = Some(id);
        self
    }

    /// Finalize the builder, open storage, and register the embedded broker.
    pub async fn build(self) -> Result<EmbeddedBroker> {
        let storage = StorageContainer::builder()
            .cluster_id(self.cluster_id.clone())
            .node_id(self.broker_id)
            .advertised_listener(self.advertised_listener)
            .schema_registry(None)
            .storage(self.storage_url)
            .build()
            .await?;
        let storage_arc: Arc<Box<dyn Storage>> = storage;
        storage_arc
            .register_broker(BrokerRegistrationRequest {
                broker_id: self.broker_id,
                cluster_id: self.cluster_id.clone(),
                incarnation_id: self.incarnation_id.unwrap_or_else(Uuid::now_v7),
                rack: None,
            })
            .await?;
        Ok(EmbeddedBroker {
            storage: storage_arc,
            cluster_id: self.cluster_id,
            broker_id: self.broker_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn assert_send<T: Send>() {}
    #[allow(dead_code)]
    fn assert_sync<T: Sync>() {}

    #[test]
    fn broker_is_send_sync() {
        assert_send::<EmbeddedBroker>();
        assert_sync::<EmbeddedBroker>();
    }

    #[test]
    fn consumer_is_send() {
        assert_send::<Consumer>();
    }

    #[test]
    fn record_is_send_sync() {
        assert_send::<EmbeddedRecord>();
        assert_sync::<EmbeddedRecord>();
    }
}
