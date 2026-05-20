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

//! Composite key definitions for SlateDB storage.
//!
//! All keys follow a consistent prefix-first, big-endian, fixed-width pattern
//! optimized for LSM-tree storage. See the parent module for the full key design.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::Partition;

/// Key for watermark storage: `w/{topic_uuid}/{partition:be32}`
///
/// Watermarks are accessed per-partition, so we use topic+partition as the key.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct WatermarkKey {
    /// Type prefix for LSM-tree grouping
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl Default for WatermarkKey {
    fn default() -> Self {
        Self {
            prefix: 'w',
            topic: Uuid::nil(),
            partition: 0,
        }
    }
}

impl WatermarkKey {
    pub(in crate::slate) fn new(topic: Uuid, partition: Partition) -> Self {
        Self {
            prefix: 'w',
            topic,
            partition,
        }
    }
}

/// Key for leader epoch history: `e/{topic_uuid}/{partition:be32}/{epoch:be32}`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct LeaderEpochKey {
    pub prefix: char,
    pub topic: Uuid,
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
    #[serde(with = "postcard::fixint::be")]
    pub epoch: i32,
}

impl LeaderEpochKey {
    pub(in crate::slate) fn new(topic: Uuid, partition: Partition, epoch: i32) -> Self {
        Self {
            prefix: 'e',
            topic,
            partition,
            epoch,
        }
    }
}

/// Prefix key for scanning all leader epochs in a topic partition.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct LeaderEpochKeyPrefix {
    pub prefix: char,
    pub topic: Uuid,
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl LeaderEpochKeyPrefix {
    pub(in crate::slate) fn new(topic: Uuid, partition: Partition) -> Self {
        Self {
            prefix: 'e',
            topic,
            partition,
        }
    }
}

/// Key for batch storage: `b/{topic_uuid}/{partition:be32}/{offset:be64}`
///
/// Batches are the most frequently accessed data. The key structure enables:
/// - Efficient sequential reads by offset (big-endian preserves order)
/// - Prefix scan for all batches in a partition
/// - Bloom filter can quickly skip non-batch keys
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct BatchKey {
    /// Type prefix 'b' for batch
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
    /// Offset within partition (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub offset: super::Offset,
}

impl Default for BatchKey {
    fn default() -> Self {
        Self {
            prefix: 'b',
            topic: Uuid::nil(),
            partition: 0,
            offset: 0,
        }
    }
}

impl BatchKey {
    pub(in crate::slate) fn new(topic: Uuid, partition: Partition, offset: super::Offset) -> Self {
        Self {
            prefix: 'b',
            topic,
            partition,
            offset,
        }
    }

    /// Create a key for range scan starting from this offset
    pub(in crate::slate) fn scan_from(
        topic: Uuid,
        partition: Partition,
        offset: super::Offset,
    ) -> Self {
        Self::new(topic, partition, offset)
    }
}

/// Prefix key for scanning all batches in a topic partition: `b/{topic_uuid}/{partition}`
///
/// This is a separate struct from `BatchKey` because we need to check if a scanned key
/// still belongs to the same topic/partition before decoding the batch data.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct BatchKeyPrefix {
    /// Type prefix 'b' for batch
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl BatchKeyPrefix {
    pub(in crate::slate) fn new(topic: Uuid, partition: Partition) -> Self {
        Self {
            prefix: 'b',
            topic,
            partition,
        }
    }
}

/// Key for storing committed offsets: `c/{group}/{topic}/{partition:be32}`
///
/// Consumer group offsets are accessed by group, then by topic-partition.
/// This enables efficient "get all offsets for a group" scans.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct OffsetCommitKey {
    /// Type prefix 'c' for commit
    pub prefix: char,
    /// Consumer group ID
    pub group: String,
    /// Topic name
    pub topic: String,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl Default for OffsetCommitKey {
    fn default() -> Self {
        Self {
            prefix: 'c',
            group: String::new(),
            topic: String::new(),
            partition: 0,
        }
    }
}

impl OffsetCommitKey {
    pub(in crate::slate) fn new(
        group: impl Into<String>,
        topic: impl Into<String>,
        partition: Partition,
    ) -> Self {
        Self {
            prefix: 'c',
            group: group.into(),
            topic: topic.into(),
            partition,
        }
    }
}

/// Prefix key for scanning all offsets in a consumer group: `c/{group}`
///
/// This is a separate struct from `OffsetCommitKey` because postcard serialization
/// includes length prefixes for strings, so we can't use an `OffsetCommitKey` with
/// empty topic as a scan prefix - it would include the empty string's length marker.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct OffsetCommitKeyPrefix {
    /// Type prefix 'c' for commit
    pub prefix: char,
    /// Consumer group ID
    pub group: String,
}

impl OffsetCommitKeyPrefix {
    pub(in crate::slate) fn new(group: impl Into<String>) -> Self {
        Self {
            prefix: 'c',
            group: group.into(),
        }
    }
}

/// Key for storing group state: `g/{group_id}`
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct GroupKey {
    /// Type prefix 'g' for group
    pub prefix: char,
    /// Group ID
    pub group_id: String,
}

impl Default for GroupKey {
    fn default() -> Self {
        Self {
            prefix: 'g',
            group_id: String::new(),
        }
    }
}

impl GroupKey {
    pub(in crate::slate) fn new(group_id: impl Into<String>) -> Self {
        Self {
            prefix: 'g',
            group_id: group_id.into(),
        }
    }
}

/// Prefix key for scanning all groups: `g`
///
/// This is a separate struct from `GroupKey` because postcard serialization
/// includes length prefixes for strings, so we can't use a `GroupKey` with
/// empty group_id as a scan prefix.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct GroupKeyPrefix {
    /// Type prefix 'g' for group
    pub prefix: char,
}

impl GroupKeyPrefix {
    pub(in crate::slate) fn new() -> Self {
        Self { prefix: 'g' }
    }
}
