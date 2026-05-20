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

//! Type definitions for SlateDB storage engine
//!
//! # Key Design for LSM-tree
//!
//! All keys follow a consistent pattern optimized for LSM-tree storage:
//!
//! ```text
//! {type_prefix}/{hierarchy...}/{leaf_id}
//! ```
//!
//! ## Key Prefixes (sorted by access pattern)
//!
//! | Prefix | Description | Key Structure |
//! |--------|-------------|---------------|
//! | `b/` | Batch data | `b/{topic_uuid}/{partition:be32}/{offset:be64}` |
//! | `c/` | Consumer group commits | `c/{group}/{topic}/{partition:be32}` |
//! | `g/` | Group state | `g/{group_id}` |
//! | `w/` | Watermarks | `w/{topic_uuid}/{partition:be32}` |
//!
//! ## Design Principles
//!
//! 1. **Prefix-first**: Type prefix comes first for efficient filtering
//! 2. **Big-endian integers**: Preserves numeric ordering in lexicographic sort
//! 3. **Fixed-width encoding**: Ensures consistent key ordering
//! 4. **Hierarchical structure**: Enables efficient prefix scans
//!
//! ## LSM-tree Considerations
//!
//! - Keys with same prefix are stored together → better compaction
//! - Bloom filters can efficiently skip unrelated key types
//! - Range scans for a partition only touch relevant SSTable blocks

mod keys;
mod txn;

pub(in crate::slate) use keys::{
    BatchKey, BatchKeyPrefix, GroupKey, GroupKeyPrefix, LeaderEpochKey, LeaderEpochKeyPrefix,
    OffsetCommitKey, OffsetCommitKeyPrefix, WatermarkKey,
};
#[allow(unused_imports)]
pub(in crate::slate) use txn::TxnId;
pub(in crate::slate) use txn::{Txn, TxnCommitOffset, TxnDetail, TxnProduceOffset};

use std::{collections::BTreeMap, time::SystemTime};

use jansu_sans_io::create_topics_request::CreatableTopic;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{GroupDetail, Version};

// Type aliases
pub(in crate::slate) type Group = String;
pub(in crate::slate) type Offset = i64;
pub(in crate::slate) type Partition = i32;
pub(in crate::slate) type ProducerEpoch = i16;
pub(in crate::slate) type ProducerId = i64;
pub(in crate::slate) type Sequence = i32;
pub(in crate::slate) type Topic = String;

// Collection types
pub(in crate::slate) type Topics = BTreeMap<Topic, TopicMetadata>;
pub(in crate::slate) type Producers = BTreeMap<ProducerId, ProducerDetail>;
pub(in crate::slate) type Brokers = BTreeMap<i32, BrokerInfo>;
pub(in crate::slate) type Transactions = BTreeMap<String, Txn>;

/// Topic metadata
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct TopicMetadata {
    pub id: Uuid,
    pub topic: CreatableTopic,
}

/// Watermark for a topic partition
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct Watermark {
    pub low: Option<i64>,
    pub high: Option<i64>,
    pub timestamps: Option<BTreeMap<i64, i64>>,
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub(in crate::slate) struct LeaderEpochValue {
    pub start_offset: Offset,
}

/// Group detail with version
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct GroupDetailVersion {
    pub detail: GroupDetail,
    pub version: Version,
}

impl GroupDetailVersion {
    pub(in crate::slate) fn detail(self, detail: GroupDetail) -> Self {
        Self { detail, ..self }
    }

    pub(in crate::slate) fn version(self, version: Version) -> Self {
        Self { version, ..self }
    }
}

/// Producer detail with sequence tracking
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct ProducerDetail {
    pub sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
}

/// Value stored for offset commits
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(default)]
pub(in crate::slate) struct OffsetCommitValue {
    pub offset: i64,
    pub leader_epoch: Option<i32>,
    pub metadata: Option<String>,
    pub commit_timestamp: Option<SystemTime>,
    pub expires_at: Option<SystemTime>,
}

/// Stored broker information
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct BrokerInfo {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}
