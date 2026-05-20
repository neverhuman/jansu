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

//! Transaction-related type definitions for SlateDB storage.

use std::{collections::BTreeMap, time::SystemTime};

use serde::{Deserialize, Serialize};

use crate::TxnState;

use super::{Group, Offset, Partition, ProducerEpoch, ProducerId, Topic};

/// Transaction produce offset range
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub(in crate::slate) struct TxnProduceOffset {
    pub offset_start: Offset,
    pub offset_end: Offset,
}

/// Transaction commit offset
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(default)]
pub(in crate::slate) struct TxnCommitOffset {
    pub committed_offset: Offset,
    pub leader_epoch: Option<i32>,
    pub metadata: Option<String>,
    pub commit_timestamp: Option<SystemTime>,
    pub expires_at: Option<SystemTime>,
}

/// Transaction identifier
#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct TxnId {
    pub transaction: String,
    pub producer_id: ProducerId,
    pub producer_epoch: ProducerEpoch,
    pub state: TxnState,
}

/// Transaction state
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct Txn {
    pub producer: ProducerId,
    pub epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

/// Transaction detail
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(in crate::slate) struct TxnDetail {
    pub transaction_timeout_ms: i32,
    pub started_at: Option<SystemTime>,
    pub state: Option<TxnState>,
    pub produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    pub offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}
