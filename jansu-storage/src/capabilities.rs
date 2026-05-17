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

//! Storage backend capability matrix.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Storage engine kind.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum StorageEngine {
    DynoStore,
    Memory,
    Null,
    Postgres,
    RedlineDb,
    SlateDb,
    Unknown,
}

/// Storage feature kind.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum StorageFeature {
    BatchValidation,
    CompressionCodecs,
    ContiguousOffsets,
    CrashRecovery,
    DeleteRecords,
    EmptyPartitionOffsets,
    FetchVisibility,
    HighWatermark,
    LastStableOffset,
    LeaderEpochHistory,
    ListOffsetsEarliestLatest,
    LogStartOffset,
    ProducerState,
    Retention,
    TimestampLookup,
    Transactions,
    Compaction,
}

/// Storage certification level.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Default,
)]
pub enum StorageCertification {
    ProductionParity,
    LimitedParity,
    DevelopmentTestOnly,
    Unsupported,
    #[default]
    Uncertified,
    NotApplicable,
}

/// Storage capability matrix.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StorageCapabilities {
    pub engine: StorageEngine,
    pub features: BTreeMap<StorageFeature, StorageCertification>,
}

impl Default for StorageCapabilities {
    fn default() -> Self {
        Self::new(StorageEngine::Unknown)
    }
}

impl StorageCapabilities {
    pub fn new(engine: StorageEngine) -> Self {
        Self {
            engine,
            features: BTreeMap::new(),
        }
    }

    pub fn certification(&self, feature: StorageFeature) -> StorageCertification {
        self.features
            .get(&feature)
            .copied()
            .unwrap_or(StorageCertification::Uncertified)
    }

    pub fn supports(&self, feature: StorageFeature) -> bool {
        matches!(
            self.certification(feature),
            StorageCertification::ProductionParity
                | StorageCertification::LimitedParity
                | StorageCertification::DevelopmentTestOnly
        )
    }

    fn phase06_core(engine: StorageEngine, certification: StorageCertification) -> Self {
        Self {
            engine,
            features: [
                (StorageFeature::BatchValidation, certification),
                (StorageFeature::ContiguousOffsets, certification),
                (StorageFeature::EmptyPartitionOffsets, certification),
                (StorageFeature::FetchVisibility, certification),
                (StorageFeature::ListOffsetsEarliestLatest, certification),
                (StorageFeature::TimestampLookup, certification),
                (StorageFeature::LogStartOffset, certification),
                (StorageFeature::HighWatermark, certification),
                (StorageFeature::LastStableOffset, certification),
            ]
            .into_iter()
            .collect(),
        }
    }

    pub fn phase06_postgres() -> Self {
        Self::phase06_core(
            StorageEngine::Postgres,
            StorageCertification::ProductionParity,
        )
    }

    pub fn phase06_memory() -> Self {
        Self::phase06_core(
            StorageEngine::Memory,
            StorageCertification::DevelopmentTestOnly,
        )
    }

    pub fn phase06_redlinedb() -> Self {
        Self::phase06_core(
            StorageEngine::RedlineDb,
            StorageCertification::LimitedParity,
        )
    }

    pub fn phase06_dynostore() -> Self {
        Self::phase06_core(
            StorageEngine::DynoStore,
            StorageCertification::LimitedParity,
        )
    }

    pub fn phase06_slatedb() -> Self {
        Self::phase06_core(StorageEngine::SlateDb, StorageCertification::LimitedParity)
    }

    pub fn phase06_null() -> Self {
        Self {
            engine: StorageEngine::Null,
            features: [
                (
                    StorageFeature::ContiguousOffsets,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::EmptyPartitionOffsets,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::FetchVisibility,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::LeaderEpochHistory,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::ListOffsetsEarliestLatest,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::TimestampLookup,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::LogStartOffset,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::HighWatermark,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::LastStableOffset,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::BatchValidation,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::CompressionCodecs,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::ProducerState,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::Transactions,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::DeleteRecords,
                    StorageCertification::Unsupported,
                ),
                (StorageFeature::Retention, StorageCertification::Unsupported),
                (
                    StorageFeature::Compaction,
                    StorageCertification::Unsupported,
                ),
                (
                    StorageFeature::CrashRecovery,
                    StorageCertification::Unsupported,
                ),
            ]
            .into_iter()
            .collect(),
        }
    }
}
