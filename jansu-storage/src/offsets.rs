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

//! Offset request / response domain types and consumer-group config helpers.

use std::time::{Duration, SystemTime};

use jansu_sans_io::{
    ErrorCode, ListOffset, offset_commit_request::OffsetCommitRequestPartition, to_system_time,
    to_timestamp,
};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

pub type ListOffsetRequest = ListOffset;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetResponse {
    pub error_code: ErrorCode,
    pub timestamp: Option<SystemTime>,
    pub offset: Option<i64>,
}

impl Default for ListOffsetResponse {
    fn default() -> Self {
        Self {
            error_code: ErrorCode::None,
            timestamp: None,
            offset: None,
        }
    }
}

impl ListOffsetResponse {
    pub fn offset(&self) -> Option<i64> {
        self.offset
    }

    pub fn timestamp(&self) -> Result<Option<i64>> {
        self.timestamp.map_or(Ok(None), |system_time| {
            to_timestamp(&system_time).map(Some).map_err(Into::into)
        })
    }

    pub fn error_code(&self) -> ErrorCode {
        self.error_code
    }
}

/// Offset Commit Request
///
/// A structure representing an [`jansu_sans_io::OffsetCommitRequestPartition](OffsetCommitRequestPartition).
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct OffsetCommitRequest {
    pub(crate) offset: i64,
    pub(crate) leader_epoch: Option<i32>,
    pub(crate) timestamp: Option<SystemTime>,
    pub(crate) metadata: Option<String>,
}

impl OffsetCommitRequest {
    pub fn offset(self, offset: i64) -> Self {
        Self { offset, ..self }
    }
}

pub(crate) const DEFAULT_OFFSET_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Committed offset record.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(default)]
pub struct OffsetFetchRecord {
    offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
    commit_timestamp: Option<SystemTime>,
    expires_at: Option<SystemTime>,
}

impl OffsetFetchRecord {
    pub fn with_offset(self, offset: i64) -> Self {
        Self { offset, ..self }
    }

    pub fn from_commit(
        commit: &OffsetCommitRequest,
        retention: Option<Duration>,
        now: SystemTime,
    ) -> Self {
        let expires_at = retention
            .or(Some(DEFAULT_OFFSET_RETENTION))
            .and_then(|retention| now.checked_add(retention));

        Self {
            offset: commit.offset,
            leader_epoch: commit.leader_epoch,
            metadata: commit.metadata.clone(),
            commit_timestamp: Some(commit.timestamp.unwrap_or(now)),
            expires_at,
        }
    }

    pub fn from_parts(
        offset: i64,
        leader_epoch: Option<i32>,
        metadata: Option<String>,
        commit_timestamp: Option<SystemTime>,
        expires_at: Option<SystemTime>,
    ) -> Self {
        Self {
            offset,
            leader_epoch,
            metadata,
            commit_timestamp,
            expires_at,
        }
    }

    pub fn expired(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }

    pub fn committed_offset(&self) -> i64 {
        self.offset
    }

    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    pub fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    pub fn commit_timestamp(&self) -> Option<SystemTime> {
        self.commit_timestamp
    }

    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }
}

impl TryFrom<&OffsetCommitRequestPartition> for OffsetCommitRequest {
    type Error = Error;

    fn try_from(value: &OffsetCommitRequestPartition) -> Result<Self, Self::Error> {
        value
            .commit_timestamp
            .map_or(Ok(None), |commit_timestamp| {
                to_system_time(commit_timestamp)
                    .map(Some)
                    .map_err(Into::into)
            })
            .map(|timestamp| Self {
                offset: value.committed_offset,
                leader_epoch: value.committed_leader_epoch,
                timestamp,
                metadata: value.committed_metadata.clone(),
            })
    }
}

/// Offset Stage
///
/// An offset stage structure represents the `last_stable`, `high_watermark` and `log_start` offsets.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct OffsetStage {
    pub(crate) last_stable: i64,
    pub(crate) high_watermark: i64,
    pub(crate) log_start: i64,
}

impl OffsetStage {
    pub fn last_stable(&self) -> i64 {
        self.last_stable
    }

    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    pub fn log_start(&self) -> i64 {
        self.log_start
    }
}

pub(crate) fn append_config_tokens(
    current: Option<&str>,
    addition: Option<&str>,
) -> Option<String> {
    let mut tokens = split_config_tokens(current);

    for token in split_config_tokens(addition) {
        if !tokens.iter().any(|existing| existing == &token) {
            tokens.push(token);
        }
    }

    join_config_tokens(tokens)
}

pub(crate) fn subtract_config_tokens(
    current: Option<&str>,
    subtraction: Option<&str>,
) -> Option<String> {
    let remove = split_config_tokens(subtraction);
    let tokens = split_config_tokens(current)
        .into_iter()
        .filter(|token| !remove.iter().any(|candidate| candidate == token))
        .collect::<Vec<_>>();

    join_config_tokens(tokens)
}

fn split_config_tokens(value: Option<&str>) -> Vec<String> {
    match value {
        Some(value) => value
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        None => Vec::new(),
    }
}

fn join_config_tokens(tokens: Vec<String>) -> Option<String> {
    if tokens.is_empty() {
        None
    } else {
        Some(tokens.join(","))
    }
}
