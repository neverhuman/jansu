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

//! Offset operations for the `null://` [`Engine`].

use std::collections::BTreeMap;

use jansu_sans_io::{ErrorCode, ListOffset};

use super::Engine;
use crate::{ListOffsetResponse, OffsetCommitRequest, Result, Topition};

impl Engine {
    pub(super) fn null_offset_commit(
        &self,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        Ok(offsets
            .iter()
            .map(|(topition, _)| (topition.to_owned(), ErrorCode::None))
            .collect())
    }

    pub(super) fn null_offset_fetch(&self, topics: &[Topition]) -> Result<BTreeMap<Topition, i64>> {
        Ok(topics
            .iter()
            .map(|topition| (topition.to_owned(), 0))
            .collect())
    }

    pub(super) fn null_list_offsets(
        &self,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        Ok(offsets
            .iter()
            .map(|(topition, _)| {
                (
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::KafkaStorageError,
                        timestamp: None,
                        offset: None,
                    },
                )
            })
            .collect())
    }
}
