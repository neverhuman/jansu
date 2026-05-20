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

//! List offsets operation for the SlateDB engine.

use std::time::SystemTime;

use jansu_sans_io::{ErrorCode, IsolationLevel, ListOffset, to_system_time};

use crate::{Error, ListOffsetResponse, Result, Topition};

use super::super::engine::Engine;
use super::super::types::{Watermark, WatermarkKey};

impl Engine {
    pub(in crate::slate) async fn list_offsets_op(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let topics = self.get_topics().await?;
        let mut responses = Vec::with_capacity(offsets.len());

        for (topition, list_offset) in offsets {
            let Some(metadata) = topics.get(&topition.topic[..]) else {
                responses.push((
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        offset: None,
                        timestamp: None,
                    },
                ));
                continue;
            };

            if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
                responses.push((
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        offset: None,
                        timestamp: None,
                    },
                ));
                continue;
            }

            let watermark_key =
                postcard::to_stdvec(&WatermarkKey::new(metadata.id, topition.partition))?;

            let watermark = self
                .db
                .get(&watermark_key)
                .await
                .map_err(Error::from)
                .and_then(|watermark| {
                    watermark.map_or(Ok(Watermark::default()), |encoded| {
                        postcard::from_bytes(&encoded[..]).map_err(Into::into)
                    })
                })?;

            let response = match list_offset {
                ListOffset::Earliest => {
                    if let Some((ts, off)) = watermark
                        .timestamps
                        .as_ref()
                        .and_then(|ts| ts.first_key_value())
                    {
                        ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(*off),
                            timestamp: to_system_time(*ts).ok(),
                        }
                    } else {
                        ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(0),
                            timestamp: None,
                        }
                    }
                }
                ListOffset::Latest => {
                    // For ReadCommitted, return Last Stable Offset instead of High Watermark
                    let offset = if isolation_level == IsolationLevel::ReadCommitted {
                        let offset_stage = self.offset_stage_op(topition).await?;
                        offset_stage.last_stable
                    } else {
                        watermark.high.unwrap_or(0)
                    };
                    let timestamp = watermark
                        .timestamps
                        .as_ref()
                        .and_then(|ts| ts.last_key_value())
                        .and_then(|(ts, _)| to_system_time(*ts).ok());

                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(offset),
                        timestamp,
                    }
                }
                ListOffset::Timestamp(target_ts) => {
                    // Find the first offset with timestamp >= target
                    // target_ts is SystemTime, need to convert to i64 for comparison
                    let target_millis = target_ts
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);

                    let result = watermark.timestamps.as_ref().and_then(|ts| {
                        ts.range(target_millis..)
                            .next()
                            .map(|(ts, off)| (*off, *ts))
                    });

                    match result {
                        Some((offset, ts)) => ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(offset),
                            timestamp: to_system_time(ts).ok(),
                        },
                        // Match PostgreSQL behavior: return offset 0 when no match found
                        None => ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(0),
                            timestamp: None,
                        },
                    }
                }
            };

            responses.push((topition.clone(), response));
        }

        Ok(responses)
    }
}
