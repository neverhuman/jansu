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

//! Offset-related Storage impl helpers: offset_stage, offset_commit, offset_for_leader_epoch,
//! offset_fetch_records, offset_fetch, list_offsets

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use jansu_sans_io::{ErrorCode, IsolationLevel, ListOffset, to_system_time};
use tracing::debug;

use crate::{
    DEFAULT_OFFSET_RETENTION, Error, LeaderEpochRecord, ListOffsetResponse, OffsetCommitRequest,
    OffsetFetchRecord, OffsetStage, Result, Topition, TxnState,
};

use super::engine::Engine;
use super::types::{
    GroupKey, LeaderEpochKey, LeaderEpochKeyPrefix, LeaderEpochValue, OffsetCommitKey,
    OffsetCommitKeyPrefix, OffsetCommitValue, Watermark, WatermarkKey,
};

impl Engine {
    pub(super) async fn impl_offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let topics = self.get_topics().await?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
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

        let high_watermark = watermark.high.unwrap_or(0);

        // Calculate last_stable by finding the minimum offset of any in-progress transaction
        let transactions = self.get_transactions().await?;
        let mut last_stable = high_watermark;

        for txn in transactions.values() {
            for txn_detail in txn.epochs.values() {
                // Consider transactions that are in-progress (Begin, PrepareCommit, or PrepareAbort)
                // These states indicate the transaction is not yet fully committed/aborted
                let is_in_progress = matches!(
                    txn_detail.state,
                    Some(TxnState::Begin)
                        | Some(TxnState::PrepareCommit)
                        | Some(TxnState::PrepareAbort)
                );
                if is_in_progress {
                    // Check if this transaction has produced to this topic/partition
                    if let Some(partitions) = txn_detail.produces.get(&topition.topic)
                        && let Some(Some(offset_range)) = partitions.get(&topition.partition)
                    {
                        // The last_stable should be the minimum of all in-flight txn start offsets
                        last_stable = last_stable.min(offset_range.offset_start);
                    }
                }
            }
        }

        Ok(OffsetStage {
            log_start: watermark.low.unwrap_or(0),
            last_stable,
            high_watermark,
        })
    }

    pub(super) async fn impl_offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let now = SystemTime::now();
        let expires_at = now.checked_add(retention.unwrap_or(DEFAULT_OFFSET_RETENTION));
        // NOTE: Reading global TOPICS map for validation is inefficient.
        let topics = self.get_topics().await?;
        let mut responses = Vec::with_capacity(offsets.len());

        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut group_inserted = false;

        for (topition, offset_commit) in offsets {
            // Verify topic exists
            if !topics.contains_key(&topition.topic[..]) {
                responses.push((topition.clone(), ErrorCode::UnknownTopicOrPartition));
                continue;
            }

            // Insert group entry if not already done in this transaction
            if !group_inserted {
                let group_key = postcard::to_stdvec(&GroupKey::new(group))?;
                // Check if group already exists
                if tx.get(&group_key).await?.is_none() {
                    // Create a minimal group entry for offset tracking
                    let group_value =
                        postcard::to_stdvec(&super::types::GroupDetailVersion::default())?;
                    tx.put(group_key, group_value)?;
                }
                group_inserted = true;
            }

            let key = postcard::to_stdvec(&OffsetCommitKey::new(
                group,
                &topition.topic,
                topition.partition,
            ))?;

            let value = postcard::to_stdvec(&OffsetCommitValue {
                offset: offset_commit.offset,
                leader_epoch: offset_commit.leader_epoch,
                metadata: offset_commit.metadata.clone(),
                commit_timestamp: Some(offset_commit.timestamp.unwrap_or(now)),
                expires_at,
            })?;

            tx.put(key, value)?;
            responses.push((topition.clone(), ErrorCode::None));
        }

        tx.commit().await.map_err(Error::from)?;

        Ok(responses)
    }

    pub(super) async fn impl_committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        let now = SystemTime::now();
        let prefix = postcard::to_stdvec(&OffsetCommitKeyPrefix::new(group_id))?;

        let mut topitions = BTreeMap::new();
        let mut scan = self.db.scan(prefix.clone()..).await?;

        while let Some(kv) = scan.next().await? {
            // Check if key still has our prefix
            if !kv.key.starts_with(&prefix) {
                break;
            }

            let key: OffsetCommitKey = postcard::from_bytes(&kv.key)?;
            let value: OffsetCommitValue = postcard::from_bytes(&kv.value)?;

            if value.expires_at.is_some_and(|expires_at| now >= expires_at) {
                continue;
            }

            _ = topitions.insert(Topition::new(key.topic, key.partition), value.offset);
        }

        Ok(topitions)
    }

    pub(super) async fn impl_offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        let topics = self.get_topics().await?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let prefix =
            postcard::to_stdvec(&LeaderEpochKeyPrefix::new(metadata.id, topition.partition))?;

        let mut scan = self.db.scan(prefix.clone()..).await?;
        while let Some(kv) = scan.next().await? {
            if !kv.key.starts_with(&prefix) {
                break;
            }

            let key: LeaderEpochKey = postcard::from_bytes(&kv.key)?;
            if key.epoch > leader_epoch {
                let value: LeaderEpochValue = postcard::from_bytes(&kv.value)?;
                return Ok(Some((key.epoch, value.start_offset)));
            }
        }

        Ok(None)
    }

    pub(super) async fn impl_leader_epoch_history(
        &self,
        topition: &Topition,
    ) -> Result<Vec<LeaderEpochRecord>> {
        let topics = self.get_topics().await?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let prefix =
            postcard::to_stdvec(&LeaderEpochKeyPrefix::new(metadata.id, topition.partition))?;

        let mut history = vec![];
        let mut scan = self.db.scan(prefix.clone()..).await?;
        while let Some(kv) = scan.next().await? {
            if !kv.key.starts_with(&prefix) {
                break;
            }

            let key: LeaderEpochKey = postcard::from_bytes(&kv.key)?;
            let value: LeaderEpochValue = postcard::from_bytes(&kv.value)?;
            history.push(LeaderEpochRecord {
                epoch: key.epoch,
                start_offset: value.start_offset,
            });
        }

        history.sort_unstable();
        Ok(history)
    }

    pub(super) async fn impl_offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        let _ = require_stable;

        let now = SystemTime::now();
        let mut responses = BTreeMap::new();

        if let Some(group_id) = group_id {
            let existing_topics = self.get_topics().await?;

            for topition in topics {
                if !existing_topics.contains_key(&topition.topic[..]) {
                    _ = responses.insert(
                        topition.clone(),
                        OffsetFetchRecord::default().with_offset(-1),
                    );
                    continue;
                }

                let key = postcard::to_stdvec(&OffsetCommitKey::new(
                    group_id,
                    &topition.topic,
                    topition.partition,
                ))?;

                let record = match self.db.get(&key).await {
                    Ok(Some(encoded)) => {
                        let value: OffsetCommitValue = postcard::from_bytes(&encoded)?;
                        let record = OffsetFetchRecord::from_parts(
                            value.offset,
                            value.leader_epoch,
                            value.metadata,
                            value.commit_timestamp,
                            value.expires_at,
                        );
                        if record.expired(now) {
                            OffsetFetchRecord::default().with_offset(-1)
                        } else {
                            record
                        }
                    }
                    Ok(None) => OffsetFetchRecord::default().with_offset(-1),
                    Err(err) => {
                        debug!(?err, ?group_id, ?topition);
                        return Err(Error::Slate(Arc::new(err)));
                    }
                };

                _ = responses.insert(topition.clone(), record);
            }
        }

        Ok(responses)
    }

    pub(super) async fn impl_offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.impl_offset_fetch_records(group_id, topics, require_stable)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(|(topition, record)| (topition, record.committed_offset()))
                    .collect()
            })
    }

    pub(super) async fn impl_list_offsets(
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
                        let offset_stage = self.impl_offset_stage(topition).await?;
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
                        None => match watermark
                            .timestamps
                            .as_ref()
                            .and_then(|ts| ts.last_key_value().map(|(ts, off)| (*ts, *off)))
                        {
                            Some((ts, off)) => ListOffsetResponse {
                                error_code: ErrorCode::None,
                                offset: Some(off + 1),
                                timestamp: to_system_time(ts).ok(),
                            },
                            None => ListOffsetResponse {
                                error_code: ErrorCode::None,
                                offset: Some(0),
                                timestamp: None,
                            },
                        },
                    }
                }
            };

            responses.push((topition.clone(), response));
        }

        Ok(responses)
    }
}
