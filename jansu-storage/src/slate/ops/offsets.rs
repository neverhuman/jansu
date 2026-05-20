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

//! Consumer-group offset and leader-epoch operations for the SlateDB engine.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use jansu_sans_io::ErrorCode;
use tracing::debug;

use crate::{
    DEFAULT_OFFSET_RETENTION, Error, LeaderEpochRecord, OffsetCommitRequest, OffsetFetchRecord,
    Result, Topition,
};

use super::super::engine::Engine;
use super::super::types::{
    GroupDetailVersion, GroupKey, LeaderEpochKey, LeaderEpochKeyPrefix, LeaderEpochValue,
    OffsetCommitKey, OffsetCommitKeyPrefix, OffsetCommitValue,
};

impl Engine {
    pub(in crate::slate) async fn offset_commit_op(
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
                    let group_value = postcard::to_stdvec(&GroupDetailVersion::default())?;
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

    pub(in crate::slate) async fn committed_offset_topitions_op(
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

    pub(in crate::slate) async fn offset_for_leader_epoch_op(
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

    pub(in crate::slate) async fn leader_epoch_history_op(
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

    pub(in crate::slate) async fn offset_fetch_records_op(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        if require_stable == Some(true) {
            tracing::warn!(
                "require_stable requested; returning offsets with current SlateDB visibility"
            );
        }

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

    pub(in crate::slate) async fn offset_fetch_op(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.offset_fetch_records_op(group_id, topics, require_stable)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(|(topition, record)| (topition, record.committed_offset()))
                    .collect()
            })
    }
}
