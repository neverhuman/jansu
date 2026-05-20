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

//! Offset-fetch and leader-epoch dispatch.
//!
//! Inherent `DynoStore` methods backing the `Storage` trait implementation.

use super::*;

impl DynoStore {
    pub(crate) async fn offset_fetch_records_dispatch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        if require_stable == Some(true) {
            warn!("require_stable requested; returning offsets with current DynoStore visibility");
        }

        let mut responses = BTreeMap::new();
        let now = SystemTime::now();

        if let Some(group_id) = group_id {
            for topition in topics {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
                    self.cluster, group_id, topition.topic, topition.partition,
                ));

                let record = match self.object_store.get(&location).await {
                    Ok(get_result) => get_result
                        .bytes()
                        .await
                        .map_err(Error::from)
                        .and_then(|encoded| {
                            serde_json::from_slice::<OffsetFetchRecord>(&encoded[..])
                                .map_err(Error::from)
                        })
                        .map(|record| {
                            if record.expired(now) {
                                OffsetFetchRecord::default().with_offset(-1)
                            } else {
                                record
                            }
                        })
                        .inspect_err(|error| error!(?error, ?group_id, ?topition))
                        .map_err(|_| Error::Api(ErrorCode::UnknownServerError)),

                    Err(object_store::Error::NotFound { .. }) => {
                        Ok(OffsetFetchRecord::default().with_offset(-1))
                    }

                    Err(error) => {
                        error!(?error, ?group_id, ?topition);
                        Err(Error::Api(ErrorCode::UnknownServerError))
                    }
                }?;

                _ = responses.insert(topition.to_owned(), record);
            }
        }

        Ok(responses)
    }

    pub(crate) async fn offset_for_leader_epoch_dispatch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        let key = format!("{}:{}", topition.topic(), topition.partition());

        self.meta
            .with(&self.object_store, |meta| {
                if let Some(epochs) = meta.leader_epoch_history.get(&key) {
                    // Find the first epoch strictly greater than the requested one.
                    // This matches the Kafka semantics: return (next_epoch, start_offset_of_next_epoch).
                    for &(epoch, start_offset) in epochs {
                        if epoch > leader_epoch {
                            return Ok(Some((epoch, start_offset)));
                        }
                    }
                }
                Ok(None)
            })
            .await
    }

    pub(crate) async fn leader_epoch_history_dispatch(
        &self,
        topition: &Topition,
    ) -> Result<Vec<LeaderEpochRecord>> {
        let key = format!("{}:{}", topition.topic(), topition.partition());

        self.meta
            .with(&self.object_store, |meta| {
                let Some(topic) = meta.topics.get(topition.topic()) else {
                    return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
                };

                if topition.partition() < 0 || topition.partition() >= topic.topic.num_partitions {
                    return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
                }

                let mut history = match meta.leader_epoch_history.get(&key) {
                    Some(epochs) => epochs
                        .iter()
                        .map(|(epoch, start_offset)| LeaderEpochRecord {
                            epoch: *epoch,
                            start_offset: *start_offset,
                        })
                        .collect::<Vec<_>>(),
                    None => Vec::new(),
                };

                history.sort_unstable();
                Ok(history)
            })
            .await
    }

    pub(crate) async fn offset_fetch_dispatch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.offset_fetch_records(group_id, topics, _require_stable)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(|(topition, record)| (topition, record.committed_offset()))
                    .collect()
            })
    }
}
