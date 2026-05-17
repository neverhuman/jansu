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

//! Query/read operations for DynoStore.

use super::*;

impl DynoStore {
    pub(super) async fn list_offsets_inner(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let stable = if isolation_level == IsolationLevel::ReadCommitted {
            self.meta
                .with(&self.object_store, |meta| {
                    Ok(meta
                        .transactions
                        .values()
                        .flat_map(|txn| {
                            txn.epochs
                                .values()
                                .filter(|detail| {
                                    detail.state.is_some_and(|state| {
                                        state != TxnState::Committed && state != TxnState::Aborted
                                    })
                                })
                                .map(BTreeMap::<Topition, Offset>::from)
                                .collect::<Vec<_>>()
                        })
                        .fold(BTreeMap::new(), |mut acc, e| {
                            debug!(?acc, ?e);
                            for (topition, offset_start) in e.iter() {
                                _ = acc
                                    .entry(topition.to_owned())
                                    .and_modify(|existing_offset_start| {
                                        if *existing_offset_start > *offset_start {
                                            *existing_offset_start = *offset_start
                                        }
                                    })
                                    .or_insert(*offset_start);
                            }

                            acc
                        }))
                })
                .await?
        } else {
            BTreeMap::new()
        };

        let mut responses = vec![];

        for (topition, offset_request) in offsets {
            // For timestamp queries, use the watermark's timestamps index which
            // stores embedded Kafka batch timestamps (base_timestamp) rather than
            // object-store last_modified wall-clock times.
            if let ListOffset::Timestamp(target_ts) = offset_request {
                let watermark = self.watermarks.lock().map(|mut locked| {
                    locked
                        .entry(topition.to_owned())
                        .or_insert_with(|| {
                            OptiCon::<Watermark>::new(self.cluster.as_str(), topition)
                        })
                        .to_owned()
                })?;

                let response = watermark
                    .with(&self.object_store, |watermark| {
                        debug!(?watermark);

                        let target_millis = target_ts
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);

                        let result = watermark.timestamps.as_ref().and_then(|ts| {
                            ts.range(target_millis..)
                                .next()
                                .map(|(ts, off)| (*off, *ts))
                        });

                        Ok(match result {
                            Some((offset, ts)) => ListOffsetResponse {
                                error_code: ErrorCode::None,
                                offset: Some(offset),
                                timestamp: SystemTime::UNIX_EPOCH
                                    .checked_add(Duration::from_millis(ts as u64))
                                    .map(Some)
                                    .unwrap_or(None),
                            },
                            // Timestamp is after all records: return high_watermark with last timestamp.
                            None => match watermark
                                .timestamps
                                .as_ref()
                                .and_then(|ts| ts.last_key_value().map(|(ts, off)| (*ts, *off)))
                            {
                                Some((ts, off)) => ListOffsetResponse {
                                    error_code: ErrorCode::None,
                                    offset: Some(off + 1),
                                    timestamp: SystemTime::UNIX_EPOCH
                                        .checked_add(Duration::from_millis(ts as u64))
                                        .map(Some)
                                        .unwrap_or(None),
                                },
                                None => ListOffsetResponse {
                                    error_code: ErrorCode::None,
                                    offset: Some(0),
                                    timestamp: None,
                                },
                            },
                        })
                    })
                    .await?;

                responses.push((topition.to_owned(), response));
                continue;
            }

            let location = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records",
                self.cluster, topition.topic, topition.partition,
            ));

            let mut list_stream = self.object_store.list(Some(&location));

            let mut candidate: Option<ObjectMeta> = None;

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                if let Some(last) = stable.get(topition)
                    && offset_request == &ListOffset::Latest
                {
                    let Some(found_offset) = candidate
                        .as_ref()
                        .and_then(|found| found.location.parts().next_back())
                        .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    else {
                        continue;
                    };

                    let Some(meta_offset) = meta
                        .location
                        .parts()
                        .next_back()
                        .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    else {
                        continue;
                    };

                    if meta_offset >= *last && found_offset > meta_offset {
                        _ = candidate.replace(meta);
                    }
                } else {
                    match offset_request {
                        ListOffset::Earliest
                            if candidate
                                .as_ref()
                                .is_none_or(|found| found.last_modified > meta.last_modified) =>
                        {
                            _ = candidate.replace(meta);
                        }

                        ListOffset::Latest
                            if candidate
                                .as_ref()
                                .is_none_or(|found| meta.last_modified > found.last_modified) =>
                        {
                            _ = candidate.replace(meta);
                        }

                        _ => continue,
                    }
                }
            }

            debug!(?candidate);

            if let Some(ref found) = candidate {
                let Some(offset) = found.location.parts().next_back() else {
                    continue;
                };

                let offset = i64::from_str(&offset.as_ref()[0..20])?;
                debug!(offset);

                responses.push((
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(match offset_request {
                            ListOffset::Latest => offset + 1,
                            _ => offset,
                        }),
                        timestamp: Some(found.last_modified.into()),
                    },
                ))
            } else {
                responses.push((
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(0),
                        ..Default::default()
                    },
                ))
            }
        }

        Ok(responses)
    }

    pub(super) async fn offset_commit_inner(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let mut responses = vec![];
        let now = SystemTime::now();

        for (topition, offset_commit) in offsets {
            if self
                .topic_metadata(&TopicId::from(topition))
                .await?
                .is_some()
            {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
                    self.cluster, group_id, topition.topic, topition.partition,
                ));

                let payload = serde_json::to_vec(&OffsetFetchRecord::from_commit(
                    offset_commit,
                    retention_time_ms,
                    now,
                ))
                .map(Bytes::from)
                .map(PutPayload::from)?;

                let options = PutOptions {
                    mode: PutMode::Overwrite,
                    attributes: json_content_type(),
                    ..Default::default()
                };

                let error_code = self
                    .object_store
                    .put_opts(&location, payload, options)
                    .await
                    .inspect_err(|err| error!(?err))
                    .inspect(|outcome| debug!(?outcome))
                    .map_or(ErrorCode::UnknownServerError, |_| ErrorCode::None);

                responses.push((topition.to_owned(), error_code));
            } else {
                responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition));
            }
        }

        Ok(responses)
    }

    pub(super) async fn committed_offset_topitions_inner(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        let mut topitions = vec![];

        {
            let location = Path::from(format!(
                "clusters/{}/groups/consumers/{}/offsets/",
                self.cluster, group_id,
            ));

            let mut list_stream = self.object_store.list(Some(&location));

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                debug!(?meta);
                let Some(topic): Option<String> = meta
                    .location
                    .parts()
                    .nth(6)
                    .inspect(|topic| debug!(?topic))
                    .map(|topic| topic.as_ref().into())
                else {
                    continue;
                };

                let Some(partition) = meta
                    .location
                    .parts()
                    .nth(8)
                    .inspect(|partition| debug!(?partition))
                    .map(|partition| i32::from_str(&partition.as_ref()[0..10]))
                    .transpose()?
                else {
                    continue;
                };

                debug!(topic, partition);

                topitions.push(Topition::new(topic, partition));
            }
        }

        self.offset_fetch_records_inner(Some(group_id), topitions.as_ref(), Some(false))
            .await
            .map(|offsets| {
                offsets
                    .into_iter()
                    .map(|(topition, record)| (topition, record.committed_offset()))
                    .collect()
            })
    }

    pub(super) async fn offset_fetch_records_inner(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        let _ = require_stable;

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

    pub(super) async fn offset_for_leader_epoch_inner(
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

    pub(super) async fn leader_epoch_history_inner(
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

                let mut history: Vec<LeaderEpochRecord> = meta
                    .leader_epoch_history
                    .get(&key)
                    .into_iter()
                    .flat_map(|epochs| {
                        epochs
                            .iter()
                            .map(|(epoch, start_offset)| LeaderEpochRecord {
                                epoch: *epoch,
                                start_offset: *start_offset,
                            })
                    })
                    .collect();

                history.sort_unstable();
                Ok(history)
            })
            .await
    }

    pub(super) async fn offset_fetch_inner(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.offset_fetch_records_inner(group_id, topics, _require_stable)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(|(topition, record)| (topition, record.committed_offset()))
                    .collect()
            })
    }
}
