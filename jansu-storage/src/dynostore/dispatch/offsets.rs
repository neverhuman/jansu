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

//! List-offsets and consumer-offset commit dispatch.
//!
//! Inherent `DynoStore` methods backing the `Storage` trait implementation.

use super::*;

impl DynoStore {
    pub(crate) async fn list_offsets_dispatch(
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
                        .reduce(|mut acc, e| {
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
                        })
                        .unwrap_or(BTreeMap::new()))
                })
                .await?
        } else {
            BTreeMap::new()
        };

        let mut responses = vec![];

        for (topition, offset_request) in offsets {
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

                        ListOffset::Timestamp(system_time)
                            if SystemTime::from(meta.last_modified) > *system_time
                                && candidate.as_ref().is_none_or(|found| {
                                    found.last_modified > meta.last_modified
                                }) =>
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

    pub(crate) async fn offset_commit_dispatch(
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

    pub(crate) async fn committed_offset_topitions_dispatch(
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

        self.offset_fetch_records(Some(group_id), topitions.as_ref(), Some(false))
            .await
            .map(|offsets| {
                offsets
                    .into_iter()
                    .map(|(topition, record)| (topition, record.committed_offset()))
                    .collect()
            })
    }
}
