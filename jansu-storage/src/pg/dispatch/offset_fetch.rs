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

//! Offset-fetch and list-offsets dispatch.
//!
//! Inherent `Postgres` methods backing the `Storage` trait implementation.

use super::*;

impl Postgres {
    pub(crate) async fn offset_fetch_records_dispatch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        debug!(cluster = self.cluster, ?group_id, ?topics, ?require_stable);

        let c = self.connection().await?;
        let now = SystemTime::now();

        let mut offsets = BTreeMap::new();

        for topic in topics {
            let record = self
                .prepare_query_opt(
                    &c,
                    "consumer_offset_select.sql",
                    &[
                        &self.cluster,
                        &group_id,
                        &self.base_topic(topic.topic()).await?,
                        &topic.partition(),
                    ],
                )
                .await
                .and_then(|maybe| {
                    maybe.map_or(Ok(OffsetFetchRecord::default().with_offset(-1)), |row| {
                        let offset = row.try_get::<_, i64>(0).map_err(Error::from)?;
                        let leader_epoch = row.try_get::<_, Option<i32>>(1).map_err(Error::from)?;
                        let commit_timestamp = row
                            .try_get::<_, Option<SystemTime>>(2)
                            .map_err(Error::from)?;
                        let metadata = row.try_get::<_, Option<String>>(3).map_err(Error::from)?;
                        let expires_at = row
                            .try_get::<_, Option<SystemTime>>(4)
                            .map_err(Error::from)?;

                        let record = OffsetFetchRecord::from_parts(
                            offset,
                            leader_epoch,
                            metadata,
                            commit_timestamp,
                            expires_at,
                        );

                        Ok(if record.expired(now) {
                            OffsetFetchRecord::default().with_offset(-1)
                        } else {
                            record
                        })
                    })
                })
                .inspect(|record| {
                    debug!(
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition,
                        offset = record.committed_offset()
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition
                    )
                })?;

            assert_eq!(None, offsets.insert(topic.to_owned(), record));
        }

        Ok(offsets)
    }

    pub(crate) async fn offset_fetch_dispatch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.offset_fetch_records(group_id, topics, require_stable)
            .await
            .map(|offsets| {
                offsets
                    .into_iter()
                    .map(|(topition, record)| (topition, record.committed_offset()))
                    .collect()
            })
    }

    pub(crate) async fn list_offsets_dispatch(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(cluster = self.cluster, ?isolation_level, ?offsets);

        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            if self
                .prepare_query_opt(
                    &c,
                    "topition_select.sql",
                    &[&self.cluster, &topition.topic(), &topition.partition()],
                )
                .await
                .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition))?
                .is_none()
            {
                responses.push((
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        timestamp: None,
                        offset: None,
                    },
                ));
                continue;
            }

            let query = match (offset_type, isolation_level) {
                (ListOffset::Earliest, _) => "list_earliest_offset.sql",
                (ListOffset::Latest, IsolationLevel::ReadCommitted) => {
                    "list_latest_offset_committed.sql"
                }
                (ListOffset::Latest, IsolationLevel::ReadUncommitted) => {
                    "list_latest_offset_uncommitted.sql"
                }
                (ListOffset::Timestamp(_), _) => "list_latest_offset_timestamp.sql",
            };

            debug!(?query);

            let list_offset = match offset_type {
                ListOffset::Earliest | ListOffset::Latest => self
                    .prepare_query_opt(
                        &c,
                        query,
                        &[&self.cluster, &topition.topic(), &topition.partition()],
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffset::Timestamp(timestamp) => self
                    .prepare_query_opt(
                        &c,
                        query,
                        &[
                            &self.cluster.as_str(),
                            &topition.topic(),
                            &topition.partition(),
                            timestamp,
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err)),
            }
            .inspect_err(|err| {
                error!(?err, cluster = self.cluster, ?topition);
            })
            .inspect(|result| debug!(?result))?
            .map_or_else(
                || {
                    let timestamp = None;
                    let offset = Some(0);
                    debug!(
                        cluster = self.cluster,
                        ?topition,
                        ?offset_type,
                        offset,
                        ?timestamp
                    );

                    Ok(ListOffsetResponse {
                        timestamp,
                        offset,
                        ..Default::default()
                    })
                },
                |row| {
                    debug!(?row);

                    row.try_get::<_, i64>(0).map(Some).and_then(|offset| {
                        row.try_get::<_, SystemTime>(1).map(Some).map(|timestamp| {
                            debug!(
                                cluster = self.cluster,
                                ?topition,
                                ?offset_type,
                                offset,
                                ?timestamp
                            );

                            ListOffsetResponse {
                                timestamp,
                                offset,
                                ..Default::default()
                            }
                        })
                    })
                },
            )?;

            responses.push((topition.clone(), list_offset));
        }

        Ok(responses).inspect(|r| debug!(?r))
    }
}
