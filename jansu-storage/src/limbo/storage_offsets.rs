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

use super::*;

pub(super) async fn offset_commit(
    this: &Engine,
    group: &str,
    retention: Option<Duration>,
    offsets: &[(Topition, OffsetCommitRequest)],
) -> Result<Vec<(Topition, ErrorCode)>> {
    debug!(cluster = this.cluster, ?group, ?retention, ?offsets);
    let mut c = this.connection().await?;
    let tx = c.transaction().await?;

    let mut cg_inserted = false;

    let mut responses = vec![];

    for (topition, offset) in offsets {
        debug!(?topition, ?offset);

        let mut rows = tx
            .query(
                &sql_lookup("topition_select.sql")?,
                (
                    this.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        if rows.next().await.inspect_err(|err| error!(?err))?.is_some() {
            if !cg_inserted {
                let rows = this
                    .prepare_execute(
                        &tx,
                        &sql_lookup("consumer_group_insert.sql")?,
                        (this.cluster.as_str(), group),
                    )
                    .await?;
                debug!(rows);

                cg_inserted = true;
            }

            let rows = this
                .prepare_execute(
                    &tx,
                    &sql_lookup("consumer_offset_insert.sql")?,
                    (
                        this.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        group,
                        offset.offset,
                        offset.leader_epoch,
                        offset.timestamp.map(LiteTimestamp),
                        offset.metadata.as_deref(),
                    ),
                )
                .await
                .inspect_err(|err| error!(?err))?;

            debug!(?rows);

            responses.push((
                topition.to_owned(),
                if rows == 0 {
                    ErrorCode::UnknownTopicOrPartition
                } else {
                    ErrorCode::None
                },
            ));
        } else {
            responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition))
        }
    }

    tx.commit().await.inspect_err(|err| error!(?err))?;

    Ok(responses)
}

pub(super) async fn committed_offset_topitions(
    this: &Engine,
    group_id: &str,
) -> Result<BTreeMap<Topition, i64>> {
    debug!(group_id);

    let mut results = BTreeMap::new();
    let now = SystemTime::now();

    let c = this.connection().await?;

    let mut rows = c
        .query(
            &sql_lookup("consumer_offset_select_by_group.sql")?,
            (this.cluster.as_str(), group_id),
        )
        .await?;

    while let Some(row) = rows.next().await? {
        let topic = row.get_value(0).map_err(Into::into).and_then(|value| {
            value
                .as_text()
                .cloned()
                .ok_or(Error::UnexpectedValue(value))
        })?;

        let partition = row.get_value(1).map_err(Into::into).and_then(|value| {
            value
                .as_integer()
                .copied()
                .map(|partition| partition as i32)
                .ok_or(Error::UnexpectedValue(value))
        })?;

        let offset = row.get_value(2).map_err(Into::into).and_then(|value| {
            value
                .as_integer()
                .copied()
                .ok_or(Error::UnexpectedValue(value))
        })?;
        let leader_epoch = row.get::<Option<i32>>(3)?;
        let commit_timestamp = match row.get_value(4).map_err(Error::from)? {
            Value::Null => None,
            other => Some(LiteTimestamp::try_from(other)?.0.into()),
        };
        let metadata = row.get::<Option<String>>(5)?;
        let expires_at = match row.get_value(6).map_err(Error::from)? {
            Value::Null => None,
            other => Some(LiteTimestamp::try_from(other)?.0.into()),
        };

        let record = OffsetFetchRecord::from_parts(
            offset,
            leader_epoch,
            metadata,
            commit_timestamp,
            expires_at,
        );

        if record.expired(now) {
            continue;
        }

        debug!(group_id, topic, partition, offset);

        assert_eq!(
            None,
            results.insert(Topition::new(topic, partition), record.committed_offset())
        );
    }

    Ok(results)
}

pub(super) async fn offset_for_leader_epoch(
    this: &Engine,
    topition: &Topition,
    leader_epoch: i32,
) -> Result<Option<(i32, i64)>> {
    debug!(cluster = this.cluster, ?topition, leader_epoch);

    let c = this.connection().await?;

    let mut rows = c
        .query(
            &sql_lookup("offset_for_leader_epoch.sql")?,
            (
                this.cluster.as_str(),
                topition.topic(),
                topition.partition(),
                leader_epoch,
            ),
        )
        .await?;

    if let Some(row) = rows.next().await? {
        let next_epoch = row.get_value(0)?.as_integer().copied().unwrap_or_default() as i32;
        let end_offset = row.get_value(1)?.as_integer().copied().unwrap_or_default() as i64;
        Ok(Some((next_epoch, end_offset)))
    } else {
        Ok(None)
    }
}

pub(super) async fn leader_epoch_history(
    this: &Engine,
    topition: &Topition,
) -> Result<Vec<LeaderEpochRecord>> {
    debug!(cluster = this.cluster, ?topition);

    let c = this.connection().await?;

    let mut topition_rows = c
        .query(
            &sql_lookup("topition_select_id.sql")?,
            (
                this.cluster.as_str(),
                topition.topic(),
                topition.partition(),
            ),
        )
        .await?;

    if topition_rows.next().await?.is_none() {
        return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
    }
    drop(topition_rows);

    let mut rows = c
        .query(
            &sql_lookup("leader_epoch_history.sql")?,
            (
                this.cluster.as_str(),
                topition.topic(),
                topition.partition(),
            ),
        )
        .await?;

    let mut history = vec![];

    while let Some(row) = rows.next().await? {
        history.push(LeaderEpochRecord {
            epoch: row.get::<Option<i32>>(0)?.unwrap_or_default(),
            start_offset: row.get::<Option<i64>>(1)?.unwrap_or_default(),
        });
    }

    debug!(cluster = this.cluster, ?topition, ?history);
    Ok(history)
}

pub(super) async fn offset_fetch(
    this: &Engine,
    group_id: Option<&str>,
    topics: &[Topition],
    require_stable: Option<bool>,
) -> Result<BTreeMap<Topition, i64>> {
    offset_fetch_records(this, group_id, topics, require_stable)
        .await
        .map(|offsets| {
            offsets
                .into_iter()
                .map(|(topition, record)| (topition, record.committed_offset()))
                .collect()
        })
}

pub(super) async fn offset_fetch_records(
    this: &Engine,
    group_id: Option<&str>,
    topics: &[Topition],
    require_stable: Option<bool>,
) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
    debug!(cluster = this.cluster, ?group_id, ?topics, ?require_stable);
    let c = this.connection().await?;
    let now = SystemTime::now();

    let mut offsets = BTreeMap::new();

    for topic in topics {
        let mut rows = c
            .query(
                &sql_lookup("consumer_offset_select.sql")?,
                (
                    this.cluster.as_str(),
                    group_id,
                    topic.topic(),
                    topic.partition(),
                ),
            )
            .await
            .inspect_err(|err| {
                error!(
                    ?err,
                    cluster = this.cluster,
                    group_id,
                    topic = topic.topic,
                    partition = topic.partition
                )
            })?;

        let record = match rows.next().await.map_err(Error::from)? {
            Some(row) => {
                let offset = row
                    .get_value(0)
                    .map_err(Error::from)?
                    .as_integer()
                    .copied()
                    .unwrap_or(-1);
                let leader_epoch = row.get::<Option<i32>>(1)?;
                let commit_timestamp = match row.get_value(2).map_err(Error::from)? {
                    Value::Null => None,
                    value => Some(LiteTimestamp::try_from(value)?.0.into()),
                };
                let metadata = row.get::<Option<String>>(3)?;
                let expires_at = match row.get_value(4).map_err(Error::from)? {
                    Value::Null => None,
                    value => Some(LiteTimestamp::try_from(value)?.0.into()),
                };

                let record = OffsetFetchRecord::from_parts(
                    offset,
                    leader_epoch,
                    metadata,
                    commit_timestamp,
                    expires_at,
                );

                if record.expired(now) {
                    OffsetFetchRecord::default().with_offset(-1)
                } else {
                    record
                }
            }
            None => OffsetFetchRecord::default().with_offset(-1),
        };

        debug!(
            cluster = this.cluster,
            group_id,
            topic = topic.topic,
            partition = topic.partition,
            offset = record.committed_offset()
        );

        assert_eq!(None, offsets.insert(topic.to_owned(), record));
    }

    Ok(offsets)
}

pub(super) async fn list_offsets(
    this: &Engine,
    isolation_level: IsolationLevel,
    offsets: &[(Topition, ListOffsetRequest)],
) -> Result<Vec<(Topition, ListOffsetResponse)>> {
    debug!(cluster = this.cluster, ?isolation_level, ?offsets);
    let c = this.connection().await?;

    let mut responses = vec![];

    for (topition, offset_type) in offsets {
        if this
            .prepare_query_opt(
                &c,
                sql_lookup("topition_select.sql")?.as_str(),
                (
                    this.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?err, cluster = this.cluster, ?topition))?
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
            (ListOffsetRequest::Earliest, _) => sql_lookup("list_earliest_offset.sql")?,
            (ListOffsetRequest::Latest, IsolationLevel::ReadCommitted) => {
                sql_lookup("list_latest_offset_committed.sql")?
            }
            (ListOffsetRequest::Latest, IsolationLevel::ReadUncommitted) => {
                sql_lookup("list_latest_offset_uncommitted.sql")?
            }
            (ListOffsetRequest::Timestamp(_), _) => {
                sql_lookup("list_latest_offset_timestamp.sql")?
            }
        };

        debug!(?query);

        let list_offset = match offset_type {
            ListOffsetRequest::Earliest | ListOffsetRequest::Latest => this
                .prepare_query_opt(
                    &c,
                    query.as_str(),
                    (
                        this.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                    ),
                )
                .await
                .inspect_err(|err| error!(?err, cluster = this.cluster, ?topition)),

            ListOffsetRequest::Timestamp(timestamp) => this
                .prepare_query_opt(
                    &c,
                    query.as_str(),
                    (
                        this.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        LiteTimestamp::from(timestamp),
                    ),
                )
                .await
                .inspect_err(|err| error!(?err)),
        }
        .inspect_err(|err| {
            error!(?err, cluster = this.cluster, ?topition);
        })
        .inspect(|result| debug!(?result))?
        .map_or_else(
            || {
                let timestamp = None;
                let offset = Some(0);
                debug!(
                    cluster = this.cluster,
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

                row.get_value(0)
                    .map_err(Into::into)
                    .map(|value| value.as_integer().copied())
                    .and_then(|offset| {
                        row.get_value(1)
                            .map_err(Into::into)
                            .and_then(LiteTimestamp::try_from)
                            .map(SystemTime::from)
                            .map(Some)
                            .map(|timestamp| {
                                debug!(
                                    cluster = this.cluster,
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
