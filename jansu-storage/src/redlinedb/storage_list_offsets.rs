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

fn row_to_list_offset_response(row: &::redlinedb::Row<'_>) -> Result<ListOffsetResponse> {
    let offset = row.get::<i64>(0).map_err(Error::from).map(Some)?;
    let timestamp_val = row.get::<Value>(1).map_err(Error::from)?;
    let timestamp = SystemTime::try_from(&timestamp_val).map(Some).map_err(Error::from)?;
    Ok(ListOffsetResponse {
        timestamp,
        offset,
        ..Default::default()
    })
}

fn query_opt_row(
    c: &mut PoolConnection,
    key: &str,
    params: impl ::redlinedb::Params,
) -> Result<bool> {
    let s = sql(key).map_err(Error::from)?;
    let mut rows = c.query(&s, params).map_err(Error::from)?;
    Ok(matches!(rows.step().map_err(Error::from)?, Step::Row(_)))
}

impl Delegate {
    pub(super) async fn delegate_list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?isolation_level, ?offsets);

        let mut c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            if !query_opt_row(
                &mut c,
                "topition_select.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition))?
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

            let list_offset = match offset_type {
                ListOffset::Timestamp(timestamp) => {
                    let s = sql("list_latest_offset_timestamp.sql").map_err(Error::from)?;
                    let mut rows = c
                        .query(
                            &s,
                            (
                                self.cluster.as_str(),
                                topition.topic(),
                                topition.partition(),
                                Value::from(*timestamp),
                            ),
                        )
                        .map_err(Error::from)
                        .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition))?;

                    match rows.step().map_err(Error::from)? {
                        Step::Row(row) => row_to_list_offset_response(&row)?,
                        Step::Done => {
                            drop(rows);
                            let s2 = sql("list_latest_offset_uncommitted.sql").map_err(Error::from)?;
                            let mut rows2 = c
                                .query(
                                    &s2,
                                    (
                                        self.cluster.as_str(),
                                        topition.topic(),
                                        topition.partition(),
                                    ),
                                )
                                .map_err(Error::from)
                                .inspect_err(|err| {
                                    error!(?err, cluster = self.cluster, ?topition)
                                })?;

                            match rows2.step().map_err(Error::from)? {
                                Step::Row(row) => row_to_list_offset_response(&row)?,
                                Step::Done => ListOffsetResponse {
                                    offset: Some(0),
                                    timestamp: None,
                                    ..Default::default()
                                },
                            }
                        }
                    }
                }

                ListOffset::Earliest | ListOffset::Latest => {
                    if matches!(offset_type, ListOffset::Latest)
                        && isolation_level == IsolationLevel::ReadCommitted
                    {
                        let params = (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                        );
                        let s = sql("redlinedb/list_latest_offset_committed_active.sql")
                            .map_err(Error::from)?;
                        let mut rows = c
                            .query(&s, params)
                            .map_err(Error::from)
                            .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition))?;

                        if let Step::Row(row) = rows.step().map_err(Error::from)? {
                            let list_offset = row_to_list_offset_response(&row)?;
                            debug!(
                                cluster = self.cluster,
                                ?topition,
                                ?offset_type,
                                ?list_offset
                            );
                            responses.push((topition.clone(), list_offset));
                            continue;
                        }
                        drop(rows);

                        let s2 = sql("list_latest_offset_uncommitted.sql").map_err(Error::from)?;
                        let mut rows2 = c
                            .query(
                                &s2,
                                (
                                    self.cluster.as_str(),
                                    topition.topic(),
                                    topition.partition(),
                                ),
                            )
                            .map_err(Error::from)
                            .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition))?;

                        let list_offset = match rows2.step().map_err(Error::from)? {
                            Step::Row(row) => row_to_list_offset_response(&row)?,
                            Step::Done => ListOffsetResponse {
                                offset: Some(0),
                                timestamp: None,
                                ..Default::default()
                            },
                        };

                        debug!(
                            cluster = self.cluster,
                            ?topition,
                            ?offset_type,
                            ?list_offset
                        );
                        responses.push((topition.clone(), list_offset));
                        continue;
                    }

                    let query = match (offset_type, isolation_level) {
                        (ListOffset::Earliest, _) => "list_earliest_offset.sql",
                        (ListOffset::Latest, IsolationLevel::ReadCommitted) => {
                            "list_latest_offset_committed.sql"
                        }
                        _ => "list_latest_offset_uncommitted.sql",
                    };

                    let s = sql(query).map_err(Error::from)?;
                    let mut rows = c
                        .query(
                            &s,
                            (
                                self.cluster.as_str(),
                                topition.topic(),
                                topition.partition(),
                            ),
                        )
                        .map_err(Error::from)
                        .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition))?;

                    match rows.step().map_err(Error::from)? {
                        Step::Row(row) => row_to_list_offset_response(&row)?,
                        Step::Done => ListOffsetResponse {
                            offset: Some(0),
                            timestamp: None,
                            ..Default::default()
                        },
                    }
                }
            };

            debug!(
                cluster = self.cluster,
                ?topition,
                ?offset_type,
                ?list_offset
            );
            responses.push((topition.clone(), list_offset));
        }

        Ok(responses).inspect(|r| {
            debug!(?r);
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "list_offsets")],
            )
        })
    }
}
