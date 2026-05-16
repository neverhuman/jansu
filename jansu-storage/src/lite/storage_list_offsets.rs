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

fn row_to_list_offset_response(row: libsql::Row) -> Result<ListOffsetResponse> {
    debug!(?row);
    row.get::<i64>(0)
        .map_err(Into::into)
        .map(Some)
        .and_then(|offset| {
            row.get_value(1)
                .map_err(Into::into)
                .and_then(LiteTimestamp::try_from)
                .map(SystemTime::from)
                .map(Some)
                .map(|timestamp| ListOffsetResponse {
                    timestamp,
                    offset,
                    ..Default::default()
                })
        })
}

impl Delegate {
    pub(super) async fn delegate_list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?isolation_level, ?offsets);

        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            if c.query_opt(
                "topition_select.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
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

            let list_offset = match offset_type {
                ListOffset::Timestamp(timestamp) => {
                    let row = c
                        .query_opt(
                            "list_latest_offset_timestamp.sql",
                            (
                                self.cluster.as_str(),
                                topition.topic(),
                                topition.partition(),
                                LiteTimestamp::from(timestamp),
                            ),
                        )
                        .await
                        .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition))?;

                    match row {
                        Some(row) => row_to_list_offset_response(row)?,
                        None => {
                            // Timestamp is after all records: return high watermark + last timestamp.
                            let hwm_row = c
                                .query_opt(
                                    "list_latest_offset_uncommitted.sql",
                                    (
                                        self.cluster.as_str(),
                                        topition.topic(),
                                        topition.partition(),
                                    ),
                                )
                                .await
                                .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition))?;

                            match hwm_row {
                                Some(row) => row_to_list_offset_response(row)?,
                                None => ListOffsetResponse {
                                    offset: Some(0),
                                    timestamp: None,
                                    ..Default::default()
                                },
                            }
                        }
                    }
                }

                ListOffset::Earliest | ListOffset::Latest => {
                    let query = match (offset_type, isolation_level) {
                        (ListOffset::Earliest, _) => "list_earliest_offset.sql",
                        (ListOffset::Latest, IsolationLevel::ReadCommitted) => {
                            "list_latest_offset_committed.sql"
                        }
                        _ => "list_latest_offset_uncommitted.sql",
                    };

                    let row = c
                        .query_opt(
                            query,
                            (
                                self.cluster.as_str(),
                                topition.topic(),
                                topition.partition(),
                            ),
                        )
                        .await
                        .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition))?;

                    match row {
                        Some(row) => row_to_list_offset_response(row)?,
                        None => ListOffsetResponse {
                            offset: Some(0),
                            timestamp: None,
                            ..Default::default()
                        },
                    }
                }
            };

            debug!(cluster = self.cluster, ?topition, ?offset_type, ?list_offset);
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
