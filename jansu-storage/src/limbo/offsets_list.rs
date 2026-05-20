//! List-offsets `Storage` operation for the Turso `Engine`.

use super::sql::sql_lookup;
use super::*;

impl Engine {
    pub(super) async fn list_offsets_impl(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(cluster = self.cluster, ?isolation_level, ?offsets);
        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            if self
                .prepare_query_opt(
                    &c,
                    sql_lookup("topition_select.sql")?.as_str(),
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
                ListOffsetRequest::Earliest | ListOffsetRequest::Latest => self
                    .prepare_query_opt(
                        &c,
                        query.as_str(),
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffsetRequest::Timestamp(timestamp) => self
                    .prepare_query_opt(
                        &c,
                        query.as_str(),
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            LiteTimestamp::from(timestamp),
                        ),
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
