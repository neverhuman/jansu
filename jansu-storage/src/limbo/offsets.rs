use super::sql::sql_lookup;
use super::*;

pub(super) fn integer_or_default(value: Value, default: i64) -> i64 {
    match value.as_integer().copied() {
        Some(integer) => integer,
        None => default,
    }
}

pub(super) fn value_to_optional_system_time(value: Value) -> Result<Option<SystemTime>> {
    match value {
        Value::Null => Ok(None),
        other => LiteTimestamp::try_from(other).map(|timestamp| Some(timestamp.0)),
    }
}

impl Engine {
    pub(super) async fn offset_stage_impl(&self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);
        let c = self.connection().await?;

        let row = self
            .prepare_query_one(
                &c,
                &sql_lookup("watermark_select.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?topition, ?err))?;

        let log_start = match row
            .get_value(0)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
        {
            Some(log_start) => log_start,
            None => 0,
        };

        let high_watermark = match row
            .get_value(1)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
        {
            Some(high_watermark) => high_watermark,
            None => 0,
        };

        let last_stable = match row
            .get_value(1)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
        {
            Some(last_stable) => last_stable,
            None => high_watermark,
        };

        debug!(cluster = self.cluster, ?topition, log_start, high_watermark,);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
    }

    pub(super) async fn offset_commit_impl(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        debug!(cluster = self.cluster, ?group, ?retention, ?offsets);
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            let mut rows = tx
                .query(
                    &sql_lookup("topition_select.sql")?,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                    ),
                )
                .await
                .inspect_err(|err| error!(?err))?;

            if rows.next().await.inspect_err(|err| error!(?err))?.is_some() {
                if !cg_inserted {
                    let rows = self
                        .prepare_execute(
                            &tx,
                            &sql_lookup("consumer_group_insert.sql")?,
                            (self.cluster.as_str(), group),
                        )
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = self
                    .prepare_execute(
                        &tx,
                        &sql_lookup("consumer_offset_insert.sql")?,
                        (
                            self.cluster.as_str(),
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

    pub(super) async fn committed_offset_topitions_impl(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        debug!(group_id);

        let mut results = BTreeMap::new();
        let now = SystemTime::now();

        let c = self.connection().await?;

        let mut rows = c
            .query(
                &sql_lookup("consumer_offset_select_by_group.sql")?,
                (self.cluster.as_str(), group_id),
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
                other => Some(LiteTimestamp::try_from(other)?.0),
            };
            let metadata = row.get::<Option<String>>(5)?;
            let expires_at = match row.get_value(6).map_err(Error::from)? {
                Value::Null => None,
                other => Some(LiteTimestamp::try_from(other)?.0),
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

    pub(super) async fn offset_for_leader_epoch_impl(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        debug!(cluster = self.cluster, ?topition, leader_epoch);

        let c = self.connection().await?;

        let mut rows = c
            .query(
                &sql_lookup("offset_for_leader_epoch.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    leader_epoch,
                ),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            let next_epoch = integer_or_default(row.get_value(0)?, 0) as i32;
            let end_offset = integer_or_default(row.get_value(1)?, 0);
            Ok(Some((next_epoch, end_offset)))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn leader_epoch_history_impl(
        &self,
        topition: &Topition,
    ) -> Result<Vec<LeaderEpochRecord>> {
        debug!(cluster = self.cluster, ?topition);

        let c = self.connection().await?;

        let mut topition_rows = c
            .query(
                &sql_lookup("topition_select_id.sql")?,
                (
                    self.cluster.as_str(),
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
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        let mut history = vec![];

        while let Some(row) = rows.next().await? {
            history.push(LeaderEpochRecord {
                epoch: match row.get::<Option<i32>>(0)? {
                    Some(epoch) => epoch,
                    None => 0,
                },
                start_offset: match row.get::<Option<i64>>(1)? {
                    Some(start_offset) => start_offset,
                    None => 0,
                },
            });
        }

        debug!(cluster = self.cluster, ?topition, ?history);
        Ok(history)
    }
}
