//! `Storage` consumer-offset and leader-epoch operations for the libSQL `Delegate`.

use super::*;

impl Delegate {
    pub(super) async fn offset_commit_inner(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?group, ?retention, ?offsets);

        let c = self.connection().await?;
        let tx = c.transaction().await?;
        let now = SystemTime::now();
        let expires_at = now.checked_add(retention.unwrap_or(DEFAULT_OFFSET_RETENTION));

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            let mut rows = c
                .query(
                    "topition_select.sql",
                    (
                        self.cluster.as_str(),
                        self.base_topic(topition.topic()).await?,
                        topition.partition(),
                    ),
                )
                .await
                .inspect_err(|err| error!(?err))?;

            if rows.next().await.inspect_err(|err| error!(?err))?.is_some() {
                if !cg_inserted {
                    let rows = c
                        .execute("consumer_group_insert.sql", (self.cluster.as_str(), group))
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = c
                    .execute(
                        "consumer_offset_insert.sql",
                        (
                            self.cluster.as_str(),
                            self.base_topic(topition.topic()).await?,
                            topition.partition(),
                            group,
                            offset.offset,
                            offset.leader_epoch,
                            offset.timestamp.or(Some(now)).map(LiteTimestamp::from),
                            offset.metadata.as_deref(),
                            expires_at.map(LiteTimestamp::from),
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

        c.commit(tx).await.inspect_err(|err| error!(?err))?;

        Ok(responses).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_commit")],
            )
        })
    }

    pub(super) async fn committed_offset_topitions_inner(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();

        debug!(group_id);

        let mut results = BTreeMap::new();
        let now = SystemTime::now();

        let c = self.connection().await?;

        let mut rows = c
            .query(
                "consumer_offset_select_by_group.sql",
                (self.cluster.as_str(), group_id),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let topic = row.get_str(0)?;
            let partition = row.get::<i32>(1)?;
            let offset = row.get::<i64>(2)?;
            let leader_epoch = row.get::<Option<i32>>(3)?;
            let commit_timestamp = value_to_system_time(row.get_value(4).map_err(Error::from)?)?;
            let metadata = row.get::<Option<String>>(5)?;
            let expires_at = value_to_system_time(row.get_value(6).map_err(Error::from)?)?;

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

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "committed_offset_topitions")],
            )
        })
    }

    pub(super) async fn offset_fetch_records_inner(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?group_id, ?topics, ?require_stable);

        let c = self.connection().await?;

        let mut offsets = BTreeMap::new();

        for topic in topics {
            let mut rows = c
                .query(
                    "consumer_offset_select.sql",
                    (
                        self.cluster.as_str(),
                        group_id,
                        self.base_topic(topic.topic()).await?,
                        topic.partition(),
                    ),
                )
                .await
                .inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition
                    )
                })?;

            let record = match rows.next().await.map_err(Error::from)? {
                Some(row) => {
                    let offset = row.get::<i64>(0)?;
                    let leader_epoch = row.get::<Option<i32>>(1)?;
                    let commit_timestamp =
                        value_to_system_time(row.get_value(2).map_err(Error::from)?)?;
                    let metadata = row.get::<Option<String>>(3)?;
                    let expires_at = value_to_system_time(row.get_value(4).map_err(Error::from)?)?;

                    let record = OffsetFetchRecord::from_parts(
                        offset,
                        leader_epoch,
                        metadata,
                        commit_timestamp,
                        expires_at,
                    );

                    if record.expired(start) {
                        OffsetFetchRecord::default().with_offset(-1)
                    } else {
                        record
                    }
                }
                None => OffsetFetchRecord::default().with_offset(-1),
            };

            debug!(
                cluster = self.cluster,
                group_id,
                topic = topic.topic,
                partition = topic.partition,
                offset = record.committed_offset()
            );

            assert_eq!(None, offsets.insert(topic.to_owned(), record));
        }

        Ok(offsets).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_fetch_records")],
            )
        })
    }

    pub(super) async fn offset_for_leader_epoch_inner(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        debug!(cluster = self.cluster, ?topition, leader_epoch);

        let c = self.connection().await?;

        let mut rows = c
            .query(
                "offset_for_leader_epoch.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    leader_epoch,
                ),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            let next_epoch = row.get_value(0)?.as_integer().copied().unwrap_or(0) as i32;
            let end_offset = row.get_value(1)?.as_integer().copied().unwrap_or(0);
            Ok(Some((next_epoch, end_offset)))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn leader_epoch_history_inner(
        &self,
        topition: &Topition,
    ) -> Result<Vec<LeaderEpochRecord>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topition);

        let c = self.connection().await?;

        let mut topition_rows = c
            .query(
                "topition_select_id.sql",
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
                "leader_epoch_history.sql",
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
                epoch: row.get::<i32>(0).inspect_err(|err| error!(?err))?,
                start_offset: row.get::<i64>(1).inspect_err(|err| error!(?err))?,
            });
        }

        debug!(cluster = self.cluster, ?topition, ?history);
        SQL_DURATION.record(
            elapsed_millis(start),
            &[KeyValue::new("operation", "leader_epoch_history")],
        );
        SQL_REQUESTS.add(
            1,
            &[
                KeyValue::new("operation", "leader_epoch_history"),
                KeyValue::new("cluster_id", self.cluster.clone()),
            ],
        );

        Ok(history)
    }

    pub(super) async fn offset_fetch_inner(
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
}
