use super::*;

impl Engine {
    pub(super) async fn impl_fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        debug!(?topition, offset, min_bytes, max_bytes, ?isolation_level);
        let high_watermark = self.offset_stage(topition).await.map(|offset_stage| {
            if isolation_level == IsolationLevel::ReadCommitted {
                offset_stage.last_stable
            } else {
                offset_stage.high_watermark
            }
        })?;

        debug!(
            cluster = self.cluster,
            ?topition,
            offset,
            ?isolation_level,
            high_watermark,
            min_bytes,
            max_bytes
        );

        let c = self.connection().await?;

        let mut records = c
            .query(
                &sql_lookup("record_fetch.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    offset,
                    (max_bytes as i64),
                    high_watermark,
                ),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let mut batches = vec![];

        if let Some(row) = records.next().await? {
            let offset_delta = 0;
            let timestamp_delta = 0;

            let record_builder = {
                let mut record_builder = Record::builder()
                    .offset_delta(offset_delta)
                    .timestamp_delta(timestamp_delta)
                    .key(
                        row.get_value(3)
                            .map(|o| o.as_blob().map(|blob| Bytes::copy_from_slice(blob)))
                            .inspect(|k| debug!(?k))
                            .inspect_err(|err| error!(?err))?,
                    )
                    .value(
                        row.get_value(4)
                            .map(|o| o.as_blob().map(|blob| Bytes::copy_from_slice(blob)))
                            .inspect(|v| debug!(?v))
                            .inspect_err(|err| error!(?err))?,
                    );

                let mut headers = c
                    .query(
                        &sql_lookup("header_fetch.sql")?,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            offset,
                        ),
                    )
                    .await?;

                while let Some(header) = headers.next().await? {
                    let mut header_builder = Header::builder();

                    if let Some(k) = header
                        .get_value(0)
                        .map(|value| value.as_blob().cloned())
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.key(Bytes::from(k));
                    }

                    if let Some(v) = header
                        .get_value(1)
                        .map(|value| value.as_blob().cloned())
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.value(Bytes::from(v));
                    }

                    record_builder = record_builder.header(header_builder);
                }

                record_builder
            };

            let mut batch_builder = inflated::Batch::builder()
                .base_offset(
                    row.get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .copied()
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect(|base_offset| debug!(base_offset))
                        .inspect_err(|err| error!(?err))?,
                )
                .attributes(
                    row.get_value(1)
                        .map(|value| {
                            value
                                .as_integer()
                                .copied()
                                .map(|attributes| attributes as i32)
                        })
                        .map(|attributes| attributes.unwrap_or(0))
                        .inspect_err(|err| error!(?err))? as i16,
                )
                .base_timestamp(
                    row.get_value(2)
                        .map_err(Error::from)
                        .and_then(LiteTimestamp::try_from)
                        .and_then(|system_time| to_timestamp(&system_time.0).map_err(Into::into))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_id(
                    row.get_value(6)
                        .map(|value| value.as_integer().copied())
                        .map(|producer_id| producer_id.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_epoch(
                    row.get_value(7)
                        .map(|value| {
                            value
                                .as_integer()
                                .copied()
                                .map(|producer_epoch| producer_epoch as i32)
                        })
                        .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))? as i16,
                )
                .record(record_builder)
                .last_offset_delta(offset_delta);

            while let Some(row) = records.next().await? {
                let attributes = row
                    .get_value(1)
                    .map(|value| {
                        value
                            .as_integer()
                            .copied()
                            .map(|attributes| attributes as i16)
                    })
                    .map(|attributes| attributes.unwrap_or(0))
                    .inspect_err(|err| error!(?err))?;

                let producer_id = row
                    .get_value(6)
                    .map(|value| value.as_integer().copied())
                    .map(|producer_id| producer_id.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;

                let producer_epoch = row
                    .get_value(7)
                    .map(|value| {
                        value
                            .as_integer()
                            .copied()
                            .map(|producer_epoch| producer_epoch as i16)
                    })
                    .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;

                if batch_builder.attributes != attributes
                    || batch_builder.producer_id != producer_id
                    || batch_builder.producer_epoch != producer_epoch
                {
                    batches.push(batch_builder.build().and_then(TryInto::try_into)?);

                    batch_builder = inflated::Batch::builder()
                        .base_offset(
                            row.get_value(0)
                                .map_err(Into::into)
                                .and_then(|value| {
                                    value
                                        .as_integer()
                                        .copied()
                                        .ok_or(Error::UnexpectedValue(value))
                                })
                                .inspect(|base_offset| debug!(base_offset))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .base_timestamp(
                            row.get_value(2)
                                .map_err(Error::from)
                                .and_then(LiteTimestamp::try_from)
                                .and_then(|system_time| {
                                    to_timestamp(&system_time.0).map_err(Into::into)
                                })
                                .inspect_err(|err| error!(?err))?,
                        )
                        .attributes(attributes)
                        .producer_id(producer_id)
                        .producer_epoch(producer_epoch);
                }

                let offset = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .copied()
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect(|offset| debug!(offset))
                    .inspect_err(|err| error!(?err))?;

                let offset_delta = i32::try_from(offset - batch_builder.base_offset)?;

                let timestamp_delta = row
                    .get_value(2)
                    .map_err(Error::from)
                    .and_then(LiteTimestamp::try_from)
                    .and_then(|system_time| {
                        to_timestamp(&system_time.0)
                            .map(|timestamp| timestamp - batch_builder.base_timestamp)
                            .map_err(Into::into)
                    })
                    .inspect(|timestamp| debug!(?timestamp))
                    .inspect_err(|err| error!(?err))?;

                let record_builder = {
                    let mut record_builder = Record::builder()
                        .offset_delta(offset_delta)
                        .timestamp_delta(timestamp_delta)
                        .key(
                            row.get_value(3)
                                .map(|value| value.as_blob().cloned())
                                .map(|o| o.map(Bytes::from))
                                .inspect(|k| debug!(?k))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .value(
                            row.get_value(4)
                                .map(|value| value.as_blob().cloned())
                                .map(|o| o.map(Bytes::from))
                                .inspect(|v| debug!(?v))
                                .inspect_err(|err| error!(?err))?,
                        );

                    let mut headers = c
                        .query(
                            &sql_lookup("header_fetch.sql")?,
                            (
                                self.cluster.as_str(),
                                topition.topic(),
                                topition.partition(),
                                offset,
                            ),
                        )
                        .await?;

                    while let Some(header) = headers.next().await? {
                        let mut header_builder = Header::builder();

                        if let Some(k) = header
                            .get_value(0)
                            .map(|value| value.as_blob().cloned())
                            .map(|o| o.map(Bytes::from))
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.key(k);
                        }

                        if let Some(v) = header
                            .get_value(1)
                            .map(|value| value.as_blob().cloned())
                            .map(|o| o.map(Bytes::from))
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.value(v);
                        }

                        record_builder = record_builder.header(header_builder);
                    }

                    record_builder
                };

                batch_builder = batch_builder
                    .record(record_builder)
                    .last_offset_delta(offset_delta);
            }

            batches.push(batch_builder.build().and_then(TryInto::try_into)?);
        } else {
            batches.push(
                inflated::Batch::builder()
                    .build()
                    .and_then(TryInto::try_into)?,
            );
        }

        Ok(batches)
    }

    pub(super) async fn impl_offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
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

        let log_start = row
            .get_value(0)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let high_watermark = row
            .get_value(1)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let last_stable = row
            .get_value(1)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(high_watermark);

        debug!(cluster = self.cluster, ?topition, log_start, high_watermark,);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
    }

    pub(super) async fn impl_offset_commit(
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

    pub(super) async fn impl_committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
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

    pub(super) async fn impl_offset_for_leader_epoch(
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
            let next_epoch = row.get_value(0)?.as_integer().copied().unwrap_or_default() as i32;
            let end_offset = row.get_value(1)?.as_integer().copied().unwrap_or_default() as i64;
            Ok(Some((next_epoch, end_offset)))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn impl_leader_epoch_history(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
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
                epoch: row.get::<Option<i32>>(0)?.unwrap_or_default(),
                start_offset: row.get::<Option<i64>>(1)?.unwrap_or_default(),
            });
        }

        debug!(cluster = self.cluster, ?topition, ?history);
        Ok(history)
    }

    pub(super) async fn impl_offset_fetch(
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

    pub(super) async fn impl_offset_fetch_records(
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
            let mut rows = c
                .query(
                    &sql_lookup("consumer_offset_select.sql")?,
                    (
                        self.cluster.as_str(),
                        group_id,
                        topic.topic(),
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
                cluster = self.cluster,
                group_id,
                topic = topic.topic,
                partition = topic.partition,
                offset = record.committed_offset()
            );

            assert_eq!(None, offsets.insert(topic.to_owned(), record));
        }

        Ok(offsets)
    }

    pub(super) async fn impl_list_offsets(
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
