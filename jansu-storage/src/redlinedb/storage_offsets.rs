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

impl Delegate {
    pub(super) async fn delegate_offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topition);

        let mut c = self.connection().await?;

        let s = sql("watermark_select_no_update.sql").map_err(Error::from)?;
        let mut rows = c
            .query(
                &s,
                (
                    self.cluster.as_str(),
                    self.base_topic(topition.topic()).await?,
                    topition.partition(),
                ),
            )
            .map_err(Error::from)
            .inspect_err(|err| error!(?topition, ?err))?;

        let step = rows.step().map_err(Error::from)?;
        let Some(row) = (match step {
            Step::Row(row) => Some(row),
            Step::Done => None,
        }) else {
            return Ok(OffsetStage {
                last_stable: 0,
                high_watermark: 0,
                log_start: 0,
            });
        };

        let log_start = row
            .get::<Option<i64>>(0)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(0);

        let high_watermark = row
            .get::<Option<i64>>(1)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(0);

        let last_stable = high_watermark;

        debug!(cluster = self.cluster, ?topition, log_start, high_watermark,);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_stage")],
            )
        })
    }

    pub(super) async fn delegate_offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?group, ?retention, ?offsets);

        let mut c = self.connection().await?;
        c.begin(BeginMode::Immediate).map_err(Error::from)?;
        let now = SystemTime::now();
        let expires_at = now.checked_add(retention.unwrap_or(DEFAULT_OFFSET_RETENTION));

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            let base = self.base_topic(topition.topic()).await?;

            let topition_exists = {
                let s = sql("topition_select.sql").map_err(Error::from)?;
                let mut rows = c
                    .query(&s, (self.cluster.as_str(), base, topition.partition()))
                    .map_err(Error::from)
                    .inspect_err(|err| error!(?err))?;
                matches!(rows.step().map_err(Error::from)?, Step::Row(_))
            };

            if topition_exists {
                if !cg_inserted {
                    let s = sql("consumer_group_insert.sql").map_err(Error::from)?;
                    let summary = c
                        .execute(&s, (self.cluster.as_str(), group))
                        .map_err(Error::from)?;
                    debug!(summary.rows_affected);
                    cg_inserted = true;
                }

                let base2 = self.base_topic(topition.topic()).await?;
                let s = sql("consumer_offset_insert.sql").map_err(Error::from)?;
                let summary = c
                    .execute(
                        &s,
                        (
                            self.cluster.as_str(),
                            base2,
                            topition.partition(),
                            group,
                            offset.offset,
                            offset.leader_epoch,
                            offset.timestamp.or(Some(now)).map(Value::from),
                            offset.metadata.as_deref(),
                            expires_at.map(Value::from),
                        ),
                    )
                    .map_err(Error::from)
                    .inspect_err(|err| error!(?err))?;

                debug!(?summary);

                responses.push((
                    topition.to_owned(),
                    if summary.rows_affected == 0 {
                        ErrorCode::UnknownTopicOrPartition
                    } else {
                        ErrorCode::None
                    },
                ));
            } else {
                responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition))
            }
        }

        let _ = c
            .commit()
            .map_err(Error::from)
            .inspect_err(|err| error!(?err))?;

        Ok(responses).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_commit")],
            )
        })
    }

    pub(super) async fn delegate_committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();

        debug!(group_id);

        let mut results = BTreeMap::new();
        let now = SystemTime::now();

        let mut c = self.connection().await?;

        let s = sql("consumer_offset_select_by_group.sql").map_err(Error::from)?;
        let mut rows = c
            .query(&s, (self.cluster.as_str(), group_id))
            .map_err(Error::from)?;

        while let Step::Row(row) = rows.step().map_err(Error::from)? {
            let topic = row.get::<String>(0)?;
            let partition = row.get::<i32>(1)?;
            let offset = row.get::<i64>(2)?;
            let leader_epoch = row.get::<Option<i32>>(3)?;
            let commit_timestamp = value_to_system_time(row.get::<Value>(4).map_err(Error::from)?)?;
            let metadata = row.get::<Option<String>>(5)?;
            let expires_at = value_to_system_time(row.get::<Value>(6).map_err(Error::from)?)?;

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

    pub(super) async fn delegate_offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?group_id, ?topics, ?require_stable);

        let mut c = self.connection().await?;

        let mut offsets = BTreeMap::new();

        for topic in topics {
            let base = self.base_topic(topic.topic()).await?;
            let s = sql("consumer_offset_select.sql").map_err(Error::from)?;
            let mut rows = c
                .query(
                    &s,
                    (self.cluster.as_str(), group_id, base, topic.partition()),
                )
                .map_err(Error::from)
                .inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition
                    )
                })?;

            let record = match rows.step().map_err(Error::from)? {
                Step::Row(row) => {
                    let offset = row.get::<i64>(0)?;
                    let leader_epoch = row.get::<Option<i32>>(1)?;
                    let commit_timestamp =
                        value_to_system_time(row.get::<Value>(2).map_err(Error::from)?)?;
                    let metadata = row.get::<Option<String>>(3)?;
                    let expires_at =
                        value_to_system_time(row.get::<Value>(4).map_err(Error::from)?)?;

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
                Step::Done => OffsetFetchRecord::default().with_offset(-1),
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

    pub(super) async fn delegate_offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        debug!(cluster = self.cluster, ?topition, leader_epoch);

        let mut c = self.connection().await?;

        let s = sql("offset_for_leader_epoch.sql").map_err(Error::from)?;
        let mut rows = c
            .query(
                &s,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    leader_epoch,
                ),
            )
            .map_err(Error::from)?;

        match rows.step().map_err(Error::from)? {
            Step::Row(row) => {
                let next_epoch = row.get::<Option<i64>>(0)?.unwrap_or(0) as i32;
                let end_offset = row.get::<Option<i64>>(1)?.unwrap_or(0);
                Ok(Some((next_epoch, end_offset)))
            }
            Step::Done => Ok(None),
        }
    }

    pub(super) async fn delegate_leader_epoch_history(
        &self,
        topition: &Topition,
    ) -> Result<Vec<LeaderEpochRecord>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topition);

        let mut c = self.connection().await?;

        let topition_exists = {
            let s = sql("topition_select_id.sql").map_err(Error::from)?;
            let mut rows = c
                .query(
                    &s,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                    ),
                )
                .map_err(Error::from)?;
            matches!(rows.step().map_err(Error::from)?, Step::Row(_))
        };

        if !topition_exists {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let s = sql("leader_epoch_history.sql").map_err(Error::from)?;
        let mut rows = c
            .query(
                &s,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .map_err(Error::from)?;

        let mut history = vec![];

        while let Step::Row(row) = rows.step().map_err(Error::from)? {
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

    pub(super) async fn delegate_offset_fetch(
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
