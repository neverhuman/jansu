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

//! Consumer-offset commit and leader-epoch dispatch.
//!
//! Inherent `Postgres` methods backing the `Storage` trait implementation.

use super::*;

impl Postgres {
    pub(crate) async fn offset_stage_dispatch(&self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);
        let c = self.connection().await?;

        let row = self
            .prepare_query_one(
                &c,
                "watermark_select.sql",
                &[
                    &self.cluster,
                    &self.base_topic(topition.topic()).await?,
                    &topition.partition(),
                ],
            )
            .await
            .inspect_err(|err| error!(?topition, ?err))?;

        let log_start = row
            .try_get::<_, Option<i64>>(0)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(0);

        let high_watermark = row
            .try_get::<_, Option<i64>>(1)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(0);

        let last_stable = row
            .try_get::<_, Option<i64>>(1)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(high_watermark);

        debug!(cluster = self.cluster, ?topition, log_start, high_watermark,);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
    }

    pub(crate) async fn offset_commit_dispatch(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        debug!(cluster = self.cluster, ?group, ?retention);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;
        let now = SystemTime::now();
        let expires_at = now.checked_add(retention.unwrap_or(DEFAULT_OFFSET_RETENTION));

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            if self
                .tx_prepare_query_opt(
                    &tx,
                    "topition_select.sql",
                    &[
                        &self.cluster,
                        &self.base_topic(topition.topic()).await?,
                        &topition.partition(),
                    ],
                )
                .await
                .inspect_err(|err| error!(?err))?
                .is_some()
            {
                if !cg_inserted {
                    let rows = self
                        .tx_prepare_execute(
                            &tx,
                            "consumer_group_insert.sql",
                            &[&self.cluster, &group],
                        )
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = self
                    .tx_prepare_execute(
                        &tx,
                        "consumer_offset_insert.sql",
                        &[
                            &self.cluster,
                            &self.base_topic(topition.topic()).await?,
                            &topition.partition(),
                            &group,
                            &offset.offset,
                            &offset.leader_epoch,
                            &Some(offset.timestamp.unwrap_or(now)),
                            &offset.metadata,
                            &expires_at,
                        ],
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

    pub(crate) async fn committed_offset_topitions_dispatch(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        debug!(group_id);

        let mut results = BTreeMap::new();
        let now = SystemTime::now();

        let c = self.connection().await?;

        for row in self
            .prepare_query(
                &c,
                "consumer_offset_select_by_group.sql",
                &[&self.cluster, &group_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let topic = row.try_get::<_, String>(0)?;
            let partition = row.try_get::<_, i32>(1)?;
            let offset = row.try_get::<_, i64>(2)?;
            let leader_epoch = row.try_get::<_, Option<i32>>(3)?;
            let commit_timestamp = row.try_get::<_, Option<SystemTime>>(4)?;
            let metadata = row.try_get::<_, Option<String>>(5)?;
            let expires_at = row.try_get::<_, Option<SystemTime>>(6)?;

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

    pub(crate) async fn offset_for_leader_epoch_dispatch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        debug!(cluster = self.cluster, ?topition, leader_epoch);

        let c = self.connection().await?;

        let mut rows = c
            .query(
                self.sql_lookup("offset_for_leader_epoch.sql")?,
                &[
                    &self.cluster.as_str(),
                    &topition.topic(),
                    &topition.partition(),
                    &leader_epoch,
                ],
            )
            .await?;

        if let Some(row) = rows.pop() {
            let next_epoch: i32 = row.try_get(0)?;
            let end_offset: i64 = row.try_get(1)?;
            Ok(Some((next_epoch, end_offset)))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn leader_epoch_history_dispatch(
        &self,
        topition: &Topition,
    ) -> Result<Vec<LeaderEpochRecord>> {
        debug!(cluster = self.cluster, ?topition);

        let c = self.connection().await?;

        if self
            .prepare_query_opt(
                &c,
                "topition_select_id.sql",
                &[&self.cluster, &topition.topic(), &topition.partition()],
            )
            .await?
            .is_none()
        {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let rows = c
            .query(
                self.sql_lookup("leader_epoch_history.sql")?,
                &[
                    &self.cluster.as_str(),
                    &topition.topic(),
                    &topition.partition(),
                ],
            )
            .await?;

        let history = rows
            .into_iter()
            .map(|row| {
                Ok(LeaderEpochRecord {
                    epoch: row.try_get::<_, i32>(0)?,
                    start_offset: row.try_get::<_, i64>(1)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        debug!(cluster = self.cluster, ?topition, ?history);
        Ok(history)
    }
}
