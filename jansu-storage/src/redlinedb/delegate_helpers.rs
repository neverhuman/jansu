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
    #[instrument(skip_all)]
    pub(super) async fn connection(&self) -> Result<PoolConnection> {
        let start = SystemTime::now();

        self.pool.acquire().await.map_err(Error::from).inspect(|_| {
            CONNECT_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            )
        })
    }

    #[instrument(skip_all)]
    pub(super) fn ensure_topition_for_produce(
        &self,
        topition: &Topition,
        connection: &mut PoolConnection,
    ) -> Result<()> {
        let topition_exists = {
            let s = sql("topition_select_id.sql").map_err(Error::from)?;
            let mut rows = connection.query(
                &s,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            ).map_err(Error::from)?;
            matches!(rows.step().map_err(Error::from)?, Step::Row(_))
        };

        if topition_exists {
            return Ok(());
        }

        let topic_exists = {
            let s = sql("topic_select_name.sql").map_err(Error::from)?;
            let mut rows = connection.query(
                &s,
                (self.cluster.as_str(), topition.topic()),
            ).map_err(Error::from)?;
            matches!(rows.step().map_err(Error::from)?, Step::Row(_))
        };

        if !topic_exists {
            let partitions = topition.partition() + 1;
            let s = sql("topic_insert.sql").map_err(Error::from)?;
            let _ = connection.execute(
                &s,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    Uuid::new_v4().to_string(),
                    partitions,
                    1_i32,
                ),
            ).map_err(Error::from)?;
        }

        for partition in 0..=topition.partition() {
            let params = (self.cluster.as_str(), topition.topic(), partition);

            let already_exists = {
                let s = sql("topition_select_id.sql").map_err(Error::from)?;
                let mut rows = connection.query(&s, params).map_err(Error::from)?;
                matches!(rows.step().map_err(Error::from)?, Step::Row(_))
            };

            if !already_exists {
                let s = sql("topition_insert.sql").map_err(Error::from)?;
                let _ = connection.execute(&s, params).map_err(Error::from)?;
                let s = sql("watermark_insert.sql").map_err(Error::from)?;
                let _ = connection.execute(&s, params).map_err(Error::from)?;
                let s = sql("leader_epoch_history_insert.sql").map_err(Error::from)?;
                let _ = connection.execute(
                    &s,
                    (self.cluster.as_str(), topition.topic(), partition, 0, 0),
                ).map_err(Error::from)?;
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub(super) fn idempotent_message_check(
        &self,
        _transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &mut PoolConnection,
    ) -> Result<()> {
        let s = sql("producer_epoch_current_for_producer.sql").map_err(Error::from)?;
        let mut rows = connection.query(
            &s,
            (self.cluster.as_str(), deflated.producer_id),
        ).map_err(Error::from)?;

        match rows.step().map_err(Error::from).inspect_err(|err| error!(?err))? {
            Step::Row(row) => {
                let current_epoch = row
                    .get::<i32>(0)
                    .map_err(Error::from)
                    .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))?
                    as i16;

                drop(rows);

                let s = sql("producer_select_for_update.sql").map_err(Error::from)?;
                let mut prows = connection.query(
                    &s,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                    ),
                ).map_err(Error::from).inspect_err(|err| {
                    error!(
                        self.cluster,
                        ?topition,
                        deflated.producer_id,
                        deflated.producer_epoch,
                        ?err
                    )
                })?;

                let Step::Row(prow) = prows.step().map_err(Error::from)? else {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                };

                let sequence = prow.get::<i32>(0).map_err(Error::from).inspect_err(|err| error!(?err))?;

                debug!(
                    self.cluster,
                    ?topition,
                    deflated.producer_id,
                    deflated.producer_epoch,
                    current_epoch,
                    sequence,
                );

                let increment = idempotent_sequence_check(&current_epoch, &sequence, deflated)?;

                debug!(increment);

                drop(prows);

                let s = sql("producer_detail_insert.sql").map_err(Error::from)?;
                let summary = connection.execute(
                    &s,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                        increment,
                    ),
                ).map_err(Error::from)?;
                assert_eq!(1, summary.rows_affected);

                Ok(())
            }
            Step::Done => Err(Error::Api(ErrorCode::UnknownProducerId)),
        }
    }

    #[instrument(skip_all)]
    pub(super) fn watermark_select_for_update(
        &self,
        topition: &Topition,
        connection: &mut PoolConnection,
    ) -> Result<(Option<i64>, Option<i64>)> {
        debug!(?topition);

        let s = sql("watermark_select_no_update.sql").map_err(Error::from)?;
        let mut rows = connection.query(
            &s,
            (
                self.cluster.as_str(),
                topition.topic(),
                topition.partition(),
            ),
        ).map_err(Error::from)?;

        match rows
            .step()
            .map_err(Error::from)
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Step::Row(row) => Ok((
                row.get::<Option<i64>>(0).map_err(Error::from).inspect_err(|err| error!(?err))?,
                row.get::<Option<i64>>(1).map_err(Error::from).inspect_err(|err| error!(?err))?,
            )),
            Step::Done => Err(Error::Api(ErrorCode::UnknownTopicOrPartition)),
        }
    }

    #[instrument(skip_all)]
    pub(super) fn maybe_record_leader_epoch_boundary(
        &self,
        topition: &Topition,
        epoch: i32,
        start_offset: i64,
        connection: &mut PoolConnection,
    ) -> Result<()> {
        let s = sql("leader_epoch_history.sql").map_err(Error::from)?;
        let mut rows = connection.query(
            &s,
            (
                self.cluster.as_str(),
                topition.topic(),
                topition.partition(),
            ),
        ).map_err(Error::from)?;

        let mut current_epoch: Option<i32> = None;
        while let Step::Row(row) = rows.step().map_err(Error::from)? {
            let row_epoch = row.get::<i32>(0).map_err(Error::from).inspect_err(|err| error!(?err))?;
            current_epoch = Some(current_epoch.map_or(row_epoch, |current| current.max(row_epoch)));
        }

        if current_epoch.is_none_or(|current| epoch > current) {
            let s = sql("leader_epoch_history_insert.sql").map_err(Error::from)?;
            let _ = connection.execute(
                &s,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    epoch,
                    start_offset,
                ),
            )
            .map_err(Error::from)
            .inspect_err(|err| error!(?err, ?topition, epoch, start_offset))?;
        }

        Ok(())
    }
}
