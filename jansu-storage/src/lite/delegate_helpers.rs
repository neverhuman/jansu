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
    pub(super) async fn connection(
        &self,
    ) -> Result<managed::Object<ConnectionManager>> {
        let start = SystemTime::now();

        self.pool.get().await.map_err(Into::into).inspect(|_| {
            CONNECT_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            )
        })
    }

    #[instrument(skip_all)]
    pub(super) async fn idempotent_message_check(
        &self,
        _transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &PoolConnection,
    ) -> Result<()> {
        let mut rows = connection
            .query(
                "producer_epoch_current_for_producer.sql",
                (self.cluster.as_str(), deflated.producer_id),
            )
            .await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            let current_epoch = row
                .get::<i32>(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))?
                as i16;

            let row = connection
                .query_one(
                    "producer_select_for_update.sql",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                    ),
                )
                .await
                .inspect_err(|err| {
                    error!(
                        self.cluster,
                        ?topition,
                        deflated.producer_id,
                        deflated.producer_epoch,
                        ?err
                    )
                })?;

            let sequence = row.get::<i32>(0).inspect_err(|err| error!(?err))?;

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

            assert_eq!(
                1,
                connection
                    .execute(
                        "producer_detail_insert.sql",
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            deflated.producer_id,
                            deflated.producer_epoch,
                            increment,
                        ),
                    )
                    .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    #[instrument(skip_all)]
    pub(super) async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        connection: &PoolConnection,
    ) -> Result<(Option<i64>, Option<i64>)> {
        debug!(?topition);

        let mut rows = connection
            .query(
                "watermark_select_no_update.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        if let Some(row) = rows
            .next()
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Ok((
                row.get::<Option<i64>>(0).inspect_err(|err| error!(?err))?,
                row.get::<Option<i64>>(1).inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    #[instrument(skip_all)]
    pub(super) async fn maybe_record_leader_epoch_boundary(
        &self,
        topition: &Topition,
        epoch: i32,
        start_offset: i64,
        connection: &PoolConnection,
    ) -> Result<()> {
        let mut rows = connection
            .query(
                "leader_epoch_history.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        let mut current_epoch: Option<i32> = None;
        while let Some(row) = rows.next().await? {
            let row_epoch = row.get::<i32>(0).inspect_err(|err| error!(?err))?;
            current_epoch = Some(current_epoch.map_or(row_epoch, |current| current.max(row_epoch)));
        }

        if current_epoch.is_none_or(|current| epoch > current) {
            _ = connection
                .execute(
                    "leader_epoch_history_insert.sql",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        epoch,
                        start_offset,
                    ),
                )
                .await
                .inspect_err(|err| error!(?err, ?topition, epoch, start_offset))?;
        }

        Ok(())
    }
}
