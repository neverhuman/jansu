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

//! Leader epoch and committed offset topitions helpers

use super::*;

impl Postgres {
    pub(super) async fn offset_for_leader_epoch_storage(
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


    pub(super) async fn leader_epoch_history_storage(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
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
