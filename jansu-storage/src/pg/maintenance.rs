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

//! Retention/compaction and virtual-topic helpers for the PostgreSQL backend.

use super::*;

impl Postgres {
    #[instrument(skip(self), ret)]
    pub(super) async fn policy_compact(&self) -> Result<u64> {
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let compacted = self
            .tx_prepare_execute(&tx, "policy_compact.sql", &[&self.cluster])
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(compacted))
    }

    #[instrument(skip(self), ret)]
    pub(super) async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let retention_secs = i32::try_from(Duration::from_hours(7 * 24).as_secs())?;

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let deleted = self
            .tx_prepare_execute(
                &tx,
                "policy_delete.sql",
                &[&self.cluster, &now, &retention_secs],
            )
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(deleted))
    }

    pub(super) async fn topic_with_key<'a>(
        &self,
        topic: &'a str,
    ) -> Result<(&'a str, Option<&'a str>)> {
        if let Some((base, key)) = topic.split_once('/')
            && self
                .describe_config(base, ConfigResource::Topic, None)
                .await
                .map(|configs| {
                    configs
                        .configs
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .find_map(|config| {
                            if config.name == "jansu.virtual" {
                                config
                                    .value
                                    .as_deref()
                                    .and_then(|config| bool::from_str(config).ok())
                            } else {
                                None
                            }
                        })
                        .unwrap_or(false)
                })?
        {
            Ok((base, Some(key)))
        } else {
            Ok((topic, None))
        }
    }

    #[instrument(skip(self), ret)]
    pub(super) async fn base_topic<'a>(&self, topic: &'a str) -> Result<&'a str> {
        self.topic_with_key(topic).await.map(|(topic, _key)| topic)
    }

    #[instrument(skip(self), ret)]
    pub(super) async fn virtual_topic_id(&self, topic: &str, key: &str) -> Result<Uuid> {
        let uuid = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("tag:jansu.io,2026-04:virtual:{topic}:{key}",).as_bytes(),
        );

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let row = self
            .prepare_query_one(
                &c,
                "virtual_topic_upsert.sql",
                &[&self.cluster, &topic, &key.as_bytes(), &uuid],
            )
            .await?;

        row.try_get::<_, Uuid>(0)
            .inspect_err(|err| error!(?err))
            .map_err(Into::into)
            .inspect(|vt| debug!(%vt))
    }
}
