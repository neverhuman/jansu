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
    #[instrument(skip(self), ret)]
    pub(super) async fn policy_compact_delete(
        &self,
        topition: i64,
        offset_id: i64,
    ) -> Result<u64> {
        let pc = self.connection().await?;

        pc.execute("lite/policy_compact_delete.sql", (topition, offset_id))
            .await
            .map(|rows| rows as u64)
            .inspect(|rows| debug!(rows))
            .map_err(Into::into)
    }

    #[instrument(skip(self))]
    pub(super) async fn policy_compact_compaction(
        &self,
        topition: i64,
        key: &[u8],
        max_offset_id: i64,
    ) -> Result<Vec<i64>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query(
                "lite/policy_compact_compaction.sql",
                (topition, key, max_offset_id),
            )
            .await?;

        let mut offsets = Vec::new();

        while let Some(row) = rows.next().await? {
            let offset = row.get::<i64>(0)?;
            offsets.push(offset);
        }

        Ok(offsets)
    }

    #[instrument(skip(self))]
    pub(super) async fn policy_compact_max_offset_id(
        &self,
        topition: i64,
        key: &[u8],
    ) -> Result<Option<i64>> {
        let pc = self.connection().await?;

        if let Some(row) = pc
            .query_opt("lite/policy_compact_max_offset_id.sql", (topition, key))
            .await?
        {
            row.get::<i64>(0).map(Some).map_err(Into::into)
        } else {
            Ok(None)
        }
        .inspect(|max_offset| debug!(max_offset))
    }

    #[instrument(skip(self))]
    pub(super) async fn policy_compact_distinct_k(
        &self,
        topition: i64,
    ) -> Result<BTreeSet<Vec<u8>>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query("lite/policy_compact_distinct_k.sql", [topition])
            .await?;

        let mut keys = BTreeSet::new();

        while let Some(row) = rows.next().await? {
            if let Some(key) = row.get::<Option<Vec<u8>>>(0)? {
                _ = keys.insert(key);
            }
        }

        debug!(keys = keys.len());

        Ok(keys)
    }

    #[instrument(skip(self))]
    pub(super) async fn policy_compact_topitions(&self) -> Result<BTreeSet<i64>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query("lite/policy_compact_topitions.sql", [self.cluster.as_str()])
            .await?;

        let mut topitions = BTreeSet::new();

        while let Some(row) = rows.next().await? {
            let topition = row.get::<i64>(0)?;
            _ = topitions.insert(topition);
        }

        Ok(topitions)
    }

    #[instrument(skip(self))]
    pub(super) async fn policy_compact(&self) -> Result<u64> {
        let start = SystemTime::now();

        match self.compaction {
            CompactionMode::Single => {
                let pc = self.connection().await?;

                pc.execute("policy_compact.sql", [self.cluster.as_str()])
                    .await
                    .map(|compacted| compacted as u64)
                    .inspect(|_| {
                        DELEGATE_REQUEST_DURATION.record(
                            elapsed_millis(start),
                            &[KeyValue::new("operation", "policy_compact_single")],
                        )
                    })
                    .map_err(Into::into)
            }
            CompactionMode::Multi => {
                let mut compacted = 0;

                for topition in self.policy_compact_topitions().await? {
                    debug!(topition);

                    for key in self.policy_compact_distinct_k(topition).await? {
                        debug!(key = ?&key[..]);

                        if let Some(max_offset_id) = self
                            .policy_compact_max_offset_id(topition, &key[..])
                            .await?
                        {
                            debug!(max_offset_id);

                            for offset in self
                                .policy_compact_compaction(topition, &key[..], max_offset_id)
                                .await?
                            {
                                debug!(offset);

                                compacted += self.policy_compact_delete(topition, offset).await?;
                            }
                        }
                    }
                }

                Ok(compacted).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "policy_compact_multi")],
                    )
                })
            }
        }
    }

    #[instrument(skip(self))]
    pub(super) async fn vacuum_into(&self) -> Result<()> {
        if let Some(vacuum_into) = self
            .vacuum_into
            .as_deref()
            .inspect(|vacuum_into| debug!(vacuum_into = vacuum_into.to_str()))
        {
            let mut staging = PathBuf::from(vacuum_into);
            if staging.add_extension("staging") {
                debug!(staging = staging.to_str());

                if let Some(vacuum_staging) = staging.to_str() {
                    let pc = self.connection().await?;
                    let rows = pc
                        .execute("lite/vacuum_into.sql", [vacuum_staging])
                        .await? as u64;

                    rename(staging, vacuum_into).await?;
                    debug!(vacuum_into = vacuum_into.to_str(), rows);
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self, now), ret)]
    pub(super) async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let start = SystemTime::now();

        let now = to_timestamp(&now)?;
        let retention_ms = u64::try_from(Duration::from_hours(7 * 24).as_millis())?;

        let pc = self.connection().await?;

        pc.execute(
            "lite/policy_delete.sql",
            (self.cluster.as_str(), now, retention_ms),
        )
        .await
        .map(|deleted| deleted as u64)
        .map_err(Into::into)
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "policy_delete")],
            )
        })
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

        let row = c
            .query_one(
                "virtual_topic_upsert.sql",
                (self.cluster.as_str(), topic, key, uuid.to_string()),
            )
            .await?;

        row.get_str(0)
            .map_err(Error::from)
            .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
            .inspect(|vt| debug!(%vt))
    }
}
