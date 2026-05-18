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
    pub(super) async fn policy_compact_delete(&self, topition: i64, offset_id: i64) -> Result<u64> {
        let mut pc = self.connection().await?;
        let s = sql("redlinedb/policy_compact_delete.sql").map_err(Error::from)?;

        pc.execute(&s, (topition, offset_id))
            .map_err(Error::from)
            .map(|summary| summary.rows_affected)
            .inspect(|rows| debug!(rows))
    }

    #[instrument(skip(self))]
    pub(super) async fn policy_compact_compaction(
        &self,
        topition: i64,
        key: &[u8],
        max_offset_id: i64,
    ) -> Result<Vec<i64>> {
        let mut pc = self.connection().await?;
        let s = sql("redlinedb/policy_compact_compaction.sql").map_err(Error::from)?;

        let mut rows = pc
            .query(&s, (topition, key, max_offset_id))
            .map_err(Error::from)?;

        let mut offsets = Vec::new();

        while let Step::Row(row) = rows.step().map_err(Error::from)? {
            let offset = row.get::<i64>(0).map_err(Error::from)?;
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
        let mut pc = self.connection().await?;
        let s = sql("redlinedb/policy_compact_max_offset_id.sql").map_err(Error::from)?;

        let mut rows = pc.query(&s, (topition, key)).map_err(Error::from)?;

        let result = match rows.step().map_err(Error::from)? {
            Step::Row(row) => row.get::<i64>(0).map(Some).map_err(Error::from)?,
            Step::Done => None,
        };

        Ok(result).inspect(|max_offset| debug!(max_offset))
    }

    #[instrument(skip(self))]
    pub(super) async fn policy_compact_distinct_k(
        &self,
        topition: i64,
    ) -> Result<BTreeSet<Vec<u8>>> {
        let mut pc = self.connection().await?;
        let s = sql("redlinedb/policy_compact_distinct_k.sql").map_err(Error::from)?;

        let mut rows = pc.query(&s, (topition,)).map_err(Error::from)?;

        let mut keys = BTreeSet::new();

        while let Step::Row(row) = rows.step().map_err(Error::from)? {
            if let Some(key) = row.get::<Option<Vec<u8>>>(0).map_err(Error::from)? {
                let _ = keys.insert(key);
            }
        }

        debug!(keys = keys.len());

        Ok(keys)
    }

    #[instrument(skip(self))]
    pub(super) async fn policy_compact_topitions(&self) -> Result<BTreeSet<i64>> {
        let mut pc = self.connection().await?;
        let s = sql("redlinedb/policy_compact_topitions.sql").map_err(Error::from)?;

        let mut rows = pc
            .query(&s, (self.cluster.as_str(),))
            .map_err(Error::from)?;

        let mut topitions = BTreeSet::new();

        while let Step::Row(row) = rows.step().map_err(Error::from)? {
            let topition = row.get::<i64>(0).map_err(Error::from)?;
            let _ = topitions.insert(topition);
        }

        Ok(topitions)
    }

    #[instrument(skip(self))]
    pub(super) async fn policy_compact(&self) -> Result<u64> {
        let start = SystemTime::now();

        match self.compaction {
            CompactionMode::Single => {
                let mut pc = self.connection().await?;
                let s = sql("policy_compact.sql").map_err(Error::from)?;

                pc.execute(&s, (self.cluster.as_str(),))
                    .map_err(Error::from)
                    .map(|summary| summary.rows_affected)
                    .inspect(|_| {
                        DELEGATE_REQUEST_DURATION.record(
                            elapsed_millis(start),
                            &[KeyValue::new("operation", "policy_compact_single")],
                        )
                    })
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

                if staging.to_str().is_some() {
                    let db = self.pool.database().clone();
                    let staging_clone = staging.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        db.backup_physical_to_path(staging_clone, PhysicalBackupOptions::default())
                    })
                    .await
                    .map_err(|e| RedlineError::new(RedlineErrorCode::Internal, e.to_string()))
                    .map_err(Error::from)?
                    .map_err(Error::from)?;

                    rename(staging, vacuum_into).await?;
                    debug!(vacuum_into = vacuum_into.to_str());
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self, now), ret)]
    pub(super) async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let start = SystemTime::now();

        let now = to_timestamp(&now)?;
        let default_retention_ms = i64::try_from(Duration::from_hours(7 * 24).as_millis())?;

        let candidates: Vec<(i64, i64, i64, i64)> = {
            let mut pc = self.connection().await?;
            let s = sql("redlinedb/policy_delete_candidates.sql").map_err(Error::from)?;
            let mut rows = pc
                .query(&s, (self.cluster.as_str(),))
                .map_err(Error::from)?;
            let mut out = vec![];
            while let Step::Row(row) = rows.step().map_err(Error::from)? {
                let topition = row.get::<i64>(0).map_err(Error::from)?;
                let offset = row.get::<i64>(1).map_err(Error::from)?;
                let timestamp = row.get::<i64>(2).map_err(Error::from)?;
                let retention = row
                    .get::<Option<String>>(3)
                    .map_err(Error::from)?
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(default_retention_ms);
                out.push((topition, offset, timestamp, retention));
            }
            out
        };

        let mut deleted = 0;

        for (topition, offset, timestamp, retention) in candidates {
            if retention >= 0 && now.saturating_sub(timestamp) > retention {
                deleted += self.policy_compact_delete(topition, offset).await?;
            }
        }

        Ok(deleted).inspect(|_| {
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

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;

        let s = sql("virtual_topic_upsert.sql").map_err(Error::from)?;
        let mut rows = c
            .query(&s, (self.cluster.as_str(), topic, key, uuid.to_string()))
            .map_err(Error::from)?;

        match rows.step().map_err(Error::from)? {
            Step::Row(row) => {
                let str_val = row.get::<String>(0).map_err(Error::from)?;
                Uuid::parse_str(&str_val)
                    .map_err(Into::into)
                    .inspect(|vt| debug!(%vt))
            }
            Step::Done => Err(Error::Api(ErrorCode::UnknownTopicOrPartition)),
        }
    }
}
