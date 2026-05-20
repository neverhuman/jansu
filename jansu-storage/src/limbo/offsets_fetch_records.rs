//! Consumer-group offset fetch `Storage` operations for the Turso `Engine`.

use super::offsets::{integer_or_default, value_to_optional_system_time};
use super::sql::sql_lookup;
use super::*;

impl Engine {
    pub(super) async fn offset_fetch_impl(
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

    pub(super) async fn offset_fetch_records_impl(
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
                    let offset = integer_or_default(row.get_value(0).map_err(Error::from)?, -1);
                    let leader_epoch = row.get::<Option<i32>>(1)?;
                    let commit_timestamp =
                        value_to_optional_system_time(row.get_value(2).map_err(Error::from)?)?;
                    let metadata = row.get::<Option<String>>(3)?;
                    let expires_at =
                        value_to_optional_system_time(row.get_value(4).map_err(Error::from)?)?;

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
}
