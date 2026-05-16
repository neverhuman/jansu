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

//! Query helpers: prepare_*, tx_prepare_*, watermark, idempotent check

use super::*;

impl Postgres {
    pub fn builder(
        connection: &str,
    ) -> Result<Builder<PhantomData<String>, PhantomData<i32>, Url, Pool>> {
        debug!(connection);
        Builder::from_str(connection)
    }

    pub(super) async fn connection(&self) -> Result<Object> {
        self.pool.get().await.map_err(Into::into)
    }

    pub(super) fn sql_lookup(&self, key: &str) -> Result<&str> {
        crate::sql::SQL.get(key)
    }

    pub(super) async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        tx: &Transaction<'_>,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        if let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "producer_epoch_current_for_producer.sql",
                &[&self.cluster, &deflated.producer_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let current_epoch = row
                .try_get::<_, i16>(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))?;

            let row = self
                .tx_prepare_query_one(
                    tx,
                    "producer_select_for_update.sql",
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                    ],
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

            let sequence = row.try_get::<_, i32>(0).inspect_err(|err| error!(?err))?;

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
                self.tx_prepare_execute(
                    tx,
                    "producer_detail_insert.sql",
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                        &increment,
                    ],
                )
                .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    pub(super) async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        tx: &Transaction<'_>,
    ) -> Result<(Option<i64>, Option<i64>)> {
        if let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "watermark_select_for_update.sql",
                &[&self.cluster, &topition.topic(), &topition.partition()],
            )
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Ok((
                row.try_get::<_, Option<i64>>(0)
                    .inspect_err(|err| error!(?err))?,
                row.try_get::<_, Option<i64>>(1)
                    .inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    pub(super) fn attributes_for_error(
        &self,
        nickname: &str,
        error: &tokio_postgres::error::Error,
    ) -> Vec<KeyValue> {
        let mut attributes = vec![
            KeyValue::new("sql", nickname.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        if let Some(db_error) = error.as_db_error() {
            if let Some(schema) = db_error.schema() {
                attributes.push(KeyValue::new("schema", schema.to_owned()));
            }

            if let Some(table) = db_error.table() {
                attributes.push(KeyValue::new("table", table.to_owned()));
            }

            if let Some(constraint) = db_error.constraint() {
                attributes.push(KeyValue::new("constraint", constraint.to_owned()));
            }
        }

        if let Some(code) = error.code() {
            attributes.push(KeyValue::new("code", format!("{code:?}")));
        }

        attributes
    }

    #[instrument(skip(self, c, params))]
    pub(super) async fn prepare_execute(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        c.execute(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    pub(super) async fn prepare_query(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    pub(super) async fn prepare_query_one(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query_one(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                debug!(?err);
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    pub(super) async fn prepare_query_opt(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query_opt(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }
}
