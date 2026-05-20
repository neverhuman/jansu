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

//! Transactional prepared-statement helpers for the PostgreSQL backend.

use super::*;

impl Postgres {
    #[instrument(skip(self, tx, params))]
    pub(super) async fn tx_prepare_execute(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let stmt_key = sql;
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(stmt_key, ?err))?;

        let execute_start = SystemTime::now();

        tx.execute(&prepared, params)
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

    #[instrument(skip(self, tx, params))]
    pub(super) async fn tx_prepare_query(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query(&prepared, params)
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

    #[instrument(skip(self, tx, params))]
    pub(super) async fn tx_prepare_query_one(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query_one(&prepared, params)
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

    #[instrument(skip(self, tx, params))]
    pub(super) async fn tx_prepare_query_opt(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query_opt(&prepared, params)
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

    #[instrument(skip(self, tx, params))]
    pub(super) async fn tx_prepare_query_raw<P, I>(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: I,
    ) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        tx.query_raw(&prepared, params).await.map_err(Into::into)
    }
}
