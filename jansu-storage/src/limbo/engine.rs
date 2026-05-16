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

impl Engine {
    pub(super) async fn connection(&self) -> Result<Connection> {
        let db = self.db.lock()?;
        db.connect().map_err(Into::into)
    }

    pub(super) fn attributes_for_error(&self, sql: &str, error: &turso::Error) -> Vec<KeyValue> {
        debug!(sql, ?error);
        vec![
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ]
    }

    pub(super) async fn prepare_execute<P>(
        &self,
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> result::Result<u64, turso::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(?connection, sql, ?params);

        let mut statement = connection.prepare(sql).await?;

        let execute_start = SystemTime::now();

        statement
            .execute(params)
            .await
            .inspect(|rows| {
                debug!(rows);

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
    }

    pub(super) async fn prepare_query_opt<P>(
        &self,
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> result::Result<Option<Row>, turso::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(?connection, sql, ?params);

        let mut statement = connection.prepare(sql).await?;

        let execute_start = SystemTime::now();

        let mut rows = statement.query(params).await.inspect_err(|err| {
            SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
        })?;

        let row = rows.next().await.inspect_err(|err| {
            SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
        })?;

        let attributes = [
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &attributes,
        );

        SQL_REQUESTS.add(1, &attributes);

        Ok(row)
    }

    pub(super) async fn prepare_query_one<P>(
        &self,
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> result::Result<Row, turso::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(?connection, sql, ?params);

        let mut statement = connection
            .prepare(sql)
            .await
            .inspect_err(|err| error!(?err, sql))?;

        let execute_start = SystemTime::now();

        let mut rows = statement.query(params).await.inspect_err(|err| {
            error!(?err, sql);
            SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
        })?;

        if let Some(row) = rows
            .next()
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| {
                error!(?err, sql);
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })?
        {
            let attributes = [
                KeyValue::new("sql", sql.to_owned()),
                KeyValue::new("cluster_id", self.cluster.clone()),
            ];

            SQL_DURATION.record(
                execute_start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &attributes,
            );

            SQL_REQUESTS.add(1, &attributes);

            Ok(row).inspect(|row| debug!(?row))
        } else {
            panic!("more or less than one row");
        }
    }

    pub(super) async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &Connection,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        let mut rows = connection
            .query(
                &sql_lookup("producer_epoch_current_for_producer.sql")?,
                (self.cluster.as_str(), deflated.producer_id),
            )
            .await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            let current_epoch = row
                .get_value(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|value| value as i16)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

            let row = self
                .prepare_query_one(
                    connection,
                    &sql_lookup("producer_select_for_update.sql")?,
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

            let sequence = row
                .get_value(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|value| value as i32)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

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
                self.prepare_execute(
                    connection,
                    &sql_lookup("producer_detail_insert.sql")?,
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

    pub(super) async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        tx: &Connection,
    ) -> Result<(Option<i64>, Option<i64>)> {
        debug!(?topition, ?tx);

        let mut rows = tx
            .query(
                &sql_lookup("watermark_select_no_update.sql")?,
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
                row.get_value(0)
                    .map(|value| value.as_integer().copied())
                    .inspect_err(|err| error!(?err))?,
                row.get_value(1)
                    .map(|value| value.as_integer().copied())
                    .inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    pub(super) async fn maybe_record_leader_epoch_boundary<'conn>(
        &self,
        topition: &Topition,
        epoch: i32,
        start_offset: i64,
        tx: &Transaction<'conn>,
    ) -> Result<()> {
        let mut rows = tx
            .query(
                &sql_lookup("leader_epoch_history.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        let mut current_epoch: Option<i32> = None;
        while let Some(row) = rows.next().await? {
            let row_epoch = row.get::<Option<i32>>(0)?.unwrap_or_default();
            current_epoch = Some(current_epoch.map_or(row_epoch, |current| current.max(row_epoch)));
        }

        if current_epoch.is_none_or(|current| epoch > current) {
            _ = self
                .prepare_execute(
                    tx,
                    &sql_lookup("leader_epoch_history_insert.sql")?,
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
