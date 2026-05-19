use super::sql::{SQL_DURATION, SQL_ERROR, SQL_REQUESTS};
use super::*;

/// Turso storage engine
///
#[derive(Clone, Debug)]
pub struct Engine {
    pub(super) cluster: String,
    pub(super) node: i32,
    pub(super) advertised_listener: Url,
    pub(super) db: Arc<Mutex<Database>>,

    pub(super) schemas: Option<Registry>,
    pub(super) lake: Option<House>,
}
impl Engine {
    pub fn builder()
    -> Builder<PhantomData<String>, PhantomData<i32>, PhantomData<Url>, PhantomData<Url>> {
        Builder::default()
    }

    pub(super) async fn connection(&self) -> Result<Connection> {
        let db = self.db.lock()?;
        db.connect().map_err(Into::into)
    }

    pub(super) fn attributes_for_error(&self, sql: &str, error: &turso::Error) -> Vec<KeyValue> {
        debug!(sql, ?error);

        let mut attributes = vec![
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        match error {
            turso::Error::SqlExecutionFailure(reason)
            | turso::Error::WalOperationError(reason)
            | turso::Error::ConversionFailure(reason)
            | turso::Error::MutexError(reason) => {
                attributes.push(KeyValue::new("reason", reason.clone()));
            }
            turso::Error::ToSqlConversionFailure(err) => {
                attributes.push(KeyValue::new("reason", err.to_string()));
            }
            turso::Error::QueryReturnedNoRows => {
                attributes.push(KeyValue::new("reason", "query returned no rows"));
            }
        }

        attributes
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
}
