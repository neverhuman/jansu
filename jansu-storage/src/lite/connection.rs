use super::*;

impl Debug for PoolConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolConnection")
            .field("connection", &self.connection)
            .finish()
    }
}

impl PoolConnection {
    async fn journal_mode(connection: &Connection) -> Result<()> {
        let mut rows = connection.query("PRAGMA journal_mode = WAL", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(journal_mode = row.get_str(0)?);
        }

        Ok(())
    }

    async fn synchronous(connection: &Connection) -> Result<()> {
        _ = connection
            .execute("PRAGMA synchronous = normal", ())
            .await
            .inspect(|rows| debug!(rows))?;

        let mut rows = connection.query("PRAGMA synchronous", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(synchronous = row.get_str(0)?);
        }

        Ok(())
    }

    async fn wal_autocheckpoint(connection: &Connection) -> Result<()> {
        let mut rows = connection.query("PRAGMA wal_autocheckpoint", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(wal_autocheckpoint = row.get_str(0)?);
        }

        Ok(())
    }

    async fn journal_size_limit(connection: &Connection) -> Result<()> {
        let mut rows = connection.query("PRAGMA journal_size_limit", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(journal_size_limit = row.get_str(0)?);
        }

        Ok(())
    }

    async fn foreign_keys(connection: &Connection) -> Result<()> {
        connection
            .execute("PRAGMA foreign_keys = ON", ())
            .await
            .map_err(Into::into)
            .and(Ok(()))
    }

    async fn init(connection: &Connection) -> Result<()> {
        Self::journal_mode(connection).await?;
        Self::synchronous(connection).await?;
        Self::wal_autocheckpoint(connection).await?;
        Self::journal_size_limit(connection).await?;
        Self::foreign_keys(connection).await?;
        Ok(())
    }

    async fn new(connection: Connection) -> Result<Self> {
        Self::init(&connection).await?;

        Ok(Self { connection })
    }

    #[instrument(skip(self))]
    async fn prepared_statement(&self, key: &str) -> Result<Statement, libsql::Error> {
        let sql = SQL
            .0
            .get(key)
            .ok_or(libsql::Error::Misuse(format!("Unknown cache key: {}", key)))?;

        self.connection.prepare(sql).await
    }

    fn attributes_for_error(&self, key: Option<&str>, error: &libsql::Error) -> Vec<KeyValue> {
        debug!(key, ?error);

        let mut attributes = if let Some(sql) = key {
            vec![KeyValue::new("key", sql.to_owned())]
        } else {
            vec![]
        };

        if let libsql::Error::SqliteFailure(code, _) = error {
            attributes.push(KeyValue::new("code", format!("{code}")));
        }

        attributes
    }

    #[instrument(skip_all)]
    pub(super) async fn transaction(&self) -> Result<Transaction> {
        let start = SystemTime::now();

        self.connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .inspect(|_tx| TRANSACTION_WITH_BEHAVIOR_DURATION.record(elapsed_millis(start), &[]))
            .inspect_err(|err| {
                error!(?err, elapsed_millis = elapsed_millis(start));

                SQL_ERROR.add(1, &self.attributes_for_error(None, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    pub(super) async fn commit(&self, tx: Transaction) -> Result<()> {
        let start = SystemTime::now();

        tx.commit()
            .await
            .inspect(|_| TRANSACTION_COMMIT_DURATION.record(elapsed_millis(start), &[]))
            .inspect_err(|err| {
                error!(?err, elapsed_millis = elapsed_millis(start));
                SQL_ERROR.add(1, &self.attributes_for_error(None, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    pub(super) async fn query<P>(&self, sql: &str, params: P) -> result::Result<Rows, libsql::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(sql, ?params);

        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        statement
            .query(params)
            .await
            .inspect(|rows| {
                debug!(?rows);

                SQL_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("sql", sql.to_owned())],
                );

                SQL_REQUESTS.add(1, &[KeyValue::new("sql", sql.to_owned())]);
            })
            .inspect_err(|err| {
                error!(?err, elapsed_millis = elapsed_millis(start));

                SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
            })
    }

    #[instrument(skip_all)]
    pub(super) async fn execute<P>(
        &self,
        sql: &str,
        params: P,
    ) -> result::Result<usize, libsql::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(sql, ?params);
        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        statement
            .execute(params)
            .await
            .inspect(|rows| {
                let elapsed_millis = elapsed_millis(start);
                debug!(rows, elapsed_millis);

                SQL_DURATION.record(elapsed_millis, &[KeyValue::new("sql", sql.to_owned())]);
                SQL_REQUESTS.add(1, &[KeyValue::new("sql", sql.to_owned())]);
            })
            .inspect_err(|err| {
                error!(?err, sql, elapsed_millis = elapsed_millis(start));

                SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
            })
    }

    #[instrument(skip_all)]
    pub(super) async fn query_opt<P>(
        &self,
        sql: &str,
        params: P,
    ) -> result::Result<Option<Row>, libsql::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(sql, ?params);

        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        let mut rows = statement.query(params).await.inspect_err(|err| {
            error!(?err, elapsed_millis = elapsed_millis(start));
            SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
        })?;

        let row = rows.next().await.inspect_err(|err| {
            error!(?err, elapsed_millis = elapsed_millis(start));
            SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
        })?;

        let attributes = [KeyValue::new("sql", sql.to_owned())];

        SQL_DURATION.record(elapsed_millis(start), &attributes);
        SQL_REQUESTS.add(1, &attributes);

        Ok(row)
    }

    #[instrument(skip_all)]
    pub(super) async fn query_one<P>(
        &self,
        sql: &str,
        params: P,
    ) -> result::Result<Row, libsql::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(sql, ?params);

        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        let mut rows = statement
            .query(params)
            .await
            .inspect(|rows| debug!(?rows))
            .inspect_err(|err| {
                if is_unique_constraint(err) {
                    debug!(?err);
                } else {
                    error!(?err, elapsed_millis = elapsed_millis(start));
                    SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
                }
            })?;

        if let Some(row) = rows
            .next()
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| {
                if is_unique_constraint(err) {
                    debug!(?err);
                } else {
                    error!(?err, elapsed_millis = elapsed_millis(start));
                    SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
                }
            })?
        {
            let attributes = [KeyValue::new("sql", sql.to_owned())];

            SQL_DURATION.record(elapsed_millis(start), &attributes);

            SQL_REQUESTS.add(1, &attributes);

            Ok(row).inspect(|row| debug!(?row))
        } else {
            panic!("more or less than one row");
        }
    }
}

impl managed::Manager for ConnectionManager {
    type Type = PoolConnection;
    type Error = Error;

    #[instrument(skip_all)]
    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let start = SystemTime::now();

        let connection = {
            let db = self.db.lock()?;
            db.connect()
        }?;

        connection.busy_timeout(self.busy_timeout)?;

        PoolConnection::new(connection)
            .await
            .inspect(|_| CONNECT_DURATION.record(elapsed_millis(start), &[]))
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        Ok(())
    }
}

pub(super) type Pool = managed::Pool<ConnectionManager>;
