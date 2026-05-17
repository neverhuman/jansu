use super::*;

#[derive(Clone, Debug)]
pub(crate) struct Pool {
    manager: ConnectionManager,
    permits: Arc<Semaphore>,
}

#[derive(Clone, Debug)]
pub(crate) struct PoolBuilder {
    manager: ConnectionManager,
}

impl Pool {
    pub(crate) fn builder(manager: ConnectionManager) -> PoolBuilder {
        PoolBuilder { manager }
    }

    pub(crate) async fn get(&self) -> Result<PoolConnection> {
        let permit = self.permits.clone().acquire_owned().await?;
        self.manager
            .create(permit)
            .await
            .inspect(|_| CONNECT_DURATION.record(0, &[]))
    }

    pub(crate) async fn backup_physical_to_path(&self, path: PathBuf) -> Result<()> {
        redline::backup_physical_to_path(self.manager.db.clone(), path)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }
}

impl PoolBuilder {
    pub(crate) fn build(self) -> Result<Pool> {
        Ok(Pool {
            manager: self.manager,
            permits: Arc::new(Semaphore::new(16)),
        })
    }
}

impl PoolConnection {
    async fn new(connection: Connection, permit: OwnedSemaphorePermit) -> Result<Self> {
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            _permit: permit,
        })
    }

    fn attributes_for_error(&self, key: Option<&str>, error: &RedlineError) -> Vec<KeyValue> {
        debug!(key, ?error);

        let mut attributes = if let Some(sql) = key {
            vec![KeyValue::new("key", sql.to_owned())]
        } else {
            vec![]
        };

        attributes.push(KeyValue::new("code", format!("{:?}", error.code())));
        attributes
    }

    fn cached_sql(&self, key: &str) -> result::Result<String, RedlineError> {
        match SQL.0.get(key).cloned() {
            Some(sql) => Ok(sql),
            None => {
                error!(key, "unknown cache key");
                Err(redline::redline_error(format!("unknown cache key: {key}")))
            }
        }
    }

    #[instrument(skip_all)]
    pub(super) async fn transaction(&self) -> Result<Transaction> {
        let start = SystemTime::now();

        Transaction::begin(self.connection.clone())
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
    pub(super) async fn query<P>(&self, sql: &str, params: P) -> result::Result<Rows, RedlineError>
    where
        P: IntoParams,
    {
        debug!(sql, ?params);

        let start = SystemTime::now();
        let cached = self.cached_sql(sql)?;

        redline::query(self.connection.clone(), cached, params)
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
                error!(?err, sql, elapsed_millis = elapsed_millis(start));
                SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
            })
    }

    #[instrument(skip_all)]
    pub(super) async fn execute<P>(
        &self,
        sql: &str,
        params: P,
    ) -> result::Result<usize, RedlineError>
    where
        P: IntoParams,
    {
        debug!(sql, ?params);
        let start = SystemTime::now();
        let cached = self.cached_sql(sql)?;

        redline::execute(self.connection.clone(), cached, params)
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
    ) -> result::Result<Option<Row>, RedlineError>
    where
        P: IntoParams,
    {
        debug!(sql, ?params);

        let start = SystemTime::now();
        let mut rows = self.query(sql, params).await.inspect_err(|err| {
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
    ) -> result::Result<Row, RedlineError>
    where
        P: IntoParams,
    {
        debug!(sql, ?params);

        let start = SystemTime::now();
        let mut rows = self.query(sql, params).await.inspect_err(|err| {
            if is_unique_constraint(err) {
                debug!(?err);
            } else {
                error!(?err, elapsed_millis = elapsed_millis(start));
                SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
            }
        })?;

        if let Some(row) = rows.next().await.inspect_err(|err| {
            if is_unique_constraint(err) {
                debug!(?err);
            } else {
                error!(?err, elapsed_millis = elapsed_millis(start));
                SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
            }
        })? {
            let attributes = [KeyValue::new("sql", sql.to_owned())];

            SQL_DURATION.record(elapsed_millis(start), &attributes);

            SQL_REQUESTS.add(1, &attributes);

            Ok(row).inspect(|row| debug!(?row))
        } else {
            Err(redline::redline_error("more or less than one row"))
        }
    }
}

impl ConnectionManager {
    #[instrument(skip_all)]
    async fn create(&self, permit: OwnedSemaphorePermit) -> Result<PoolConnection> {
        let start = SystemTime::now();

        let mut connection = Connection::new(self.db.connect()?);
        connection.set_busy_timeout(self.busy_timeout);

        PoolConnection::new(connection, permit)
            .await
            .inspect(|_| CONNECT_DURATION.record(elapsed_millis(start), &[]))
    }
}
