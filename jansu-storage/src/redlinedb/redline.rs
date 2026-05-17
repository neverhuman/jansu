use std::{
    collections::VecDeque,
    fmt::Debug,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::task::JoinError;

use crate::Topition;

pub(crate) use ::redlinedb::{
    BeginMode, Database, Error as RedlineError, ErrorCode as RedlineErrorCode, OpenOptions,
    PhysicalBackupOptions, PhysicalBackupStats, Value,
};

pub(crate) type RedlineResult<T> = Result<T, RedlineError>;

pub(crate) struct Connection {
    inner: ::redlinedb::Connection,
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}

impl Connection {
    pub(crate) fn new(inner: ::redlinedb::Connection) -> Self {
        Self { inner }
    }

    pub(crate) fn set_busy_timeout(&mut self, timeout: std::time::Duration) {
        self.inner.set_busy_timeout(timeout);
    }

    fn query_collect<P>(&mut self, sql: &str, params: P) -> RedlineResult<Rows>
    where
        P: IntoParams,
    {
        let mut statement = self.inner.prepare(sql)?;
        statement.bind_all(params.into_params())?;
        let column_count = statement.column_count();
        let mut rows = VecDeque::new();

        loop {
            match statement.step() {
                Ok(::redlinedb::Step::Row(row)) => {
                    let mut values = Vec::with_capacity(column_count);
                    for index in 0..column_count {
                        values.push(row.get::<Value>(index)?);
                    }
                    rows.push_back(Row { values });
                }
                Ok(::redlinedb::Step::Done) => break,
                Err(err) if err.code() == RedlineErrorCode::NotFound => break,
                Err(err) => return Err(err),
            }
        }

        Ok(Rows { rows })
    }

    fn execute_params<P>(&mut self, sql: &str, params: P) -> RedlineResult<usize>
    where
        P: IntoParams,
    {
        self.inner
            .execute(sql, params.into_params())
            .map(|summary| summary.rows_affected as usize)
    }

    fn begin_immediate(&mut self) -> RedlineResult<()> {
        self.inner.begin(BeginMode::Immediate)
    }

    fn commit(&mut self) -> RedlineResult<()> {
        self.inner.commit().map(|_| ())
    }

    fn rollback(&mut self) -> RedlineResult<()> {
        self.inner.rollback()
    }
}

pub(crate) struct Transaction {
    connection: Arc<Mutex<Connection>>,
    committed: bool,
}

impl Transaction {
    pub(crate) async fn begin(connection: Arc<Mutex<Connection>>) -> RedlineResult<Self> {
        with_connection(connection.clone(), |connection| {
            connection.begin_immediate()
        })
        .await?;
        Ok(Self {
            connection,
            committed: false,
        })
    }

    pub(crate) async fn commit(mut self) -> RedlineResult<()> {
        with_connection(self.connection.clone(), |connection| connection.commit()).await?;
        self.committed = true;
        Ok(())
    }

    pub(crate) async fn rollback(mut self) -> RedlineResult<()> {
        with_connection(self.connection.clone(), |connection| connection.rollback()).await?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed
            && let Ok(mut connection) = self.connection.lock()
        {
            let _ = connection.rollback();
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Row {
    values: Vec<Value>,
}

impl Row {
    pub(crate) fn get<T>(&self, index: usize) -> RedlineResult<T>
    where
        T: FromRedlineValue,
    {
        match self.values.get(index) {
            Some(value) => T::from_value(value),
            None => Err(redline_error("column index out of range")),
        }
    }

    pub(crate) fn get_str(&self, index: usize) -> RedlineResult<&str> {
        match self.values.get(index) {
            Some(Value::Text(value)) => Ok(value.as_ref()),
            Some(_) => Err(redline_error("column is not text")),
            None => Err(redline_error("column index out of range")),
        }
    }

    pub(crate) fn get_value(&self, index: usize) -> RedlineResult<Value> {
        match self.values.get(index).cloned() {
            Some(value) => Ok(value),
            None => Err(redline_error("column index out of range")),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Rows {
    rows: VecDeque<Row>,
}

impl Rows {
    pub(crate) async fn next(&mut self) -> RedlineResult<Option<Row>> {
        Ok(self.rows.pop_front())
    }
}

pub(crate) trait ValueExt {
    fn as_integer(&self) -> Option<&i64>;
    fn as_text(&self) -> Option<&str>;
}

impl ValueExt for Value {
    fn as_integer(&self) -> Option<&i64> {
        match self {
            Self::Integer(value) => Some(value),
            _ => None,
        }
    }

    fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value.as_ref()),
            _ => None,
        }
    }
}

pub(crate) trait FromRedlineValue: Sized {
    fn from_value(value: &Value) -> RedlineResult<Self>;
}

impl FromRedlineValue for Value {
    fn from_value(value: &Value) -> RedlineResult<Self> {
        Ok(value.clone())
    }
}

impl FromRedlineValue for i64 {
    fn from_value(value: &Value) -> RedlineResult<Self> {
        match value {
            Value::Integer(value) => Ok(*value),
            _ => Err(redline_error("column is not integer")),
        }
    }
}

impl FromRedlineValue for i32 {
    fn from_value(value: &Value) -> RedlineResult<Self> {
        i64::from_value(value).and_then(|value| {
            i32::try_from(value).map_err(|_| redline_error("integer does not fit i32"))
        })
    }
}

impl FromRedlineValue for i16 {
    fn from_value(value: &Value) -> RedlineResult<Self> {
        i64::from_value(value).and_then(|value| {
            i16::try_from(value).map_err(|_| redline_error("integer does not fit i16"))
        })
    }
}

impl FromRedlineValue for bool {
    fn from_value(value: &Value) -> RedlineResult<Self> {
        i64::from_value(value).map(|value| value != 0)
    }
}

impl FromRedlineValue for String {
    fn from_value(value: &Value) -> RedlineResult<Self> {
        match value {
            Value::Text(value) => Ok(value.to_string()),
            _ => Err(redline_error("column is not text")),
        }
    }
}

impl FromRedlineValue for Vec<u8> {
    fn from_value(value: &Value) -> RedlineResult<Self> {
        match value {
            Value::Blob(value) => Ok(value.as_ref().to_vec()),
            _ => Err(redline_error("column is not blob")),
        }
    }
}

impl<T> FromRedlineValue for Option<T>
where
    T: FromRedlineValue,
{
    fn from_value(value: &Value) -> RedlineResult<Self> {
        match value {
            Value::Null => Ok(None),
            _ => T::from_value(value).map(Some),
        }
    }
}

pub(crate) trait IntoParams: Debug {
    fn into_params(self) -> Vec<Value>;
}

impl IntoParams for () {
    fn into_params(self) -> Vec<Value> {
        Vec::new()
    }
}

impl IntoParams for Vec<Value> {
    fn into_params(self) -> Vec<Value> {
        self
    }
}

impl IntoParams for &[Value] {
    fn into_params(self) -> Vec<Value> {
        self.to_vec()
    }
}

impl<T, const N: usize> IntoParams for [T; N]
where
    T: IntoRedlineValue + Debug,
{
    fn into_params(self) -> Vec<Value> {
        self.into_iter()
            .map(IntoRedlineValue::into_redline_value)
            .collect()
    }
}

impl<T, const N: usize> IntoParams for &[T; N]
where
    T: IntoRedlineValue + Clone + Debug,
{
    fn into_params(self) -> Vec<Value> {
        self.iter()
            .cloned()
            .map(IntoRedlineValue::into_redline_value)
            .collect()
    }
}

macro_rules! tuple_params {
    ($($name:ident),+) => {
        impl<$($name),+> IntoParams for ($($name,)+)
        where
            $($name: IntoRedlineValue + Debug,)+
        {
            #[allow(non_snake_case)]
            fn into_params(self) -> Vec<Value> {
                let ($($name,)+) = self;
                vec![$($name.into_redline_value(),)+]
            }
        }
    };
}

tuple_params!(A);
tuple_params!(A, B);
tuple_params!(A, B, C);
tuple_params!(A, B, C, D);
tuple_params!(A, B, C, D, E);
tuple_params!(A, B, C, D, E, F);
tuple_params!(A, B, C, D, E, F, G);
tuple_params!(A, B, C, D, E, F, G, H);
tuple_params!(A, B, C, D, E, F, G, H, I);
tuple_params!(A, B, C, D, E, F, G, H, I, J);
tuple_params!(A, B, C, D, E, F, G, H, I, J, K);
tuple_params!(A, B, C, D, E, F, G, H, I, J, K, L);

pub(crate) trait IntoRedlineValue {
    fn into_redline_value(self) -> Value;
}

impl IntoRedlineValue for Value {
    fn into_redline_value(self) -> Value {
        self
    }
}

impl IntoRedlineValue for &Value {
    fn into_redline_value(self) -> Value {
        self.clone()
    }
}

impl IntoRedlineValue for i64 {
    fn into_redline_value(self) -> Value {
        Value::Integer(self)
    }
}

impl IntoRedlineValue for i32 {
    fn into_redline_value(self) -> Value {
        Value::Integer(i64::from(self))
    }
}

impl IntoRedlineValue for i16 {
    fn into_redline_value(self) -> Value {
        Value::Integer(i64::from(self))
    }
}

impl IntoRedlineValue for u64 {
    fn into_redline_value(self) -> Value {
        match i64::try_from(self) {
            Ok(value) => Value::Integer(value),
            Err(_) => {
                tracing::warn!(value = self, "u64 value exceeds redline integer range");
                Value::Integer(i64::MAX)
            }
        }
    }
}

impl IntoRedlineValue for usize {
    fn into_redline_value(self) -> Value {
        match i64::try_from(self) {
            Ok(value) => Value::Integer(value),
            Err(_) => {
                tracing::warn!(value = self, "usize value exceeds redline integer range");
                Value::Integer(i64::MAX)
            }
        }
    }
}

impl IntoRedlineValue for bool {
    fn into_redline_value(self) -> Value {
        Value::Integer(i64::from(self))
    }
}

impl IntoRedlineValue for &str {
    fn into_redline_value(self) -> Value {
        Value::Text(Arc::from(self))
    }
}

impl IntoRedlineValue for String {
    fn into_redline_value(self) -> Value {
        Value::Text(Arc::from(self))
    }
}

impl IntoRedlineValue for &String {
    fn into_redline_value(self) -> Value {
        Value::Text(Arc::from(self.as_str()))
    }
}

impl IntoRedlineValue for &[u8] {
    fn into_redline_value(self) -> Value {
        Value::Blob(Arc::from(self))
    }
}

impl IntoRedlineValue for Vec<u8> {
    fn into_redline_value(self) -> Value {
        Value::Blob(Arc::from(self.into_boxed_slice()))
    }
}

impl IntoRedlineValue for &Vec<u8> {
    fn into_redline_value(self) -> Value {
        Value::Blob(Arc::from(self.as_slice()))
    }
}

impl IntoRedlineValue for Bytes {
    fn into_redline_value(self) -> Value {
        Value::Blob(Arc::from(self.to_vec().into_boxed_slice()))
    }
}

impl IntoRedlineValue for &Bytes {
    fn into_redline_value(self) -> Value {
        Value::Blob(Arc::from(self.as_ref()))
    }
}

impl IntoRedlineValue for &Topition {
    fn into_redline_value(self) -> Value {
        Value::Text(Arc::from(format!("{}-{}", self.topic(), self.partition())))
    }
}

impl<T> IntoRedlineValue for Option<T>
where
    T: IntoRedlineValue,
{
    fn into_redline_value(self) -> Value {
        self.map_or(Value::Null, IntoRedlineValue::into_redline_value)
    }
}

pub(crate) async fn with_connection<T, F>(
    connection: Arc<Mutex<Connection>>,
    f: F,
) -> RedlineResult<T>
where
    T: Send + 'static,
    F: FnOnce(&mut Connection) -> RedlineResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut connection = connection
            .lock()
            .map_err(|_| redline_error("redlinedb connection lock poisoned"))?;
        f(&mut connection)
    })
    .await
    .map_err(join_error)?
}

pub(crate) async fn query<P>(
    connection: Arc<Mutex<Connection>>,
    sql: String,
    params: P,
) -> RedlineResult<Rows>
where
    P: IntoParams,
{
    let params = params.into_params();
    with_connection(connection, move |connection| {
        connection.query_collect(sql.as_str(), params)
    })
    .await
}

pub(crate) async fn execute<P>(
    connection: Arc<Mutex<Connection>>,
    sql: String,
    params: P,
) -> RedlineResult<usize>
where
    P: IntoParams,
{
    let params = params.into_params();
    with_connection(connection, move |connection| {
        connection.execute_params(sql.as_str(), params)
    })
    .await
}

pub(crate) async fn backup_physical_to_path(
    database: Database,
    path: PathBuf,
) -> RedlineResult<PhysicalBackupStats> {
    tokio::task::spawn_blocking(move || {
        database.backup_physical_to_path(path, PhysicalBackupOptions::default())
    })
    .await
    .map_err(join_error)?
}

pub(crate) fn redline_error(message: impl Into<String>) -> RedlineError {
    RedlineError::new(RedlineErrorCode::Error, message.into())
}

fn join_error(error: JoinError) -> RedlineError {
    RedlineError::new(RedlineErrorCode::Internal, error.to_string())
}
