//! Core libSQL `Delegate` engine type plus connection-pool and constraint helpers.

use super::*;

/// LibSQL/SQLite storage engine
///
#[derive(Clone, Debug)]
pub(crate) struct Delegate {
    pub(super) cluster: String,
    pub(super) node: i32,
    pub(super) advertised_listener: Url,
    pub(super) pool: Pool,

    pub(super) schemas: Option<Registry>,

    pub(super) lake: Option<House>,

    pub(super) vacuum_into: Option<PathBuf>,

    pub(super) maintenance: Arc<Semaphore>,
    pub(super) compaction: CompactionMode,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionManager {
    pub(super) db: Arc<Mutex<Database>>,
    pub(super) busy_timeout: Duration,
}

pub(crate) struct PoolConnection {
    pub(super) connection: Connection,
}

pub(super) fn is_unique_constraint(error: &libsql::Error) -> bool {
    matches!(
        error,
        libsql::Error::SqliteFailure(SQLITE_CONSTRAINT_UNIQUE, _)
    )
}

pub(super) fn value_to_system_time(value: Value) -> Result<Option<SystemTime>> {
    match value {
        Value::Null => Ok(None),
        other => LiteTimestamp::try_from(other)
            .map(SystemTime::from)
            .map(Some),
    }
}

pub(super) fn unique_constraint(error_code: ErrorCode) -> impl Fn(libsql::Error) -> Error {
    move |err| {
        if let libsql::Error::SqliteFailure(code, ref reason) = err {
            debug!(code, reason);

            if code == SQLITE_CONSTRAINT_UNIQUE {
                Error::Api(error_code)
            } else {
                err.into()
            }
        } else {
            err.into()
        }
    }
}
