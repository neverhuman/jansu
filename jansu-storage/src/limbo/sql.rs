use super::*;

pub(super) static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sqlite_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
        .build()
});

pub(super) static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_sqlite_requests")
        .with_description("The number of SQL requests made")
        .build()
});

pub(super) static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_sqlite_error")
        .with_description("The SQL error count")
        .build()
});

pub(super) fn fix_parameters(sql: &str) -> Result<String> {
    Regex::new(r"\$(?<i>\d+)")
        .map(|re| re.replace_all(sql, "?$i").into_owned())
        .map_err(Into::into)
}

pub(super) fn sql_lookup(key: &str) -> Result<String> {
    crate::sql::SQL
        .get(key)
        .and_then(|sql| fix_parameters(sql).inspect(|sql| debug!(key, sql)))
}

pub(super) fn unique_constraint(error_code: ErrorCode) -> impl Fn(turso::Error) -> Error {
    move |err| {
        if let turso::Error::SqlExecutionFailure(reason) = &err {
            let reason = reason.to_ascii_lowercase();
            if reason.contains("unique") || reason.contains("constraint") {
                return Error::Api(error_code);
            }
        }

        err.into()
    }
}
