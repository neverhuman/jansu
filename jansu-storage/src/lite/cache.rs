//! DDL and SQL statement caches for the libSQL storage engine.
//!
//! The `include_sql!` macro is defined in the parent module so the test module
//! can also use it.

use super::*;

pub(super) static DDL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        ("010-cluster.sql", include_sql!("../ddl/010-cluster.sql")),
        (
            "020-consumer-group.sql",
            include_sql!("../ddl/020-consumer-group.sql"),
        ),
        ("020-producer.sql", include_sql!("../ddl/020-producer.sql")),
        (
            "020-scram-credential.sql",
            include_sql!("../ddl/020-scram-credential.sql"),
        ),
        ("020-topic.sql", include_sql!("../ddl/020-topic.sql")),
        (
            "030-consumer-group-detail.sql",
            include_sql!("../ddl/030-consumer-group-detail.sql"),
        ),
        (
            "030-producer-epoch.sql",
            include_sql!("../ddl/030-producer-epoch.sql"),
        ),
        (
            "030-topic-configuration.sql",
            include_sql!("../ddl/030-topic-configuration.sql"),
        ),
        ("030-topition.sql", include_sql!("../ddl/030-topition.sql")),
        ("030-txn.sql", include_sql!("../ddl/030-txn.sql")),
        (
            "030-virtual-topic.sql",
            include_sql!("../ddl/030-virtual-topic.sql"),
        ),
        (
            "040-consumer-offset.sql",
            include_sql!("../ddl/040-consumer-offset.sql"),
        ),
        ("040-header.sql", include_sql!("../ddl/040-header.sql")),
        (
            "040-producer-detail.sql",
            include_sql!("../ddl/040-producer-detail.sql"),
        ),
        ("040-record.sql", include_sql!("../ddl/040-record.sql")),
        (
            "040-leader-epoch-history.sql",
            include_sql!("../ddl/040-leader-epoch-history.sql"),
        ),
        (
            "040-txn-detail.sql",
            include_sql!("../ddl/040-txn-detail.sql"),
        ),
        (
            "040-watermark.sql",
            include_sql!("../ddl/040-watermark.sql"),
        ),
        (
            "050-txn-offset-commit.sql",
            include_sql!("../ddl/050-txn-offset-commit.sql"),
        ),
        (
            "050-txn-topition.sql",
            include_sql!("../ddl/050-txn-topition.sql"),
        ),
        (
            "060-txn-offset-commit-tp.sql",
            include_sql!("../ddl/060-txn-offset-commit-tp.sql"),
        ),
        (
            "060-txn-produce-offset.sql",
            include_sql!("../ddl/060-txn-produce-offset.sql"),
        ),
    ];

    Cache::new(BTreeMap::from(mapping))
});

pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
    Cache::new(
        crate::sql::SQL
            .iter()
            .map(|(name, sql)| fix_parameters(sql).map(|sql| (*name, sql)))
            .collect::<Result<BTreeMap<_, _>>>()
            .unwrap_or_else(|_| BTreeMap::new()),
    )
});

pub(super) fn fix_parameters(sql: &str) -> Result<String> {
    Regex::new(r"\$(?<i>\d+)")
        .map(|re| re.replace_all(sql, "?$i").into_owned())
        .map_err(Into::into)
}
