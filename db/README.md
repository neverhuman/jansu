# Database Surface

Jansu stores relational schema bootstrap SQL in `etc/initdb.d/` and query
assets in `jansu-storage/src/sql/` plus the libSQL-specific assets under
`jansu-storage/src/lite/`.

The storage crate owns runtime database access. Broker, service, auth, schema,
and CLI crates must go through typed storage/service APIs instead of opening
database connections directly.
