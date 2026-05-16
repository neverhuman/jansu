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

#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, D> {
    pub(super) cluster: C,
    pub(super) node: N,
    pub(super) advertised_listener: L,
    pub(super) storage: D,
    pub(super) schemas: Option<Registry>,
    pub(super) lake: Option<House>,
}

impl<C, N, L, D> Builder<C, N, L, D> {
    pub(crate) fn cluster<T>(self, cluster: T) -> Builder<String, N, L, D>
    where
        T: Into<String>,
    {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub(crate) fn node(self, node: i32) -> Builder<C, i32, L, D> {
        debug!(node);
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub(crate) fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, D> {
        debug!(%advertised_listener);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub(crate) fn storage(self, storage: Url) -> Builder<C, N, L, Url> {
        debug!(%storage);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub(crate) fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, D> {
        Self { schemas, ..self }
    }

    pub(crate) fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }
}

pub(super) static DDL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        ("010-cluster.sql", include_sql!("../ddl/010-cluster.sql")),
        (
            "020-consumer-group.sql",
            include_sql!("../ddl/020-consumer-group.sql"),
        ),
        ("020-producer.sql", include_sql!("../ddl/020-producer.sql")),
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
        ("040-txn-detail.sql", include_sql!("../ddl/040-txn-detail.sql")),
        ("040-watermark.sql", include_sql!("../ddl/040-watermark.sql")),
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
        if let turso::Error::SqlExecutionFailure(ref msg) = err {
            debug!(msg);
            if msg.contains("UNIQUE constraint") {
                return Error::Api(error_code);
            }
        }
        err.into()
    }
}

impl Builder<String, i32, Url, Url> {
    pub(crate) async fn build(self) -> Result<Engine> {
        debug!(domain = self.storage.domain(), path = self.storage.path());

        let mut path = env::current_dir().inspect(|current_dir| debug!(?current_dir))?;

        if let Some(domain) = self.storage.domain() {
            path.push(domain);
        }

        if let Some(relative) = self.storage.path().strip_prefix("/") {
            path.push(relative);
        } else {
            path.push(self.storage.path());
        }

        debug!(?path);

        let db = turso::Builder::new_local(path.to_str().unwrap())
            .build()
            .await?;

        let connection = db.connect()?;

        for (name, ddl) in DDL.iter() {
            _ = connection
                .execute(ddl.as_str(), ())
                .await
                .inspect(|rows| debug!(name, rows))
                .inspect_err(|err| error!(name, ?err));
        }

        Ok(Engine {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            db: Arc::new(Mutex::new(db)),
            schemas: self.schemas,
            lake: self.lake,
        })
    }
}
