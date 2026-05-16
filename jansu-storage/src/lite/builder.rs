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

use std::{
    collections::BTreeMap,
    env,
    path::PathBuf,
    result,
    str::FromStr,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use rama::{Context, Layer as _, Service as _};
use regex::Regex;
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use url::Url;

use jansu_schema::{Registry, lake::House};

use crate::{
    ChannelRequestLayer, Error, RequestChannelService, RequestStorageService, Result, Storage,
    bounded_channel,
    proxy::SemaphoreProxy,
    sql::{Cache, remove_comments},
};

use super::{ConnectionManager, Delegate, Engine, Pool};

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

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

pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
    Cache::new(
        crate::sql::SQL
            .iter()
            .map(|(name, sql)| fix_parameters(sql).map(|sql| (*name, sql)))
            .collect::<Result<BTreeMap<_, _>>>()
            .unwrap_or_default(),
    )
});

pub(super) fn fix_parameters(sql: &str) -> Result<String> {
    Regex::new(r"\$(?<i>\d+)")
        .map(|re| re.replace_all(sql, "?$i").into_owned())
        .map_err(Into::into)
}

#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, D> {
    cluster: C,
    node: N,
    advertised_listener: L,
    storage: D,
    schemas: Option<Registry>,
    lake: Option<House>,
    cancellation: CancellationToken,
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
            cancellation: self.cancellation,
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
            cancellation: self.cancellation,
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
            cancellation: self.cancellation,
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
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, D> {
        Self { schemas, ..self }
    }

    pub(crate) fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }

    pub(crate) fn cancellation(self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum CompactionMode {
    Single,
    #[default]
    Multi,
}

impl FromStr for CompactionMode {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "single" => Ok(Self::Single),
            "multi" => Ok(Self::Multi),
            otherwise => Err(Error::Message(otherwise.to_owned())),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum CommunicationMode {
    Mpsc,
    Direct,
    #[default]
    Semaphore,
}

impl FromStr for CommunicationMode {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "direct" => Ok(Self::Direct),
            "mpsc" => Ok(Self::Mpsc),
            "semaphore" => Ok(Self::Semaphore),
            otherwise => Err(Error::Message(otherwise.to_owned())),
        }
    }
}

impl Builder<String, i32, Url, Url> {
    pub(crate) async fn build(self) -> Result<Arc<Box<dyn Storage>>> {
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

        let vacuum_into = self.storage.query_pairs().find_map(|(k, v)| {
            if k == "vacuum_into" {
                Some(PathBuf::from(v.as_ref()))
            } else {
                None
            }
        });

        let busy_timeout = self
            .storage
            .query_pairs()
            .find_map(|(k, v)| {
                if k == "busy_timeout" {
                    human_units::Duration::from_str(v.as_ref())
                        .map(|duration| duration.0)
                        .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                        .ok()
                } else {
                    None
                }
            })
            .unwrap_or(Duration::from_secs(5));

        let compaction = self
            .storage
            .query_pairs()
            .find_map(|(k, v)| {
                if k == "compaction" {
                    CompactionMode::from_str(v.as_ref()).ok()
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let db = libsql::Builder::new_local(path).build().await?;

        {
            let connection = db.connect()?;

            for (name, ddl) in DDL.iter() {
                _ = connection
                    .execute(ddl.as_str(), ())
                    .await
                    .inspect(|rows| debug!(name, rows))
                    .inspect_err(|err| error!(name, ?err));
            }
        }

        match self
            .storage
            .query_pairs()
            .find_map(|(k, v)| {
                if k == "mode" {
                    CommunicationMode::from_str(v.as_ref()).ok()
                } else {
                    None
                }
            })
            .unwrap_or_default()
        {
            CommunicationMode::Mpsc => {
                let (sender, receiver) = bounded_channel(1);
                let mut server = JoinSet::new();

                let _ = {
                    let cancellation = self.cancellation.clone();

                    let storage = Delegate {
                        cluster: self.cluster,
                        node: self.node,
                        advertised_listener: self.advertised_listener,
                        pool: Pool::builder(ConnectionManager {
                            db: Arc::new(Mutex::new(db)),
                            busy_timeout,
                        })
                        .build()?,
                        schemas: self.schemas,
                        lake: self.lake,
                        vacuum_into,
                        maintenance: Arc::new(Semaphore::new(1)),
                        compaction,
                    };

                    server.spawn(async move {
                        let server = ChannelRequestLayer::new(cancellation)
                            .into_layer(RequestStorageService::new(storage));

                        server.serve(Context::default(), receiver).await
                    })
                };

                let inner = RequestChannelService::new(sender);

                Ok(Arc::new(Box::new(Engine {
                    server: Arc::new(server),
                    inner,
                }) as Box<dyn Storage>))
            }

            CommunicationMode::Direct => Ok(Arc::new(Box::new(Delegate {
                cluster: self.cluster,
                node: self.node,
                advertised_listener: self.advertised_listener,
                pool: Pool::builder(ConnectionManager {
                    db: Arc::new(Mutex::new(db)),
                    busy_timeout,
                })
                .build()?,
                schemas: self.schemas,
                lake: self.lake,
                vacuum_into,
                maintenance: Arc::new(Semaphore::new(1)),
                compaction,
            }) as Box<dyn Storage>)),

            CommunicationMode::Semaphore => Ok(Arc::new(Box::new(SemaphoreProxy::new(Delegate {
                cluster: self.cluster,
                node: self.node,
                advertised_listener: self.advertised_listener,
                pool: Pool::builder(ConnectionManager {
                    db: Arc::new(Mutex::new(db)),
                    busy_timeout,
                })
                .build()?,
                schemas: self.schemas,
                lake: self.lake,
                vacuum_into,
                maintenance: Arc::new(Semaphore::new(1)),
                compaction,
            })) as Box<dyn Storage>)),
        }
    }
}
