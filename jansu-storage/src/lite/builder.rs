use super::*;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum CompactionMode {
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
            .unwrap_or(CompactionMode::Multi);

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
            .unwrap_or(CommunicationMode::Semaphore)
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
