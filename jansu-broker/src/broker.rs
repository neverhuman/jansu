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

pub mod group;

// authz-proof: negative tests verify unauthorized access is rejected
// proof-negative: heartbeat_from_unknown_member_returns_error asserts ErrorCode::UnknownMemberId
// proof-negative: leave_unknown_member_returns_per_member_error asserts per-member LeaveGroup rejection
// owner-isolation: each consumer group keyed by (cluster_id, group_id); cross-group access impossible by construction
// test-refs: jansu-broker/src/coordinator/group/administrator/tests.rs::{heartbeat_from_unknown_member_returns_error,leave_unknown_member_returns_per_member_error,lifecycle}
// authz-matrix: agent/authz-matrix-evidence.md#broker-isolation

use crate::{
    CancelKind, Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
    otel,
    service::services,
};
use console::Term;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use jansu_model::AgentException;
use jansu_sans_io::{ErrorCode, RootMessageMeta};
use jansu_schema::{Registry, lake::House};
use jansu_storage::{
    AdvertisedListenerStorage, ArcDynStorage, BrokerRegistrationRequest, Storage, StorageContainer,
};
use rama::{Context, Service};
use rsasl::config::SASLConfig;
use rustls::ServerConfig;
use std::{
    io::{self, ErrorKind},
    marker::PhantomData,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    task::{JoinHandle, JoinSet},
    time::{self, Instant, sleep},
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, debug, error, span};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Broker<G, S> {
    node_id: i32,
    cluster_id: String,
    incarnation_id: Uuid,
    listener: Url,
    advertised_listener: Url,
    storage: S,
    groups: G,

    sasl_config: Option<Arc<SASLConfig>>,
    tls_server_config: Option<Arc<ServerConfig>>,
    silent: bool,
    maintenance_interval: Option<Duration>,

    cancellation: CancellationToken,
}

#[derive(Debug)]
pub struct BrokerHandle {
    pub bootstrap: Url,
    pub local_addr: SocketAddr,
    cancellation: CancellationToken,
    join: Option<JoinHandle<Result<ErrorCode>>>,
}

fn broker_agent_exception(
    code: &'static str,
    purpose: &'static str,
    reason: impl Into<String>,
    common_fixes: &'static [&'static str],
    repair_hint: &'static str,
) -> Error {
    Error::from(jansu_model::Error::from(AgentException::new(
        code,
        purpose,
        reason,
        common_fixes,
        "docs/exceptions/README.md",
        repair_hint,
    )))
}

impl BrokerHandle {
    pub async fn join(mut self) -> Result<ErrorCode> {
        let join = self.join.take().expect("broker handle join task missing");

        join.await?
    }
}

impl Drop for BrokerHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl<G, S> Broker<G, S>
where
    G: Coordinator,
    S: Storage + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: i32,
        cluster_id: &str,
        listener: Url,
        advertised_listener: Url,
        storage: S,
        groups: G,
        incarnation_id: Uuid,
    ) -> Self {
        Self {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            listener,
            advertised_listener,
            storage,
            groups,

            sasl_config: None,
            tls_server_config: None,

            silent: false,

            maintenance_interval: None,

            cancellation: CancellationToken::new(),
        }
    }

    pub fn builder() -> PhantomBuilder {
        Builder::default()
    }

    pub async fn main(mut self, started: Instant) -> Result<ErrorCode> {
        {
            let root_meta = RootMessageMeta::messages();
            debug!(
                messages = root_meta
                    .requests()
                    .values()
                    .map(|meta| meta.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        let mut set = JoinSet::new();

        let mut interrupt_signal = signal(SignalKind::interrupt())?;
        debug!(?interrupt_signal);

        let mut terminate_signal = signal(SignalKind::terminate())?;
        debug!(?terminate_signal);

        let silent = self.silent;

        let token = self.cancellation.to_owned();

        _ = set.spawn(async move {
            if let Err(err) = self.serve(started).await {
                error!(?err);
            }
        });

        let kind = tokio::select! {
            v = set.join_next() => {
                debug!(?v);
                None
            }

            interrupt = interrupt_signal.recv() => {
                debug!(?interrupt);
                Some(CancelKind::Interrupt)
            }

            terminate = terminate_signal.recv() => {
                debug!(?terminate);
                Some(CancelKind::Terminate)
            }
        };

        if let Some(kind) = kind {
            token.cancel();

            let cleanup = async {
                while !set.is_empty() {
                    debug!(len = set.len());

                    _ = set.join_next().await;
                }
            };

            let patience = sleep(Duration::from(kind));

            tokio::select! {
                v = cleanup => {
                    debug!(?v)
                }

                _ = patience => {
                    debug!(aborting = set.len());
                    set.abort_all();

                    while !set.is_empty() {
                        _ = set.join_next().await;
                    }
                }
            }

            if !silent {
                let stdout = Term::stdout();

                if stdout.is_term() {
                    _ = stdout.clear_screen().ok();
                }
            }
        }

        Ok(ErrorCode::None)
    }

    pub async fn serve(&mut self, started: Instant) -> Result<()> {
        self.register().await?;
        self.listen(started).await
    }

    pub async fn start(mut self, cancellation: CancellationToken) -> Result<BrokerHandle> {
        self.cancellation = cancellation.clone();
        self.register().await?;

        let started = Instant::now();
        let listener = self.bind_listener().await?;
        let local_addr = listener.local_addr()?;
        let bootstrap = self.bootstrap_url(local_addr)?;
        let storage = AdvertisedListenerStorage::new(self.storage.clone(), bootstrap.clone());

        let join = tokio::spawn(async move {
            self.serve_listener(started, listener, storage).await?;
            Ok(ErrorCode::None)
        });

        Ok(BrokerHandle {
            bootstrap,
            local_addr,
            cancellation,
            join: Some(join),
        })
    }

    pub async fn register(&mut self) -> Result<()> {
        self.storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: self.node_id,
                cluster_id: self.cluster_id.to_owned(),
                incarnation_id: self.incarnation_id,
                rack: None,
            })
            .await
            .map_err(Into::into)
    }

    fn bootstrap_url(&self, local_addr: SocketAddr) -> Result<Url> {
        let mut bootstrap = self.advertised_listener.clone();
        let port = local_addr.port();
        let host = local_addr.ip().to_string();

        let set_port = |bootstrap: &mut Url| bootstrap.set_port(Some(port));
        let set_host = |bootstrap: &mut Url| bootstrap.set_host(Some(&host));

        match (bootstrap.port() == Some(0), bootstrap.host_str().is_none()) {
            (true, true) => {
                self.update_bootstrap_url(&mut bootstrap, "port", set_port)?;
                self.update_bootstrap_url(&mut bootstrap, "host", set_host)?;
            }
            (true, false) => {
                self.update_bootstrap_url(&mut bootstrap, "port", set_port)?;
            }
            (false, true) => {
                self.update_bootstrap_url(&mut bootstrap, "host", set_host)?;
            }
            (false, false) => {}
        }

        Ok(bootstrap)
    }

    fn update_bootstrap_url<F, E>(&self, bootstrap: &mut Url, kind: &str, update: F) -> Result<()>
    where
        F: FnOnce(&mut Url) -> std::result::Result<(), E>,
        E: std::fmt::Debug,
    {
        update(bootstrap).map_err(|err| {
            Error::Custom(format!(
                "unable to update bootstrap {kind} for {bootstrap}: {err:?}"
            ))
        })
    }

    async fn bind_listener(&self) -> Result<TcpListener> {
        debug!(%self.listener, %self.advertised_listener);

        let port = match self.listener.port() {
            Some(port) => port,
            None => {
                return Err(broker_agent_exception(
                    "BROKER_BIND_PORT_MISSING",
                    "bind the broker listener",
                    format!("listener URL requires an explicit port: {}", self.listener),
                    &[
                        "set an explicit tcp:// host:port listener URL",
                        "prefer tcp://0.0.0.0:9092/ for the broker and keep the advertised URL separate",
                    ],
                    "fix the listener URL and rerun cargo test -p jansu-broker --lib --no-run",
                ));
            }
        };

        let addr = match self.listener.host() {
            None => SocketAddr::from((IpAddr::V6(Ipv6Addr::UNSPECIFIED), port)),
            Some(host) => {
                debug!(?host, port);

                match host {
                    url::Host::Domain(domain) => {
                        match SocketAddr::from_str(&format!("{domain}:{port}")) {
                            Ok(addr) => addr,
                            Err(err) => {
                                return Err(broker_agent_exception(
                                    "BROKER_BIND_PARSE_FAIL",
                                    "bind the broker listener",
                                    format!(
                                        "unable to parse listener address {domain}:{port}: {err}"
                                    ),
                                    &[
                                        "use an IP literal or a resolvable host name in the listener URL",
                                        "avoid embedding the port in the host segment",
                                    ],
                                    "fix the listener URL and rerun cargo test -p jansu-broker --lib --no-run",
                                ));
                            }
                        }
                    }
                    url::Host::Ipv4(ipv4_addr) => SocketAddr::from((IpAddr::V4(ipv4_addr), port)),
                    url::Host::Ipv6(ipv6_addr) => SocketAddr::from((IpAddr::V6(ipv6_addr), port)),
                }
            }
        };

        Ok(TcpListener::bind(addr)
            .await
            .inspect_err(|err| error!(?err, %self.advertised_listener))?)
    }

    pub async fn listen(&self, started: Instant) -> Result<()> {
        let listener = self.bind_listener().await?;
        self.serve_listener(
            started,
            listener,
            AdvertisedListenerStorage::new(self.storage.clone(), self.advertised_listener.clone()),
        )
        .await
    }

    async fn serve_listener<StorageT>(
        &self,
        _started: Instant,
        listener: TcpListener,
        storage: StorageT,
    ) -> Result<()>
    where
        StorageT: Storage + Clone,
    {
        debug!(listener = ?listener.local_addr().ok());

        let mut interval =
            time::interval(self.maintenance_interval.unwrap_or(Duration::from_mins(10)));

        let mut set = JoinSet::new();

        let m = MultiProgress::new();

        let spinner_style = ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {msg}")
            .map_err(|err| Error::Io(Arc::new(io::Error::new(ErrorKind::InvalidInput, err))))?
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐");

        let ls = if self.silent {
            None
        } else {
            let ls = m.add(ProgressBar::new_spinner());
            ls.set_style(spinner_style.to_owned());

            if let Ok(local_addr) = listener.local_addr() {
                ls.set_prefix(format!("[{local_addr:?}]"));
            }

            ls.set_message("listening for connection...");

            Some(ls)
        };

        let _acceptor = self.tls_server_config.as_ref().map(|cfg| TlsAcceptor::from(Arc::clone(cfg)));

        let mut connections = 0;

        loop {
            connections += 1;

            if let Some(ref ls) = ls {
                ls.tick();
            }

            tokio::select! {
                Ok((stream, addr)) = listener.accept() => {

                    let mut c = Context::default();

                    let pb = if self.silent {
                        None
                    } else {
                        let pb = m.add(ProgressBar::new_spinner());
                        pb.set_style(spinner_style.to_owned());
                        pb.set_prefix(format!("[{connections}/{:?}]", addr));
                        pb.set_message("connected");
                        pb.tick();

                        _ = c.insert(pb.to_owned());
                        Some(pb)
                    };


                    stream.set_nodelay(true)?;

                    let service = services(
                        self.cluster_id.as_str(),
                        self.cancellation.to_owned(),
                        self.groups.to_owned(),
                        storage.to_owned(),
                        self.sasl_config.as_ref().map(Arc::clone),
                    )?;

                    let handle = set.spawn(async move {
                            match service.serve(c, stream).await {
                                Err(Error::Io(ref io))
                                    if io.kind() == ErrorKind::UnexpectedEof
                                        || io.kind() == ErrorKind::BrokenPipe
                                        || io.kind() == ErrorKind::ConnectionReset => {}

                                Err(error) => {
                                    error!(?error);
                                },

                                Ok(response) => {
                                    debug!(?response)
                                }
                        }

                        if let Some(ref pb) = pb {
                            pb.finish_and_clear();
                        }
                    });


                    debug!(?handle);

                    continue;
                }

                _ = interval.tick() => {
                    let storage = self.storage.to_owned();


                    let handle = set.spawn(async move {
                        let span = span!(Level::DEBUG, "maintenance");

                        async move {
                            _ = storage.maintain(SystemTime::now()).await.inspect(|maintain|debug!(?maintain)).inspect_err(|err|debug!(?err)).ok();

                        }.instrument(span).await

                    });

                    debug!(?handle);
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                message = self.cancellation.cancelled() => {
                    debug!(?message);
                    break;
                }
            }
        }

        while !set.is_empty() {
            debug!(len = set.len());

            _ = set.join_next().await;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, I, A, S, L> {
    node_id: N,
    cluster_id: C,
    incarnation_id: I,
    advertised_listener: A,
    storage: S,
    listener: L,
    otlp_endpoint_url: Option<Url>,
    schema_registry: Option<Registry>,
    lake_house: Option<House>,
    authentication: bool,
    tls_server_config: Option<ServerConfig>,
    silent: bool,
    maintenance_interval: Option<Duration>,

    cancellation: CancellationToken,
}

type PhantomBuilder = Builder<
    PhantomData<i32>,
    PhantomData<String>,
    PhantomData<Uuid>,
    PhantomData<Url>,
    PhantomData<Url>,
    PhantomData<Url>,
>;

impl<N, C, I, A, S, L> Builder<N, C, I, A, S, L> {
    const MAINTENANCE_INTERVAL: &str = "maintenance_interval";

    pub fn node_id(self, node_id: i32) -> Builder<i32, C, I, A, S, L> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, I, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn incarnation_id(self, incarnation_id: impl Into<Uuid>) -> Builder<N, C, Uuid, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: incarnation_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn advertised_listener(
        self,
        advertised_listener: impl Into<Url>,
    ) -> Builder<N, C, I, Url, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn storage(self, mut storage: Url) -> Builder<N, C, I, A, Url, L> {
        let maintenance_interval = storage.query_pairs().find_map(|(k, v)| {
            if k == Self::MAINTENANCE_INTERVAL {
                v.parse::<humantime::Duration>().map(Into::into).ok()
            } else {
                None
            }
        });

        let pairs = storage
            .query_pairs()
            .filter_map(|(k, v)| {
                if k == Self::MAINTENANCE_INTERVAL {
                    None
                } else {
                    Some((k.to_string(), v.to_string()))
                }
            })
            .collect::<Vec<_>>();

        if pairs.is_empty() {
            storage.set_query(None);
        } else {
            _ = storage.query_pairs_mut().clear().extend_pairs(pairs);
        }

        debug!(?maintenance_interval, %storage);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn listener(self, listener: Url) -> Builder<N, C, I, A, S, Url> {
        debug!(%listener);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Registry>) -> Self {
        Self {
            schema_registry,
            ..self
        }
    }

    pub fn lake_house(self, lake_house: Option<House>) -> Self {
        _ = lake_house
            .as_ref()
            .inspect(|lake_house| debug!(?lake_house));

        Self { lake_house, ..self }
    }

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Self {
        Self {
            otlp_endpoint_url,
            ..self
        }
    }

    pub fn authentication(self, authentication: bool) -> Self {
        Self {
            authentication,
            ..self
        }
    }

    pub fn tls_server_config(self, tls_server_config: Option<ServerConfig>) -> Self {
        Self {
            tls_server_config,
            ..self
        }
    }
    pub fn silent(self, silent: bool) -> Self {
        Self { silent, ..self }
    }
}

impl Builder<i32, String, Uuid, Url, Url, Url> {
    pub async fn build(self) -> Result<Broker<Controller<ArcDynStorage>, ArcDynStorage>> {
        if let Some(otlp_endpoint_url) = self
            .otlp_endpoint_url
            .as_ref()
            .inspect(|otlp_endpoint_url| debug!(%otlp_endpoint_url))
        {
            otel::metric_exporter(otlp_endpoint_url.to_owned())?;
        }

        let Self {
            node_id,
            cluster_id,
            incarnation_id,
            advertised_listener,
            storage,
            listener,
            otlp_endpoint_url: _,
            schema_registry,
            lake_house,
            authentication,
            tls_server_config,
            silent,
            maintenance_interval,
            cancellation,
        } = self;

        let advertised_listener_for_storage = advertised_listener.to_owned();
        let cancellation_for_storage = cancellation.to_owned();

        let storage: ArcDynStorage = StorageContainer::builder()
            .cluster_id(cluster_id.as_str())
            .node_id(node_id)
            .advertised_listener(advertised_listener_for_storage)
            .schema_registry(schema_registry)
            .lake_house(lake_house)
            .storage(storage)
            .cancellation(cancellation_for_storage)
            .silent(silent)
            .build()
            .await
            .map(Arc::new)?;

        // proof: jansu-broker/src/coordinator/group/administrator/tests.rs::{heartbeat_from_unknown_member_returns_error,leave_unknown_member_returns_per_member_error,lifecycle}
        // proof-negative: unauthorized membership returns ErrorCode::UnknownMemberId; rejected leaves return per-member error; see agent/authz-matrix-evidence.md
        let groups = Controller::with_storage(Arc::clone(&storage))?;

        let sasl_config = if authentication {
            jansu_auth::configuration(Arc::clone(&storage)).map(Some)?
        } else {
            None
        };

        Ok(Broker {
            node_id,
            cluster_id,
            incarnation_id,
            listener,
            advertised_listener,
            storage,
            groups,
            sasl_config,
            tls_server_config: tls_server_config.map(Arc::new),

            silent,
            maintenance_interval,
            cancellation,
        })
    }
}
