use std::{collections::BTreeMap, time::SystemTime};

use backoff::{ExponentialBackoffBuilder, future::retry};
use deadpool::managed;
use jansu_sans_io::{ApiKey, ApiVersionsRequest, RootMessageMeta};
use jansu_service::host_port;
use opentelemetry::KeyValue;
use tokio::{net::TcpStream, time::Duration};
use tracing::debug;
use url::Url;

use crate::{
    Client, Error,
    metrics::{TCP_CONNECT_DURATION, TCP_CONNECT_ERRORS},
};

///  Broker connection stream with [`correlation id`][`Header#variant.Request.field.correlation_id`]
#[derive(Debug)]
pub struct Connection {
    pub(crate) stream: TcpStream,
    pub(crate) correlation_id: i32,
}

/// Manager of supported API versions for a broker
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionManager {
    broker: Url,
    client_id: Option<String>,
    versions: BTreeMap<i16, i16>,
}

impl ConnectionManager {
    /// Build a manager with a broker endpoint
    pub fn builder(broker: Url) -> Builder {
        Builder::broker(broker)
    }

    /// Client id used in requests to the broker
    pub fn client_id(&self) -> Option<String> {
        self.client_id.clone()
    }

    /// The version supported by the broker for a given api key
    pub fn api_version(&self, api_key: i16) -> Result<i16, Error> {
        self.versions
            .get(&api_key)
            .copied()
            .ok_or(Error::UnknownApiKey(api_key))
    }
}

const INITIAL_CONNECTION_TIMEOUT_MILLIS: u64 = 30_000;

impl managed::Manager for ConnectionManager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        debug!(%self.broker);

        let attributes = [KeyValue::new("broker", self.broker.to_string())];
        let start = SystemTime::now();

        let addr = host_port(self.broker.clone()).await?;

        let backoff = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_millis(
                INITIAL_CONNECTION_TIMEOUT_MILLIS,
            )))
            .build();
        retry(backoff, || async {
            Ok(TcpStream::connect(addr)
                .await
                .inspect(|_| {
                    TCP_CONNECT_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &attributes,
                    )
                })
                .inspect_err(|err| {
                    debug!(broker = %self.broker, ?err, elapsed = start.elapsed().map_or(0, |duration| duration.as_millis() as u64));
                    TCP_CONNECT_ERRORS.add(1, &attributes);
                })
                .map(|stream| Connection {
                    stream,
                    correlation_id: 0,
                })?)
        })
        .await
        .map_err(Into::into)
    }

    async fn recycle(
        &self,
        obj: &mut Self::Type,
        metrics: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        debug!(?obj, ?metrics);

        Ok(())
    }
}

/// A managed [`Pool`] of broker [`Connection`]s
pub type Pool = managed::Pool<ConnectionManager>;

/// [Build][`Builder#method.build`] a [`Connection`] [`Pool`] to a [broker][`Builder#method.broker`]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    broker: Url,
    client_id: Option<String>,
}

impl Builder {
    /// Broker URL
    pub fn broker(broker: Url) -> Self {
        Self {
            broker,
            client_id: None,
        }
    }

    /// Client id used when making requests to the broker
    pub fn client_id(self, client_id: Option<String>) -> Self {
        Self { client_id, ..self }
    }

    /// Inquire with the broker supported api versions
    async fn bootstrap(&self) -> Result<BTreeMap<i16, i16>, Error> {
        // Create an interim pool to establish the API requests
        // and versions supported by the broker
        let versions = BTreeMap::from([(ApiVersionsRequest::KEY, 0)]);

        let req = ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()));

        let client = Pool::builder(ConnectionManager {
            broker: self.broker.clone(),
            client_id: self.client_id.clone(),
            versions,
        })
        .build()
        .map(Client::new)?;

        let supported = RootMessageMeta::messages().requests();

        client.call(req).await.map(|response| {
            response
                .api_keys
                .into_iter()
                .flatten()
                .filter_map(|api| {
                    supported.get(&api.api_key).and_then(|supported| {
                        if api.min_version >= supported.version.valid.start {
                            Some((
                                api.api_key,
                                api.max_version.min(supported.version.valid.end),
                            ))
                        } else {
                            None
                        }
                    })
                })
                .collect()
        })
    }

    /// Establish the API versions supported by the broker returning a [`Pool`]
    pub async fn build(self) -> Result<Pool, Error> {
        self.bootstrap().await.and_then(|versions| {
            Pool::builder(ConnectionManager {
                broker: self.broker,
                client_id: self.client_id,
                versions,
            })
            .build()
            .map_err(Into::into)
        })
    }
}
