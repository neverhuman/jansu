use std::time::SystemTime;

use bytes::Bytes;
use deadpool::managed::{Object, PoolError};
use jansu_sans_io::{Body, Frame, Header, Request};
use jansu_service::{FrameBytesLayer, FrameBytesService};
use opentelemetry::KeyValue;
use rama::{Context, Layer, Service};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};
use tracing::{Instrument, Level, debug, span};

use crate::{
    ConnectionManager, Error, Pool,
    metrics::{
        POOL_GET_DURATION, TCP_BYTES_RECEIVED, TCP_BYTES_SENT, TCP_RECEIVE_DURATION,
        TCP_RECEIVE_ERRORS, TCP_SEND_DURATION, TCP_SEND_ERRORS, status_update,
    },
};

/// Inject the [`Pool`][`Pool`] into the [`Service`] [`Context`] of this [`Layer`] using [`FramePoolService`]
#[derive(Clone, Debug)]
pub struct FramePoolLayer {
    pool: Pool,
}

impl FramePoolLayer {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for FramePoolLayer {
    type Service = FramePoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FramePoolService {
            pool: self.pool.clone(),
            inner,
        }
    }
}

/// Inject the [`Pool`][`Pool`] into the [`Service`] [`Context`] of the inner [`Service`]
#[derive(Clone, Debug)]
pub struct FramePoolService<S> {
    pool: Pool,
    inner: S,
}

impl<State, S> Service<State, Frame> for FramePoolService<S>
where
    S: Service<Pool, Frame, Response = Frame>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.pool.clone());
        self.inner.serve(ctx, req).await
    }
}

/// Inject the [`Pool`][`Pool`] into the [`Service`] [`Context`] of this [`Layer`] using [`RequestPoolService`]
#[derive(Clone, Debug)]
pub struct RequestPoolLayer {
    pool: Pool,
}

impl RequestPoolLayer {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for RequestPoolLayer {
    type Service = RequestPoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestPoolService {
            pool: self.pool.clone(),
            inner,
        }
    }
}

/// Inject the [`Pool`][`Pool`] into the [`Service`] [`Context`] of the inner [`Service`]
#[derive(Clone, Debug)]
pub struct RequestPoolService<S> {
    pool: Pool,
    inner: S,
}

impl<State, S, Q> Service<State, Q> for RequestPoolService<S>
where
    Q: Request,
    S: Service<Pool, Q>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    /// serve the request, injecting the pool into the context of the inner service
    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.pool.clone());
        self.inner.serve(ctx, req).await
    }
}

/// API client using a [`Connection`] [`Pool`]
#[derive(Clone, Debug)]
pub struct Client {
    service:
        RequestPoolService<RequestConnectionService<FrameBytesService<BytesConnectionService>>>,
}

impl Client {
    /// Create a new client using the supplied pool
    pub fn new(pool: Pool) -> Self {
        let service = (
            RequestPoolLayer::new(pool),
            RequestConnectionLayer,
            FrameBytesLayer,
        )
            .into_layer(BytesConnectionService);

        Self { service }
    }

    /// Make an API request using the connection from the pool
    pub async fn call<Q>(&self, req: Q) -> Result<Q::Response, Error>
    where
        Q: Request,
        Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
    {
        self.service.serve(Context::default(), req).await
    }
}

/// A [`Layer`] that takes a [`Connection`] from the [`Pool`] calling an inner [`Service`] with that [`Connection`] as [`Context`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionLayer;

impl<S> Layer<S> for FrameConnectionLayer {
    type Service = FrameConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that takes a [`Connection`] from the [`Pool`] calling an inner [`Service`] with that [`Connection`] as [`Context`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionService<S> {
    inner: S,
}

impl<S> Service<Pool, Frame> for FrameConnectionService<S>
where
    S: Service<Object<ConnectionManager>, Frame, Response = Frame>,
    S::Error: From<Error> + From<PoolError<Error>> + From<jansu_sans_io::Error>,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<Pool>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let api_key = req.api_key()?;
        let api_version = req.api_version()?;
        let client_id = req
            .client_id()
            .map(|client_id| client_id.map(|client_id| client_id.to_string()))?;

        let pool = ctx.state();
        status_update(pool);

        let connection = {
            let start = SystemTime::now();
            pool.get().await.inspect(|_| {
                POOL_GET_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[],
                );
            })?
        };

        let correlation_id = connection.correlation_id;

        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            body: req.body,
        };

        let (ctx, _) = ctx.swap_state(connection);

        self.inner.serve(ctx, frame).await
    }
}

/// A [`Layer`] of [`RequestConnectionService`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionLayer;

impl<S> Layer<S> for RequestConnectionLayer {
    type Service = RequestConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// Take a [`Connection`] from the [`Pool`]. Enclose the [`Request`]
/// in a [`Frame`] using latest API version supported by the broker. Call the
/// inner service with the [`Frame`] using the [`Connection`] as [`Context`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionService<S> {
    inner: S,
}

impl<Q, S> Service<Pool, Q> for RequestConnectionService<S>
where
    Q: Request,
    S: Service<Object<ConnectionManager>, Frame, Response = Frame>,
    S::Error: From<Error>
        + From<PoolError<Error>>
        + From<jansu_sans_io::Error>
        + From<<Q::Response as TryFrom<Body>>::Error>,
{
    type Response = Q::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<Pool>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        let pool = ctx.state();
        status_update(pool);

        let api_key = Q::KEY;
        let api_version = pool.manager().api_version(api_key)?;
        let client_id = pool.manager().client_id();
        let connection = {
            let start = SystemTime::now();
            pool.get().await.inspect(|_| {
                POOL_GET_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[],
                );
            })?
        };

        let correlation_id = connection.correlation_id;

        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            body: req.into(),
        };

        let (ctx, _) = ctx.swap_state(connection);

        let frame = self.inner.serve(ctx, frame).await?;

        Q::Response::try_from(frame.body)
            .inspect(|response| debug!(?response))
            .map_err(Into::into)
    }
}

/// A [`Service`] that writes a frame represented by [`Bytes`] to a [`Connection`] [`Context`], returning the [`Bytes`] frame response.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesConnectionService;

impl BytesConnectionService {
    async fn write(
        &self,
        stream: &mut TcpStream,
        frame: Bytes,
        attributes: &[KeyValue],
    ) -> Result<(), Error> {
        debug!(frame = ?&frame[..]);

        let start = SystemTime::now();

        stream
            .write_all(&frame[..])
            .await
            .inspect(|_| {
                TCP_SEND_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes,
                );

                TCP_BYTES_SENT.add(frame.len() as u64, attributes);
            })
            .inspect_err(|_| {
                TCP_SEND_ERRORS.add(1, attributes);
            })
            .map_err(Into::into)
    }

    async fn read(&self, stream: &mut TcpStream, attributes: &[KeyValue]) -> Result<Bytes, Error> {
        let start = SystemTime::now();

        let mut size = [0u8; 4];
        _ = stream.read_exact(&mut size).await?;

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = stream
            .read_exact(&mut buffer[4..])
            .await
            .inspect(|_| {
                TCP_RECEIVE_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes,
                );

                TCP_BYTES_RECEIVED.add(buffer.len() as u64, attributes);
            })
            .inspect_err(|_| {
                TCP_RECEIVE_ERRORS.add(1, attributes);
            })?;

        Ok(Bytes::from(buffer)).inspect(|frame| debug!(frame = ?&frame[..]))
    }
}

impl Service<Object<ConnectionManager>, Bytes> for BytesConnectionService {
    type Response = Bytes;
    type Error = Error;

    async fn serve(
        &self,
        mut ctx: Context<Object<ConnectionManager>>,
        req: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let c = ctx.state_mut();

        let local = c.stream.local_addr()?;
        let peer = c.stream.peer_addr()?;

        let attributes = [KeyValue::new("peer", peer.to_string())];

        let span = span!(Level::DEBUG, "client", local = %local, peer = %peer);

        async move {
            self.write(&mut c.stream, req, &attributes).await?;

            c.correlation_id += 1;

            self.read(&mut c.stream, &attributes).await
        }
        .instrument(span)
        .await
    }
}

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}
