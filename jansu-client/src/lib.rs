// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Jansu Client
//!
//! Jansu API client.
//!
//! # Simple [`Request`] client
//!
//! ```no_run
//! use jansu_client::{Client, ConnectionManager, Error};
//! use jansu_sans_io::MetadataRequest;
//! use rama::{Service as _, Context};
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let origin = ConnectionManager::builder(Url::parse("tcp://localhost:9092")?)
//!     .client_id(Some(env!("CARGO_PKG_NAME").into()))
//!     .build()
//!     .await
//!     .map(Client::new)?;
//!
//! let response = origin
//!     .call(
//!         MetadataRequest::default()
//!             .topics(Some([].into()))
//!             .allow_auto_topic_creation(Some(false))
//!             .include_cluster_authorized_operations(Some(false))
//!             .include_topic_authorized_operations(Some(false)),
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Proxy: [`Layer`] Composition
//!
//! An example API proxy listening for requests on `tcp://localhost:9092` that
//! forwards each [`Frame`] to an origin broker on `tcp://example.com:9092`:
//!
//! ```no_run
//! use rama::{Context, Layer as _, Service as _};
//! use jansu_client::{
//!     BytesConnectionService, ConnectionManager, Error, FrameConnectionLayer,
//!     FramePoolLayer,
//! };
//! use jansu_service::{
//!     BytesFrameLayer, FrameBytesLayer, TcpBytesLayer, TcpContextLayer, TcpListenerLayer,
//!     host_port,
//! };
//! use tokio::net::TcpListener;
//! use tokio_util::sync::CancellationToken;
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! // forward protocol frames to the origin using a connection pool:
//! let origin = ConnectionManager::builder(Url::parse("tcp://example.com:9092")?)
//!     .client_id(Some(env!("CARGO_PKG_NAME").into()))
//!     .build()
//!     .await?;
//!
//! // a tcp listener used by the proxy
//! let listener =
//!     TcpListener::bind(host_port(Url::parse("tcp://localhost:9092")?).await?).await?;
//!
//! // listen for requests until cancelled
//! let token = CancellationToken::new();
//!
//! let stack = (
//!     // server layers: reading tcp -> bytes -> frames:
//!     TcpListenerLayer::new(token),
//!     TcpContextLayer::default(),
//!     TcpBytesLayer::<()>::default(),
//!     BytesFrameLayer::default(),
//!
//!     // client layers: writing frames -> connection pool -> bytes -> origin:
//!     FramePoolLayer::new(origin),
//!     FrameConnectionLayer,
//!     FrameBytesLayer,
//! )
//!     .into_layer(BytesConnectionService);
//!
//! stack.serve(Context::default(), listener).await?;
//!
//! # Ok(())
//! # }
//! ```

mod connection;
mod error;
mod metrics;
mod service;

pub use connection::{Builder, Connection, ConnectionManager, Pool};
pub use error::Error;
pub use service::{
    BytesConnectionService, Client, FrameConnectionLayer, FrameConnectionService, FramePoolLayer,
    FramePoolService, RequestConnectionLayer, RequestConnectionService, RequestPoolLayer,
    RequestPoolService,
};

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use jansu_sans_io::{MetadataRequest, MetadataResponse};
    use jansu_service::{
        BytesFrameLayer, FrameBytesLayer, FrameRouteService, RequestLayer, ResponseService,
        TcpBytesLayer, TcpContextLayer, TcpListenerLayer,
    };
    use rama::{Context, Layer as _, Service as _};
    use tokio::{net::TcpListener, task::JoinSet};
    use tokio_util::sync::CancellationToken;
    use tracing::debug;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;
    use url::Url;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard, Error> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    async fn server(cancellation: CancellationToken, listener: TcpListener) -> Result<(), Error> {
        let server = (
            TcpListenerLayer::new(cancellation),
            TcpContextLayer::default(),
            TcpBytesLayer::default(),
            BytesFrameLayer::default(),
        )
            .into_layer(
                FrameRouteService::builder()
                    .with_service(RequestLayer::<MetadataRequest>::new().into_layer(
                        ResponseService::new(|_ctx: Context<()>, _req: MetadataRequest| {
                            Ok::<_, Error>(
                                MetadataResponse::default()
                                    .brokers(Some([].into()))
                                    .topics(Some([].into()))
                                    .cluster_id(Some("abc".into()))
                                    .controller_id(Some(111))
                                    .throttle_time_ms(Some(0))
                                    .cluster_authorized_operations(Some(-1)),
                            )
                        }),
                    ))
                    .and_then(|builder| builder.build())?,
            );

        server.serve(Context::default(), listener).await
    }

    #[tokio::test]
    async fn tcp_client_server() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let cancellation = CancellationToken::new();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;

        let mut join = JoinSet::new();

        let _server = {
            let cancellation = cancellation.clone();
            join.spawn(async move { server(cancellation, listener).await })
        };

        let origin = (
            RequestPoolLayer::new(
                ConnectionManager::builder(
                    Url::parse(&format!("tcp://{local_addr}")).inspect(|url| debug!(%url))?,
                )
                .client_id(Some(env!("CARGO_PKG_NAME").into()))
                .build()
                .await
                .inspect(|pool| debug!(?pool))?,
            ),
            RequestConnectionLayer,
            FrameBytesLayer,
        )
            .into_layer(BytesConnectionService);

        let response = origin
            .serve(
                Context::default(),
                MetadataRequest::default()
                    .topics(Some([].into()))
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false)),
            )
            .await?;

        assert_eq!(Some("abc"), response.cluster_id.as_deref());
        assert_eq!(Some(111), response.controller_id);

        cancellation.cancel();

        let joined = join.join_all().await;
        debug!(?joined);

        Ok(())
    }
}
