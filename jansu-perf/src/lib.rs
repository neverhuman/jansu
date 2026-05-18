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

use core::{
    fmt::{self, Debug, Display},
    result,
};
use std::{
    io,
    marker::PhantomData,
    num::NonZeroU32,
    pin::Pin,
    sync::{Arc, LazyLock, PoisonError},
    time::Duration,
};

use bytes::Bytes;
use governor::{InsufficientCapacity, Quota, RateLimiter};
use jansu_client::{Client, ConnectionManager};
use jansu_sans_io::ErrorCode;
use nonzero_ext::nonzero;
use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_sdk::{error::OTelSdkError, metrics::SdkMeterProvider};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use tokio::{
    signal::unix::{SignalKind, signal},
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use url::Url;

mod exporter;
mod observation;
mod producer;

#[cfg(test)]
mod tests;

use exporter::MetricExporter;
use producer::Producer;

pub type Result<T, E = Error> = result::Result<T, E>;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Client(#[from] jansu_client::Error),
    ExporterBuild(#[from] ExporterBuildError),
    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),
    OtelSdk(#[from] OTelSdkError),
    Random(#[from] getrandom::Error),
    Poison,
    Protocol(#[from] jansu_sans_io::Error),
    UnknownHost(String),
    Url(#[from] url::ParseError),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancelKind {
    Interrupt,
    Terminate,
    Timeout,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Perf {
    broker: Url,
    topic: String,
    partition: i32,
    batch_size: u32,
    record_size: usize,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct Builder<B, T> {
    broker: B,
    topic: T,
    partition: i32,
    batch_size: u32,
    record_size: usize,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
}

impl Default for Builder<PhantomData<Url>, PhantomData<String>> {
    fn default() -> Self {
        Self {
            broker: Default::default(),
            topic: Default::default(),
            partition: Default::default(),
            batch_size: 1,
            record_size: 1024,
            per_second: None,
            throughput: None,
            producers: 1,
            duration: None,
        }
    }
}

impl<B, T> Builder<B, T> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T> {
        Self { partition, ..self }
    }

    pub fn batch_size(self, batch_size: u32) -> Self {
        Self { batch_size, ..self }
    }

    pub fn record_size(self, record_size: usize) -> Self {
        Self {
            record_size,
            ..self
        }
    }

    pub fn per_second(self, per_second: Option<u32>) -> Self {
        Self { per_second, ..self }
    }

    pub fn throughput(self, throughput: Option<u32>) -> Self {
        Self { throughput, ..self }
    }

    pub fn producers(self, producers: u32) -> Self {
        Self { producers, ..self }
    }

    pub fn duration(self, duration: Option<Duration>) -> Self {
        Self { duration, ..self }
    }
}

impl Builder<Url, String> {
    pub fn build(self) -> Perf {
        Perf {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
        }
    }
}

impl Perf {
    pub fn builder() -> Builder<PhantomData<Url>, PhantomData<String>> {
        Builder::default()
    }

    pub async fn main(self) -> Result<ErrorCode> {
        let token = CancellationToken::new();

        let meter_provider = {
            let exporter = MetricExporter::new(token.clone());
            let meter_provider = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .build();
            global::set_meter_provider(meter_provider.clone());

            meter_provider
        };

        let mut interrupt_signal = signal(SignalKind::interrupt())?;
        debug!(?interrupt_signal);

        let mut terminate_signal = signal(SignalKind::terminate())?;
        debug!(?terminate_signal);

        let rate_limiter = self
            .per_second
            .or(self.throughput)
            .inspect(|limit| debug!(?limit))
            .and_then(NonZeroU32::new)
            .map(Quota::per_second)
            .map(RateLimiter::direct)
            .map(Arc::new)
            .inspect(|rate_limiter| debug!(?rate_limiter));

        let record_data = {
            let mut data = vec![0u8; self.record_size];
            getrandom::fill(&mut data)?;
            Bytes::from(data)
        };

        let batch_size = NonZeroU32::new(self.batch_size)
            .inspect(|batch_size| debug!(batch_size = batch_size.get()))
            .unwrap_or(nonzero!(10u32));

        let mut set = JoinSet::new();

        let client = ConnectionManager::builder(self.broker)
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        for id in 0..self.producers {
            let producer = Producer {
                id,
                rate_limiter: rate_limiter.clone(),
                topic: self.topic.clone(),
                partition: self.partition,
                record_data: record_data.clone(),
                token: token.clone(),
                client: client.clone(),
                batch_size,
                throughput: self.throughput,
            };

            _ = set.spawn(async move {
                loop {
                    match producer.rate_limited().await {
                        Ok(false) | Err(_) => break,
                        _ => continue,
                    }
                }
            });
        }

        let join_all = async {
            while !set.is_empty() {
                debug!(len = set.len());
                _ = set.join_next().await;
            }
        };

        let duration = self
            .duration
            .map(sleep)
            .map(Box::pin)
            .map(|pinned| pinned as Pin<Box<dyn Future<Output = ()>>>)
            .unwrap_or(Box::pin(std::future::pending()) as Pin<Box<dyn Future<Output = ()>>>);

        let cancellation = tokio::select! {

            timeout = duration => {
                debug!(?timeout);
                token.cancel();
                Some(CancelKind::Timeout)
            }

            completed = join_all => {
                debug!(?completed);
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

        debug!(?cancellation);

        meter_provider
            .shutdown()
            .inspect(|shutdown| debug!(?shutdown))?;

        if let Some(CancelKind::Timeout) = cancellation {
            sleep(Duration::from_secs(5)).await;
        }

        debug!(abort = set.len());
        set.abort_all();

        while !set.is_empty() {
            _ = set.join_next().await;
        }

        Ok(ErrorCode::None)
    }
}
