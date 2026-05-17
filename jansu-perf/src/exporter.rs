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
    sync::Mutex,
    time::{Duration, SystemTime},
};

use opentelemetry::InstrumentationScope;
use opentelemetry_sdk::{
    error::OTelSdkResult,
    metrics::{
        Temporality,
        data::{AggregatedMetrics, Metric, MetricData, ResourceMetrics},
        exporter::PushMetricExporter,
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};

use crate::observation::{Info, Latency, ObservationLatency};

#[derive(Debug)]
pub(crate) struct MetricExporter {
    started_at: SystemTime,
    temporality: Temporality,
    previous: Mutex<Option<ObservationLatency>>,
    cancellation: CancellationToken,
}

impl MetricExporter {
    pub(crate) fn new(cancellation: CancellationToken) -> Self {
        let started_at = SystemTime::now();
        Self {
            started_at,
            temporality: Default::default(),
            previous: Default::default(),
            cancellation,
        }
    }

    #[instrument(skip_all, fields(scope = scope.name(), metric = metric.name()))]
    fn info(&self, scope: &InstrumentationScope, metric: &Metric, info: &mut Info) {
        match (scope.name(), metric.name(), metric.data()) {
            ("jansu-client", "tcp_bytes_sent", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.current.observation.bytes_sent =
                    sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                "jansu-perf",
                "produce_record_count",
                AggregatedMetrics::U64(MetricData::Sum(sum)),
            ) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.current.observation.record_count =
                    sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                "jansu-perf",
                "produce_duration",
                AggregatedMetrics::U64(MetricData::Histogram(histogram)),
            ) => {
                info.current.latency = Latency::from(histogram);
            }

            _ => (),
        }
    }
}

impl PushMetricExporter for MetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let cancelled = self.cancellation.is_cancelled();

        if cancelled {
            if let Some(previous) = *self.previous.lock().expect("previous") {
                let mut info = Info::new(self.started_at);
                info.current = previous;

                println!("{}", info);
            }
        } else {
            let mut previous = self.previous.lock().expect("previous");

            let mut info = Info::new(self.started_at).with_previous(previous.take());

            for scope in metrics.scope_metrics() {
                debug!(scope = scope.scope().name());

                for metric in scope.metrics() {
                    debug!(scope = scope.scope().name(), metric = metric.name());

                    self.info(scope.scope(), metric, &mut info);
                }
            }

            println!("{info}");

            _ = previous.replace(info.current);
        }

        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    #[instrument]
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        let _ = timeout;
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        self.temporality
    }
}
