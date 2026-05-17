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

use core::fmt::{self, Display};
use std::{
    ops::AddAssign,
    time::{Duration, SystemTime},
};

use human_units::{
    FormatDuration,
    iec::{Byte, Prefix},
};
use opentelemetry_sdk::metrics::data::Histogram;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Observation {
    pub(crate) taken_at: SystemTime,
    pub(crate) bytes_sent: u64,
    pub(crate) record_count: u64,
}

impl AddAssign for Observation {
    fn add_assign(&mut self, rhs: Self) {
        self.taken_at = self.taken_at.max(rhs.taken_at);
        self.bytes_sent += rhs.bytes_sent;
        self.record_count += rhs.record_count;
    }
}

impl Default for Observation {
    fn default() -> Self {
        Self {
            taken_at: SystemTime::now(),
            bytes_sent: Default::default(),
            record_count: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub(crate) struct Info {
    pub(crate) started_at: SystemTime,
    pub(crate) previous: Option<ObservationLatency>,
    pub(crate) current: ObservationLatency,
}

impl Info {
    pub(crate) fn new(started_at: SystemTime) -> Self {
        Self {
            started_at,
            current: Default::default(),
            previous: Default::default(),
        }
    }

    pub(crate) fn with_previous(self, previous: Option<ObservationLatency>) -> Self {
        Self { previous, ..self }
    }

    pub(crate) fn elapsed(&self) -> Duration {
        self.current
            .observation
            .taken_at
            .duration_since(
                self.previous
                    .map_or(self.started_at, |previous| previous.observation.taken_at),
            )
            .expect("duration")
    }

    pub(crate) fn bytes_sent(&self) -> u64 {
        self.current.observation.bytes_sent
            - self
                .previous
                .map_or(0, |previous| previous.observation.bytes_sent)
    }

    pub(crate) fn records_sent(&self) -> u64 {
        self.current.observation.record_count
            - self
                .previous
                .map_or(0, |previous| previous.observation.record_count)
    }

    pub(crate) fn records_sent_per_second(&self) -> f64 {
        let elapsed = u32::try_from(self.elapsed().as_secs()).unwrap_or(u32::MAX);
        let records = u32::try_from(self.records_sent()).unwrap_or(u32::MAX);
        f64::from(records) / f64::from(elapsed)
    }

    pub(crate) fn bandwidth(&self) -> Byte {
        self.bytes_sent()
            .checked_div(self.elapsed().as_secs())
            .map(|throughput| Byte::with_iec_prefix(throughput, Prefix::None))
            .expect("throughput")
    }
}

impl Display for Info {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "elapsed: {}, {} records sent, {:.1} records/s, ({}/s), latency: {} min, {:.1}ms avg, {} max",
            self.elapsed().format_duration(),
            self.records_sent(),
            self.records_sent_per_second(),
            self.bandwidth().format_iec(),
            self.current
                .latency
                .min
                .map(|min| min.format_duration())
                .expect("minimum"),
            self.current.latency.mean.expect("mean"),
            self.current
                .latency
                .max
                .map(|max| max.format_duration())
                .expect("max")
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
pub(crate) struct Latency {
    pub(crate) min: Option<Duration>,
    pub(crate) max: Option<Duration>,
    pub(crate) mean: Option<f64>,
}

impl From<&Histogram<u64>> for Latency {
    fn from(histogram: &Histogram<u64>) -> Self {
        let min = histogram
            .data_points()
            .filter_map(|dp| dp.min())
            .min()
            .map(Duration::from_millis);

        let max = histogram
            .data_points()
            .filter_map(|dp| dp.max())
            .max()
            .map(Duration::from_millis);

        let sum = u32::try_from(histogram.data_points().map(|dp| dp.sum()).sum::<u64>())
            .unwrap_or(u32::MAX);
        let count = u32::try_from(histogram.data_points().map(|dp| dp.count()).sum::<u64>())
            .unwrap_or(u32::MAX);

        let mean = Some(f64::from(sum) / f64::from(count));

        Self { min, max, mean }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
pub(crate) struct ObservationLatency {
    pub(crate) observation: Observation,
    pub(crate) latency: Latency,
}
