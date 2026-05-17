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

use std::time::{Duration, SystemTime};

use crate::observation::{Info, Observation, ObservationLatency};

#[test]
fn add_assign_observation() {
    let now = SystemTime::now();
    let delta = Duration::from_secs(5);

    let previous = Observation {
        taken_at: now.checked_sub(delta).expect("previous"),
        bytes_sent: 32_123,
        record_count: 12_321,
    };

    let mut current = Observation {
        taken_at: now,
        bytes_sent: 43_234,
        record_count: 54_345,
    };

    current += previous;

    assert_eq!(now, current.taken_at);
    assert_eq!(75_357, current.bytes_sent);
    assert_eq!(66_666, current.record_count);
}

#[test]
fn middle_observation() {
    let now = SystemTime::now();
    let elapsed = Duration::from_secs(4);

    let previous = {
        let observation = Observation {
            taken_at: now.checked_sub(elapsed).expect("previous"),
            bytes_sent: 43_234,
            record_count: 212,
        };

        ObservationLatency {
            observation,
            latency: Default::default(),
        }
    };

    let mut info = Info::new(now).with_previous(Some(previous));

    info.current = {
        let observation = Observation {
            taken_at: now,
            bytes_sent: 65_456,
            record_count: 656,
        };

        ObservationLatency {
            observation,
            latency: Default::default(),
        }
    };

    assert_eq!(elapsed, info.elapsed());
    assert_eq!(5_555, info.bandwidth().0);
    assert_eq!(111f64, info.records_sent_per_second());
}

#[test]
fn last_or_first_observation() {
    let now = SystemTime::now();
    let elapsed = Duration::from_secs(4);

    let mut info = Info::new(now.checked_sub(elapsed).expect("elapsed"));

    info.current = {
        let observation = Observation {
            taken_at: now,
            bytes_sent: 65_456,
            record_count: 656,
        };

        ObservationLatency {
            observation,
            latency: Default::default(),
        }
    };

    assert_eq!(elapsed, info.elapsed());
    assert_eq!(16_364, info.bandwidth().0);
    assert_eq!(164f64, info.records_sent_per_second());
}
