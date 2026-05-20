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

mod combine_tests;
mod flight_recorder;
mod produce_tests;

use bytes::Bytes;
use jansu_sans_io::record::{Record, deflated, inflated};
use tracing::debug;

use crate::Result;

fn into_batch(
    attributes: i16,
    producer_id: i64,
    producer_epoch: i16,
    base_offset: i64,
    records: &[Bytes],
) -> Result<deflated::Batch> {
    let base_sequence = 0;

    let mut inflated = inflated::Batch::builder()
        .attributes(attributes)
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_offset(base_offset)
        .last_offset_delta(records.len() as i32 - 1)
        .base_sequence(base_sequence);

    for (offset_delta, value) in records.iter().enumerate() {
        inflated = inflated.record(
            Record::builder()
                .value(value.clone().into())
                .offset_delta(offset_delta as i32),
        );
    }

    inflated
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))
        .map_err(Into::into)
}

fn into_batches(
    attributes: i16,
    producer_id: i64,
    producer_epoch: i16,
    base_offset: i64,
    batches: &[Vec<Bytes>],
) -> Result<Vec<deflated::Batch>> {
    let mut split = vec![];
    let mut base_sequence = 0;

    for batch in batches {
        let mut inflated = inflated::Batch::builder()
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .last_offset_delta(batch.len() as i32 - 1)
            .base_sequence(base_sequence);

        for (offset_delta, value) in batch.iter().enumerate() {
            inflated = inflated.record(
                Record::builder()
                    .value(value.clone().into())
                    .offset_delta(offset_delta as i32),
            );
        }

        split.push(
            inflated
                .build()
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?,
        );

        base_sequence += batch.len() as i32;
    }

    Ok(split)
}
