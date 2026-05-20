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

//! Coalescing of multiple deflated batches into a single deflated batch.

use jansu_sans_io::record::{Record, deflated, inflated};
use tracing::{debug, instrument};

use crate::Result;

#[instrument(skip_all)]
pub(crate) fn combine(batches: Vec<deflated::Batch>) -> Result<Option<deflated::Batch>> {
    debug!(len = batches.len());

    let mut i = batches.into_iter();

    let Some(first) = i.next() else {
        return Ok(None);
    };

    let mut sink = inflated::Batch::try_from(first)?;
    debug!(
        sink.base_offset,
        sink.last_offset_delta, sink.base_sequence, sink.max_timestamp
    );

    for batch in i {
        let batch = inflated::Batch::try_from(batch)?;

        debug!(
            sink.last_offset_delta,
            sink.max_timestamp, batch.base_offset, batch.last_offset_delta, batch.base_sequence
        );

        sink.records.append(
            &mut batch
                .records
                .into_iter()
                .map(|record| Record {
                    offset_delta: record.offset_delta + sink.last_offset_delta + 1,
                    timestamp_delta: record.timestamp_delta
                        + (sink.base_timestamp - batch.base_timestamp),
                    ..record
                })
                .collect::<Vec<_>>(),
        );

        sink.last_offset_delta += batch.last_offset_delta + 1;
        sink.max_timestamp = sink.max_timestamp.max(batch.max_timestamp);
    }

    debug!(
        sink.base_offset,
        sink.last_offset_delta, sink.base_sequence, sink.max_timestamp
    );

    deflated::Batch::try_from(sink)
        .map(Some)
        .map_err(Into::into)
}
