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

//! Shared helpers for the SQL-backed storage engines: the embedded statement
//! [`Cache`], comment stripping and idempotent-sequence checks.

use std::{
    cmp::Ordering,
    hash::{DefaultHasher, Hash, Hasher as _},
};

use jansu_sans_io::{ErrorCode, record::deflated};
use tracing::debug;
use uuid::Uuid;

use crate::{Error, Result};

mod comments;

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
mod cache;
#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
mod statements;

#[cfg(test)]
mod tests;

pub(crate) use comments::remove_comments;

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
pub(crate) use cache::{Cache, SQL};

pub(crate) fn idempotent_sequence_check(
    producer_epoch: &i16,
    sequence: &i32,
    deflated: &deflated::Batch,
) -> Result<i32> {
    match producer_epoch.cmp(&deflated.producer_epoch) {
        Ordering::Equal => match sequence.cmp(&deflated.base_sequence) {
            Ordering::Equal => Ok(deflated.last_offset_delta + 1),

            Ordering::Greater => {
                debug!(?sequence, ?deflated.base_sequence);
                Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
            }

            Ordering::Less => {
                debug!(?sequence, ?deflated.base_sequence);
                Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
            }
        },

        Ordering::Greater => Err(Error::Api(ErrorCode::ProducerFenced)),

        Ordering::Less => Err(Error::Api(ErrorCode::InvalidProducerEpoch)),
    }
}

pub(crate) fn default_hash<H>(h: &H) -> Uuid
where
    H: Hash,
{
    let mut s = DefaultHasher::new();
    h.hash(&mut s);
    Uuid::from_u128(s.finish() as u128)
}
