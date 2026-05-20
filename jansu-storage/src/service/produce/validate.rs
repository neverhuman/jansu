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

//! Batch validation for [`ProduceService`].

use jansu_sans_io::{
    BatchAttribute, ErrorCode,
    record::{Record, deflated, inflated},
};
use rama::Context;
use tracing::debug;

use super::ProduceService;
use crate::{Error, Result, Storage, StorageFeature};

impl ProduceService {
    pub(super) fn exactness_enabled<G>(&self, ctx: &Context<G>) -> bool
    where
        G: Storage,
    {
        ctx.state()
            .capabilities()
            .supports(StorageFeature::BatchValidation)
    }

    pub(super) fn validate_batch(batch: &deflated::Batch) -> Result<()> {
        let _compression =
            BatchAttribute::try_from(batch.attributes).map_err(|error| match error {
                jansu_sans_io::Error::UnknownCompressionType(_) => {
                    Error::Api(ErrorCode::UnsupportedCompressionType)
                }
                otherwise => Error::from(otherwise),
            })?;

        if batch.magic != Self::MAGIC_V2 {
            return Err(Error::Api(ErrorCode::InvalidRecord));
        }

        // Kafka broker default message.max.bytes = 1_048_588 (1 MiB + 12 bytes overhead).
        // The batch_length field represents the size of the batch payload after the
        // base_offset (8 bytes) and batch_length (4 bytes) header, so the total
        // on-wire size is batch_length + 12.
        const MAX_MESSAGE_BYTES: i32 = 1_048_588;
        let total_batch_size = batch.batch_length + 12;
        if total_batch_size > MAX_MESSAGE_BYTES {
            return Err(Error::Api(ErrorCode::MessageTooLarge));
        }

        if batch.base_timestamp < 0
            || batch.max_timestamp < 0
            || batch.max_timestamp < batch.base_timestamp
        {
            return Err(Error::Api(ErrorCode::InvalidTimestamp));
        }

        let records: Vec<Record> = batch.try_into().map_err(|error| {
            debug!(?error);
            Error::Api(ErrorCode::CorruptMessage)
        })?;

        if records.is_empty() {
            return Err(Error::Api(ErrorCode::InvalidRecord));
        }

        if records
            .iter()
            .enumerate()
            .any(|(index, record)| record.offset_delta != index as i32)
        {
            return Err(Error::Api(ErrorCode::InvalidRecord));
        }

        let expected_record_count =
            u32::try_from(records.len()).map_err(|_| Error::Api(ErrorCode::InvalidRecord))?;
        if batch.record_count != expected_record_count {
            return Err(Error::Api(ErrorCode::InvalidRecord));
        }

        let expected_last_offset_delta =
            i32::try_from(records.len() - 1).map_err(|_| Error::Api(ErrorCode::InvalidRecord))?;
        if batch.last_offset_delta != expected_last_offset_delta {
            return Err(Error::Api(ErrorCode::InvalidRecord));
        }

        let inflated = inflated::Batch {
            base_offset: batch.base_offset,
            batch_length: batch.batch_length,
            partition_leader_epoch: batch.partition_leader_epoch,
            magic: batch.magic,
            crc: batch.crc,
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            records,
        };

        let canonical = deflated::Batch::try_from(inflated).map_err(|error| {
            debug!(?error);
            Error::Api(ErrorCode::CorruptMessage)
        })?;

        if canonical.batch_length != batch.batch_length || canonical.crc != batch.crc {
            return Err(Error::Api(ErrorCode::CorruptMessage));
        }

        Ok(())
    }
}
