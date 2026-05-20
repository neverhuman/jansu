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

//! Protobuf reflect value decoding and message descriptor processing

use std::ops::Deref;

use arrow::array::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    LargeBinaryBuilder, StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
};
use bytes::Bytes;
use chrono::DateTime;
use jansu_sans_io::ErrorCode;
use protobuf::{
    CodedInputStream,
    reflect::{MessageDescriptor, ReflectValueRef},
};
use protobuf_json_mapping::print_to_string;
use tracing::{debug, error};

use crate::{Error, Result};

use super::GOOGLE_PROTOBUF_TIMESTAMP;
use super::decode::append_struct_builder;

pub(super) fn decode_value(
    value: ReflectValueRef<'_>,
    builder: &mut dyn ArrayBuilder,
) -> Result<()> {
    debug!(?value);

    match value {
        ReflectValueRef::U32(value) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| {
                i32::try_from(value)
                    .map_err(Into::into)
                    .map(|value| builder.append_value(value))
            }),

        ReflectValueRef::U64(value) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| {
                i64::try_from(value)
                    .map_err(Into::into)
                    .map(|value| builder.append_value(value))
            }),

        ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::I64(value) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::F32(value) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::F64(value) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::Bool(value) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::String(value) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::Bytes(value) => builder
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::Message(message_ref) => {
            if message_ref.descriptor_dyn().full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                let message = print_to_string(message_ref.deref())?;
                debug!(message = message.trim_matches('"'));

                let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                    .inspect(|dt| debug!(?dt))
                    .map(|dt| dt.timestamp_micros())?;
                debug!(?value);

                builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_value(value))
            } else {
                builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .ok_or(Error::Downcast)
                    .inspect_err(|err| error!(?err, ?message_ref))
                    .and_then(|builder| append_struct_builder(message_ref.deref(), builder))
            }
        }
    }
}

pub(super) fn process_message_descriptor<'a, T>(
    descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
    builders: &mut T,
) -> Result<()>
where
    T: Iterator<Item = &'a mut Box<dyn ArrayBuilder>>,
{
    let Some(descriptor) = descriptor else {
        return Ok(());
    };

    debug!(descriptor = descriptor.name(), ?encoded,);

    let message = {
        let mut message = descriptor.new_instance();
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            message
                .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
                .inspect_err(|err| error!(?err))
                .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
        })?;

        message
    };

    builders
        .next()
        .ok_or(Error::BuilderExhausted)
        .map(|column| column.as_any_mut())
        .inspect(|column| debug!(?column))
        .and_then(|column| {
            column
                .downcast_mut::<StructBuilder>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| debug!(?err))
        })
        .and_then(|column| append_struct_builder(message.as_ref(), column))
        .inspect_err(|err| debug!(?err))
}
