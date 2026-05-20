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

//! Protobuf message decoding into Arrow struct and map builders

use std::ops::Deref;

use arrow::array::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    LargeBinaryBuilder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
    TimestampMicrosecondBuilder,
};
use chrono::DateTime;
use protobuf::{
    MessageDyn,
    reflect::{FieldDescriptor, ReflectValueRef, RuntimeFieldType},
};
use protobuf_json_mapping::print_to_string;
use tracing::{debug, error};

use crate::{Error, Result};

use super::GOOGLE_PROTOBUF_TIMESTAMP;
use super::decode_value::decode_value;

pub(super) fn append_struct_builder(
    message: &dyn MessageDyn,
    builder: &mut StructBuilder,
) -> Result<()> {
    debug!(%message, ?builder);
    for (index, ref field) in message.descriptor_dyn().fields().enumerate() {
        debug!(field_name = field.name());

        match field.runtime_field_type() {
            RuntimeFieldType::Singular(singular) => {
                debug!(?singular);

                match field.get_singular_field_or_default(message) {
                    ReflectValueRef::U32(value) => builder
                        .field_builder::<Int32Builder>(index)
                        .ok_or(Error::Downcast)
                        .and_then(|values| {
                            i32::try_from(value)
                                .map_err(Into::into)
                                .map(|value| values.append_value(value))
                        })?,

                    ReflectValueRef::U64(value) => builder
                        .field_builder::<Int64Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .and_then(|values| {
                            i64::try_from(value)
                                .map_err(Into::into)
                                .map(|value| values.append_value(value))
                        })?,

                    ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => builder
                        .field_builder::<Int32Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::I64(value) => builder
                        .field_builder::<Int64Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::F32(value) => builder
                        .field_builder::<Float32Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::F64(value) => builder
                        .field_builder::<Float64Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Bool(value) => builder
                        .field_builder::<BooleanBuilder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::String(value) => builder
                        .field_builder::<StringBuilder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Bytes(value) => builder
                        .field_builder::<LargeBinaryBuilder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Message(message_ref) => {
                        if message_ref.deref().descriptor_dyn().full_name()
                            == GOOGLE_PROTOBUF_TIMESTAMP
                        {
                            let message = print_to_string(message_ref.deref())?;
                            debug!(message = message.trim_matches('"'));

                            let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                                .inspect(|dt| debug!(?dt))
                                .map(|dt| dt.timestamp_micros())?;
                            debug!(?value);

                            builder
                                .field_builder::<TimestampMicrosecondBuilder>(index)
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                                .inspect_err(|err| debug!(?err, ?message_ref, ?builder))?
                        } else {
                            builder
                                .field_builder::<StructBuilder>(index)
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .and_then(|builder| {
                                    append_struct_builder(message_ref.deref(), builder)
                                })
                                .inspect_err(|err| debug!(?err, ?message_ref, ?builder))?
                        }
                    }
                }
            }

            RuntimeFieldType::Repeated(repeated) => {
                debug!(?repeated);

                let builder = builder
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
                    .ok_or(Error::Downcast)
                    .inspect_err(|err| error!(?err, ?repeated))?;

                let values = builder.values().as_any_mut();

                for value in field.get_repeated(message) {
                    match value {
                        ReflectValueRef::U32(value) => values
                            .downcast_mut::<Int32Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .and_then(|builder| {
                                i32::try_from(value)
                                    .map_err(Into::into)
                                    .map(|value| builder.append_value(value))
                            })?,

                        ReflectValueRef::U64(value) => values
                            .downcast_mut::<Int64Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .and_then(|builder| {
                                i64::try_from(value)
                                    .map_err(Into::into)
                                    .map(|value| builder.append_value(value))
                            })?,

                        ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => values
                            .downcast_mut::<Int32Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::I64(value) => values
                            .downcast_mut::<Int64Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::F32(value) => values
                            .downcast_mut::<Float32Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::F64(value) => values
                            .downcast_mut::<Float64Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Bool(value) => values
                            .downcast_mut::<BooleanBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::String(value) => values
                            .downcast_mut::<StringBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Bytes(value) => values
                            .downcast_mut::<LargeBinaryBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Message(message_ref) => {
                            if message_ref.deref().descriptor_dyn().full_name()
                                == GOOGLE_PROTOBUF_TIMESTAMP
                            {
                                let message = print_to_string(message_ref.deref())?;
                                debug!(message = message.trim_matches('"'));

                                let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                                    .inspect(|dt| debug!(?dt))
                                    .map(|dt| dt.timestamp_micros())?;
                                debug!(?value);

                                values
                                    .downcast_mut::<TimestampMicrosecondBuilder>()
                                    .ok_or(Error::BadDowncast {
                                        field: field.name().to_owned(),
                                    })
                                    .map(|builder| builder.append_value(value))?
                            } else {
                                values
                                    .downcast_mut::<StructBuilder>()
                                    .ok_or(Error::BadDowncast {
                                        field: field.name().to_owned(),
                                    })
                                    .inspect_err(|err| error!(?err, ?message_ref))
                                    .and_then(|builder| {
                                        append_struct_builder(message_ref.deref(), builder)
                                    })?
                            }
                        }
                    }
                }

                builder.append(true);
            }

            RuntimeFieldType::Map(key, value) => {
                debug!(?key, ?value);

                builder
                    .as_any_mut()
                    .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                    .ok_or(Error::BadDowncast {
                        field: field.name().to_owned(),
                    })
                    .and_then(|builder| append_map_builder(message, field, builder))?
            }
        }
    }

    builder.append(true);

    Ok(())
}

pub(super) fn append_map_builder(
    message: &dyn MessageDyn,
    field: &FieldDescriptor,
    builder: &mut MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
) -> Result<()> {
    for (key, value) in &field.get_map(message) {
        decode_value(key, builder.keys())?;
        decode_value(value, builder.values())?;
    }

    builder.append(true).map_err(Into::into)
}
