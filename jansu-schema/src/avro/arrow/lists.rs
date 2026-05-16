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

use std::collections::HashMap;

use apache_avro::{
    schema::{ArraySchema, MapSchema, Schema as AvroSchema},
    types::Value,
};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder, NullBuilder, StringBuilder,
    StructBuilder, Time32MillisecondBuilder, Time64MicrosecondBuilder,
};
use tracing::{debug, error};

use crate::{Error, Result};

use super::structs::append_struct_builder;
use super::values::append_value;

macro_rules! try_as {
    ($name:ident, $pattern:path, $type:ty) => {
        fn $name(value: Value) -> Result<$type> {
            if let $pattern(value) = value {
                Ok(value)
            } else {
                Err(Error::InvalidValue(value))
            }
        }
    };
}

try_as!(try_as_i32, Value::Int, i32);
try_as!(try_as_bool, Value::Boolean, bool);
try_as!(try_as_i64, Value::Long, i64);
try_as!(try_as_f32, Value::Float, f32);
try_as!(try_as_f64, Value::Double, f64);
try_as!(try_as_bytes, Value::Bytes, Vec<u8>);
try_as!(try_as_string, Value::String, String);
try_as!(try_as_record, Value::Record, Vec<(String, Value)>);

pub(super) fn append_list_builder(
    schema: &ArraySchema,
    values: Vec<Value>,
    builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    match schema.items.as_ref() {
        AvroSchema::Null => builder
            .values()
            .as_any_mut()
            .downcast_mut::<NullBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_bool)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_nulls(values.len()))
            })?,

        AvroSchema::Boolean => builder
            .values()
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_bool)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Int => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Long => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i64)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Float => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_f32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Double => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_f64)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Bytes => builder
            .values()
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_bytes)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::String | AvroSchema::Uuid => builder
            .values()
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_string)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::Array(schema) => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: format!("nested Avro array inside list builder is not supported: {schema:?}"),
            });
        }
        AvroSchema::Map(schema) => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: format!("Avro map inside list builder is not supported: {schema:?}"),
            });
        }
        AvroSchema::Union(schema) => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: format!("Avro union inside list builder is not supported: {schema:?}"),
            });
        }

        AvroSchema::Record(schema) => builder
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_record)
                    .collect::<Result<Vec<_>>>()
                    .and_then(|values| {
                        values
                            .into_iter()
                            .map(|items| append_struct_builder(schema, items, builder))
                            .collect::<Result<Vec<_>>>()
                    })
            })
            .map(|_| ())?,

        AvroSchema::Enum(schema) => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: format!("Avro enum inside list builder is not supported: {schema:?}"),
            });
        }
        AvroSchema::Fixed(schema) => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: format!("Avro fixed inside list builder is not supported: {schema:?}"),
            });
        }
        AvroSchema::Decimal(schema) => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: format!("Avro decimal inside list builder is not supported: {schema:?}"),
            });
        }
        AvroSchema::BigDecimal => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from("Avro BigDecimal inside list builder is not supported"),
            });
        }

        AvroSchema::Date => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::TimeMillis => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Time32MillisecondBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::TimeMicros => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i64)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::TimestampMillis => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from("Avro TimestampMillis inside list builder is not supported"),
            });
        }
        AvroSchema::TimestampMicros => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from("Avro TimestampMicros inside list builder is not supported"),
            });
        }
        AvroSchema::TimestampNanos => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from("Avro TimestampNanos inside list builder is not supported"),
            });
        }
        AvroSchema::LocalTimestampMillis => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from(
                    "Avro LocalTimestampMillis inside list builder is not supported",
                ),
            });
        }
        AvroSchema::LocalTimestampMicros => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from(
                    "Avro LocalTimestampMicros inside list builder is not supported",
                ),
            });
        }
        AvroSchema::LocalTimestampNanos => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from(
                    "Avro LocalTimestampNanos inside list builder is not supported",
                ),
            });
        }
        AvroSchema::Duration => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from("Avro Duration inside list builder is not supported"),
            });
        }
        AvroSchema::Ref { name } => {
            return Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: format!("Avro schema reference {name:?} inside list builder cannot be resolved"),
            });
        }
    }

    builder.append(true);

    Ok(())
}

pub(super) fn append_map_builder(
    schema: &MapSchema,
    values: HashMap<String, Value>,
    builder: &mut MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
) -> Result<()> {
    debug!(?schema, ?values);

    for (key, value) in values {
        append_value(None, Value::String(key), builder.keys())?;
        append_value(None, value, builder.values())?;
    }

    builder.append(true).map_err(Into::into)
}
