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

//! AVRO value append into Arrow column builders

use apache_avro::{schema::Schema as AvroSchema, types::Value};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder, StringBuilder,
    StructBuilder, Time32MillisecondBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder,
};
use tracing::{debug, error};

use crate::{Error, Result};

use super::NullableVariant;
use super::list::{append_list_builder, append_map_builder};
use super::record::{append_duration_value, append_struct_builder, decimal_i128};

pub(super) fn append_value(
    schema: Option<&AvroSchema>,
    value: Value,
    column: &mut Box<dyn ArrayBuilder>,
) -> Result<()> {
    debug!(?value);

    match (schema, value) {
        (Some(AvroSchema::Boolean), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Int), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Long), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Float), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Double), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Bytes), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::String), Value::Null) | (Some(AvroSchema::Enum(_)), Value::Null) => {
            column
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_null())
        }

        (Some(AvroSchema::Fixed(_)), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Array(schema)), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema))
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Record(_)), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Map(schema)), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema))
            .inspect(|_| debug!(?schema))
            .and_then(|builder| builder.append(true).map_err(Into::into)),

        (Some(AvroSchema::Date), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::TimeMillis), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Time32MillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::TimeMicros), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::TimestampMillis), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::TimestampMicros), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::TimestampNanos), Value::Null)
        | (Some(AvroSchema::LocalTimestampNanos), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::LocalTimestampMillis), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::LocalTimestampMicros), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Decimal(_)), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::BigDecimal), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Duration), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Uuid), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (schema, Value::Null) => Err(Error::Message(format!(
            "Avro Arrow null append cannot convert schema {schema:?}"
        ))),

        (_, Value::Boolean(value)) => column
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Int(value)) => column
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Long(value)) => column
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Float(value)) => column
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Double(value)) => column
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Bytes(value)) => column
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::String(value)) | (Some(AvroSchema::Enum(_)), Value::Enum(_, value)) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Fixed(_, value)) => column
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (Some(AvroSchema::Union(schema)), Value::Union(_, value)) => {
            debug!(?schema, ?value);

            if let Some(schema) = schema.nullable_variant() {
                append_value(Some(schema), *value, column)
            } else {
                Err(Error::Message(format!(
                    "Avro Arrow value append cannot encode non-null union {schema:?}"
                )))
            }
        }

        (Some(AvroSchema::Array(schema)), Value::Array(values)) => column
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| append_list_builder(schema, values, builder)),

        (Some(AvroSchema::Record(schema)), Value::Record(items)) => column
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| append_struct_builder(schema, items, builder)),

        (Some(AvroSchema::Map(schema)), Value::Map(values)) => column
            .as_any_mut()
            .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .inspect(|_| debug!(?schema, ?values))
            .and_then(|builder| append_map_builder(schema, values, builder)),

        (Some(AvroSchema::Date), Value::Date(value)) => column
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (Some(AvroSchema::Decimal(_)), Value::Decimal(value)) => column
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| decimal_i128(value).map(|value| builder.append_value(value))),

        (Some(AvroSchema::BigDecimal), Value::BigDecimal(value)) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value.to_string())),

        (_, Value::TimeMillis(value)) => column
            .as_any_mut()
            .downcast_mut::<Time32MillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimeMicros(value)) => column
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimestampMillis(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimestampMicros(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimestampNanos(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::LocalTimestampMillis(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),
        (_, Value::LocalTimestampMicros(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),
        (_, Value::LocalTimestampNanos(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Duration(value)) => column
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| append_duration_value(value, builder)),

        (_, Value::Uuid(value)) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value.to_string())),

        (schema, value) => Err(Error::Message(format!(
            "Avro Arrow value append cannot convert schema {schema:?} and value {value:?}"
        ))),
    }
}
