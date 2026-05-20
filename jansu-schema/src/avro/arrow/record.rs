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

//! AVRO Arrow record builder, struct append and column processing

use std::iter::zip;

use apache_avro::{
    Reader,
    schema::{RecordSchema, Schema as AvroSchema},
    types::Value,
};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder, NullBuilder,
    StringBuilder, StructBuilder, Time32MillisecondBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    UInt32Builder,
};
use bytes::Bytes;
use jansu_sans_io::ErrorCode;
use num_bigint::BigInt;
use tracing::{debug, error};

use crate::{Error, Result, avro::Schema};

use super::NullableVariant;
use super::list::{append_list_builder, append_map_builder};
use super::value::append_value;

#[derive(Default)]
pub(super) struct RecordBuilder(pub(super) Vec<Box<dyn ArrayBuilder>>);

impl TryFrom<&Schema> for RecordBuilder {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        debug!(?schema);

        schema
            .complete
            .as_ref()
            .map_or(Ok(vec![]), |complete| {
                complete
                    .fields
                    .iter()
                    .inspect(|field| debug!(?field))
                    .map(|field| schema.schema_array_builder(&[&field.name], &field.schema))
                    .collect::<Result<Vec<_>>>()
            })
            .map(Self)
    }
}

macro_rules! try_as {
    ($name:ident, $pattern:path, $type:ty) => {
        pub(super) fn $name(value: Value) -> Result<$type> {
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

pub(super) fn conversion_error(context: &str, schema: &AvroSchema, value: Option<&Value>) -> Error {
    value.map_or_else(
        || {
            Error::Message(format!(
                "Avro Arrow {context} cannot convert schema {schema:?}"
            ))
        },
        |value| {
            Error::Message(format!(
                "Avro Arrow {context} cannot convert schema {schema:?} and value {value:?}"
            ))
        },
    )
}

pub(super) fn decimal_i128(value: apache_avro::Decimal) -> Result<i128> {
    let big_int = BigInt::from(value);
    let rendered = big_int.to_string();
    big_int.try_into().map_err(|_| {
        Error::Message(format!(
            "Avro decimal value exceeds Arrow decimal128: {rendered}"
        ))
    })
}

pub(super) fn append_duration_value(
    value: apache_avro::Duration,
    builder: &mut StructBuilder,
) -> Result<()> {
    builder
        .field_builder::<UInt32Builder>(0)
        .ok_or(Error::Downcast)
        .map(|values| values.append_value(u32::from(value.months())))?;
    builder
        .field_builder::<UInt32Builder>(1)
        .ok_or(Error::Downcast)
        .map(|values| values.append_value(u32::from(value.days())))?;
    builder
        .field_builder::<UInt32Builder>(2)
        .ok_or(Error::Downcast)
        .map(|values| values.append_value(u32::from(value.millis())))?;
    builder.append(true);
    Ok(())
}

pub(super) fn append_struct_builder(
    schema: &RecordSchema,
    items: Vec<(String, Value)>,
    builder: &mut StructBuilder,
) -> Result<()> {
    for (index, (field, (name, value))) in zip(schema.fields.as_slice(), items).enumerate() {
        debug!(?index, ?field, ?name, ?value);

        match (&field.schema, value) {
            (AvroSchema::Null, Value::Null) => builder
                .field_builder::<NullBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_null())?,

            (AvroSchema::Boolean, Value::Boolean(value)) => builder
                .field_builder::<BooleanBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Int, Value::Int(value)) => builder
                .field_builder::<Int32Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Long, Value::Long(value)) => builder
                .field_builder::<Int64Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Float, Value::Float(value)) => builder
                .field_builder::<Float32Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Double, Value::Double(value)) => builder
                .field_builder::<Float64Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Bytes, Value::Bytes(value)) => builder
                .field_builder::<LargeBinaryBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::String, Value::String(value))
            | (AvroSchema::Enum(_), Value::Enum(_, value)) => builder
                .field_builder::<StringBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Array(schema), Value::Array(values)) => builder
                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
                .ok_or(Error::BadDowncast { field: name })
                .inspect_err(|err| error!(?err, ?schema, ?values))
                .and_then(|builder| append_list_builder(schema, values, builder))?,

            (AvroSchema::Map(schema), Value::Map(values)) => builder
                .field_builder::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(index)
                .ok_or(Error::BadDowncast { field: name })
                .inspect_err(|err| error!(?err, ?schema, ?values))
                .and_then(|builder| append_map_builder(schema, values, builder))?,

            (AvroSchema::Union(schema), Value::Union(_, value)) => {
                return Err(conversion_error(
                    if schema.nullable_variant().is_some() {
                        "struct nullable-union append"
                    } else {
                        "struct non-null union append"
                    },
                    &field.schema,
                    Some(&value),
                ));
            }

            (AvroSchema::Record(schema), Value::Record(items)) => builder
                .field_builder::<StructBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .and_then(|builder| append_struct_builder(schema, items, builder))?,

            (AvroSchema::Fixed(_), Value::Fixed(_, value)) => builder
                .field_builder::<LargeBinaryBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Decimal(_), Value::Decimal(value)) => builder
                .field_builder::<Decimal128Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .and_then(|values| decimal_i128(value).map(|value| values.append_value(value)))?,

            (AvroSchema::BigDecimal, Value::BigDecimal(value)) => builder
                .field_builder::<StringBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value.to_string()))?,

            (AvroSchema::Uuid, Value::Uuid(value)) => builder
                .field_builder::<StringBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value.to_string()))?,

            (AvroSchema::Date, Value::Date(value)) => builder
                .field_builder::<Date32Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimeMillis, Value::TimeMillis(value)) => builder
                .field_builder::<Time32MillisecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimeMicros, Value::TimeMicros(value)) => builder
                .field_builder::<Time64MicrosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimestampMillis, Value::TimestampMillis(value)) => builder
                .field_builder::<TimestampMillisecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimestampMicros, Value::TimestampMicros(value)) => builder
                .field_builder::<TimestampMicrosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimestampNanos, Value::TimestampNanos(value)) => builder
                .field_builder::<TimestampNanosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::LocalTimestampMillis, Value::LocalTimestampMillis(value)) => builder
                .field_builder::<TimestampMillisecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::LocalTimestampMicros, Value::LocalTimestampMicros(value)) => builder
                .field_builder::<TimestampMicrosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::LocalTimestampNanos, Value::LocalTimestampNanos(value)) => builder
                .field_builder::<TimestampNanosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Duration, Value::Duration(value)) => builder
                .field_builder::<StructBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .and_then(|values| append_duration_value(value, values))?,

            (AvroSchema::Ref { name }, value) => {
                return Err(Error::Message(format!(
                    "Avro Arrow struct conversion cannot resolve reference {} for value {value:?}",
                    name.name
                )));
            }
            (schema, value) => {
                return Err(conversion_error("struct append", schema, Some(&value)));
            }
        }
    }

    builder.append(true);
    Ok(())
}

pub(super) fn process<'a, T>(
    schema: Option<&AvroSchema>,
    encoded: Option<Bytes>,
    builders: &mut T,
) -> Result<()>
where
    T: Iterator<Item = &'a mut Box<dyn ArrayBuilder>>,
{
    schema.map_or(Ok(()), |schema| {
        builders
            .next()
            .ok_or(Error::BuilderExhausted)
            .and_then(|builder| {
                encoded
                    .map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
                        Reader::with_schema(schema, &encoded[..])?
                            .next()
                            .transpose()
                            .map_err(Into::into)
                    })
                    .inspect(|value| debug!(?value))
                    .and_then(|value| value.ok_or(Error::Api(ErrorCode::InvalidRecord)))
                    .and_then(|value| append_value(Some(schema), value, builder))
                    .inspect_err(|err| error!(?err, ?schema))
            })
    })
}
