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

//! AVRO schema Arrow builder inference

use apache_avro::schema::Schema as AvroSchema;
use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
        Float32Builder, Float64Builder, Int32Builder, Int64Builder, LargeBinaryBuilder,
        ListBuilder, MapBuilder, NullBuilder, StringBuilder, StructBuilder,
        Time32MillisecondBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, UInt32Builder,
    },
    datatypes::{DataType, Field},
};
use tracing::debug;

use crate::{Error, Result, avro::Schema};

use super::{NULLABLE, NullableVariant, append};

impl Schema {
    pub(super) fn schema_array_builder(
        &self,
        path: &[&str],
        schema: &AvroSchema,
    ) -> Result<Box<dyn ArrayBuilder>> {
        debug!(?path, ?schema);
        match schema {
            AvroSchema::Null => Ok(Box::new(NullBuilder::new())),
            AvroSchema::Boolean => Ok(Box::new(BooleanBuilder::new())),
            AvroSchema::Int => Ok(Box::new(Int32Builder::new())),
            AvroSchema::Long => Ok(Box::new(Int64Builder::new())),
            AvroSchema::Float => Ok(Box::new(Float32Builder::new())),
            AvroSchema::Double => Ok(Box::new(Float64Builder::new())),
            AvroSchema::Bytes => Ok(Box::new(LargeBinaryBuilder::new())),
            AvroSchema::String | AvroSchema::Uuid | AvroSchema::Enum(_) => {
                Ok(Box::new(StringBuilder::new()))
            }

            AvroSchema::Array(schema) => self
                .schema_array_builder(path, &schema.items)
                .map(ListBuilder::new)
                .and_then(|list_builder| {
                    self.schema_data_type(path, &schema.items).map(|data_type| {
                        list_builder.with_field(self.new_list_field(path, data_type))
                    })
                })
                .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>),

            AvroSchema::Map(schema) => self
                .schema_array_builder(
                    {
                        let mut path = Vec::from(path);
                        path.push("entries");
                        path.push("values");
                        path
                    }
                    .as_slice(),
                    &schema.types,
                )
                .and_then(|builder| {
                    self.schema_data_type(path, &schema.types).map(|data_type| {
                        let path = {
                            let mut path = Vec::from(path);
                            path.push("entries");
                            path
                        };

                        MapBuilder::new(
                            None,
                            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
                            builder,
                        )
                        .with_keys_field(self.new_nullable_field(
                            &path[..],
                            "keys",
                            DataType::Utf8,
                            !NULLABLE,
                        ))
                        .with_values_field(self.new_field(
                            &path[..],
                            "values",
                            data_type,
                        ))
                    })
                })
                .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>),

            AvroSchema::Union(schema) => {
                if let Some(schema) = schema.nullable_variant() {
                    self.schema_array_builder(path, schema)
                } else {
                    Err(Error::Message(format!(
                        "Avro Arrow builder conversion cannot encode non-null union {schema:?}"
                    )))
                }
            }

            AvroSchema::Record(schema) => schema
                .fields
                .iter()
                .map(|record_field| {
                    let inside = &append(path, &record_field.name)[..];

                    self.schema_data_type(inside, &record_field.schema)
                        .map(|data_type| self.new_field(path, &record_field.name, data_type))
                        .and_then(|field| {
                            self.schema_array_builder(inside, &record_field.schema)
                                .map(|builder| (field, builder))
                        })
                })
                .collect::<Result<(Vec<_>, Vec<_>)>>()
                .map(|(fields, builders)| StructBuilder::new(fields, builders))
                .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>),

            AvroSchema::Fixed(_schema) => Ok(Box::new(LargeBinaryBuilder::new())),

            AvroSchema::Decimal(schema) => u8::try_from(schema.precision)
                .and_then(|precision| {
                    i8::try_from(schema.scale).map(|scale| {
                        let data_type = if precision <= 16 {
                            DataType::Decimal128(precision, scale)
                        } else {
                            DataType::Decimal256(precision, scale)
                        };

                        if precision <= 16 {
                            Box::new(Decimal128Builder::new().with_data_type(data_type))
                                as Box<dyn ArrayBuilder>
                        } else {
                            Box::new(Decimal256Builder::new().with_data_type(data_type))
                                as Box<dyn ArrayBuilder>
                        }
                    })
                })
                .map_err(Into::into),

            AvroSchema::BigDecimal => Ok(Box::new(StringBuilder::new())),
            AvroSchema::Date => Ok(Box::new(Date32Builder::new())),
            AvroSchema::TimeMillis => Ok(Box::new(Time32MillisecondBuilder::new())),
            AvroSchema::TimeMicros => Ok(Box::new(Time64MicrosecondBuilder::new())),
            AvroSchema::TimestampMillis => Ok(Box::new(TimestampMillisecondBuilder::new())),
            AvroSchema::TimestampMicros => Ok(Box::new(TimestampMicrosecondBuilder::new())),
            AvroSchema::TimestampNanos => Ok(Box::new(TimestampNanosecondBuilder::new())),
            AvroSchema::LocalTimestampMillis => Ok(Box::new(TimestampMillisecondBuilder::new())),
            AvroSchema::LocalTimestampMicros => Ok(Box::new(TimestampMicrosecondBuilder::new())),
            AvroSchema::LocalTimestampNanos => Ok(Box::new(TimestampNanosecondBuilder::new())),

            AvroSchema::Duration => Ok(Box::new(StructBuilder::new(
                vec![
                    Field::new("month", DataType::UInt32, NULLABLE),
                    Field::new("days", DataType::UInt32, NULLABLE),
                    Field::new("milliseconds", DataType::UInt32, NULLABLE),
                ],
                vec![
                    Box::new(UInt32Builder::new()),
                    Box::new(UInt32Builder::new()),
                    Box::new(UInt32Builder::new()),
                ],
            ))),

            AvroSchema::Ref { name } => Err(Error::Message(format!(
                "Avro Arrow builder conversion cannot resolve reference {}",
                name.name
            ))),
        }
    }
}
