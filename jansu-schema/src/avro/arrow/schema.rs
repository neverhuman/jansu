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

//! AVRO schema Arrow data type inference

use apache_avro::schema::Schema as AvroSchema;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit, UnionFields, UnionMode};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use tracing::debug;

use crate::{ARROW_LIST_FIELD_NAME, Error, Result, avro::Schema};

use super::{NULLABLE, NullableVariant, SORTED_MAP_KEYS, append};

impl Schema {
    pub(super) fn new_list_field(&self, path: &[&str], data_type: DataType) -> Field {
        self.new_field(path, ARROW_LIST_FIELD_NAME, data_type)
    }

    pub(super) fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        self.new_nullable_field(path, name, data_type, NULLABLE)
    }

    pub(super) fn new_nullable_field(
        &self,
        path: &[&str],
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Field {
        debug!(?path, name, ?data_type, ?nullable);

        let path = append(path, name).join(".");

        Field::new(name.to_owned(), data_type, nullable).with_metadata(
            self.ids
                .get(path.as_str())
                .inspect(|field_id| debug!(?path, name, field_id))
                .map(|field_id| (PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string()))
                .into_iter()
                .collect(),
        )
    }

    pub(super) fn schema_data_type(&self, path: &[&str], schema: &AvroSchema) -> Result<DataType> {
        debug!(?path, ?schema);

        match schema {
            AvroSchema::Null => Ok(DataType::Null),
            AvroSchema::Boolean => Ok(DataType::Boolean),
            AvroSchema::Int => Ok(DataType::Int32),
            AvroSchema::Long => Ok(DataType::Int64),
            AvroSchema::Float => Ok(DataType::Float32),
            AvroSchema::Double => Ok(DataType::Float64),
            AvroSchema::Bytes => Ok(DataType::LargeBinary),
            AvroSchema::String | AvroSchema::Uuid | AvroSchema::Enum(_) => Ok(DataType::Utf8),

            AvroSchema::Array(schema) => self
                .schema_data_type(path, &schema.items)
                .inspect(|data_type| debug!(?schema, ?data_type))
                .map(|data_type| {
                    DataType::List(FieldRef::new(self.new_list_field(path, data_type)))
                }),

            AvroSchema::Map(schema) => self
                .schema_data_type(path, &schema.types)
                .inspect(|value| debug!(?schema, ?value))
                .map(|value| {
                    let inside = append(path, "entries");

                    DataType::Map(
                        FieldRef::new(self.new_nullable_field(
                            path,
                            "entries",
                            DataType::Struct(Fields::from_iter([
                                self.new_nullable_field(
                                    &inside[..],
                                    "keys",
                                    DataType::Utf8,
                                    !NULLABLE,
                                ),
                                self.new_field(&inside[..], "values", value),
                            ])),
                            !NULLABLE,
                        )),
                        SORTED_MAP_KEYS,
                    )
                }),

            AvroSchema::Union(schema) => {
                debug!(?schema);

                if let Some(schema) = schema.nullable_variant() {
                    self.schema_data_type(path, schema)
                } else {
                    schema
                        .variants()
                        .iter()
                        .enumerate()
                        .map(|(index, variant)| {
                            self.schema_data_type(path, variant)
                                .map(|data_type| {
                                    Field::new(format!("field{}", index + 1), data_type, NULLABLE)
                                })
                                .inspect(|field| debug!(?field))
                        })
                        .collect::<Result<Vec<_>>>()
                        .inspect(|fields| debug!(?fields))
                        .and_then(|fields| {
                            i8::try_from(schema.variants().len())
                                .map_err(Into::into)
                                .and_then(|type_ids| {
                                    UnionFields::try_new((1..=type_ids).collect::<Vec<_>>(), fields)
                                        .map_err(Into::into)
                                })
                        })
                        .inspect(|union_fields| debug!(?union_fields))
                        .map(|fields| DataType::Union(fields, UnionMode::Dense))
                }
            }

            AvroSchema::Record(schema) => {
                debug!(?schema);
                schema
                    .fields
                    .iter()
                    .map(|field| {
                        let inside = append(path, &field.name);

                        self.schema_data_type(&inside[..], &field.schema)
                            .map(|data_type| self.new_field(path, &field.name, data_type))
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(Fields::from)
                    .map(DataType::Struct)
            }

            AvroSchema::Fixed(schema) => i32::try_from(schema.size)
                .map(DataType::FixedSizeBinary)
                .map_err(Into::into),

            AvroSchema::Decimal(schema) => u8::try_from(schema.precision)
                .and_then(|precision| {
                    i8::try_from(schema.scale).map(|scale| {
                        if precision <= 16 {
                            DataType::Decimal128(precision, scale)
                        } else {
                            DataType::Decimal256(precision, scale)
                        }
                    })
                })
                .map_err(Into::into),

            AvroSchema::BigDecimal => Ok(DataType::Utf8),

            AvroSchema::Date => Ok(DataType::Date32),

            AvroSchema::TimeMillis => Ok(DataType::Time32(TimeUnit::Millisecond)),

            AvroSchema::TimeMicros => Ok(DataType::Time64(TimeUnit::Microsecond)),

            AvroSchema::TimestampMillis => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),

            AvroSchema::TimestampMicros => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),

            AvroSchema::TimestampNanos => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),

            AvroSchema::LocalTimestampMillis => {
                Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
            }

            AvroSchema::LocalTimestampMicros => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }

            AvroSchema::LocalTimestampNanos => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),

            AvroSchema::Duration => Ok(DataType::Struct(Fields::from_iter([
                Field::new("month", DataType::UInt32, NULLABLE),
                Field::new("days", DataType::UInt32, NULLABLE),
                Field::new("milliseconds", DataType::UInt32, NULLABLE),
            ]))),

            AvroSchema::Ref { name } => Err(Error::Message(format!(
                "Avro Arrow schema conversion cannot resolve reference {}",
                name.name
            ))),
        }
    }
}
