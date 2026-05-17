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

use apache_avro::{
    Reader,
    schema::{Schema as AvroSchema, UnionSchema},
    types::Value,
};
use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
        Float32Builder, Float64Builder, Int32Builder, Int64Builder, LargeBinaryBuilder,
        ListBuilder, MapBuilder, NullBuilder, StringBuilder, StructBuilder,
        Time32MillisecondBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        UInt32Builder,
    },
    datatypes::{
        DataType, Field, FieldRef, Fields, Schema as ArrowSchema, TimeUnit, UnionFields, UnionMode,
    },
    record_batch::RecordBatch,
};
use bytes::Bytes;
use chrono::{DateTime, Datelike};
use jansu_sans_io::{ErrorCode, record::inflated::Batch};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use tracing::{debug, error, instrument};

use crate::{
    ARROW_LIST_FIELD_NAME, AsArrow, Error, Result,
    avro::{Schema, r, schema_write},
    lake::LakeHouseType,
};

const NULLABLE: bool = true;
const SORTED_MAP_KEYS: bool = false;

pub(super) trait NullableVariant {
    fn nullable_variant(&self) -> Option<&AvroSchema>;
}

impl NullableVariant for UnionSchema {
    fn nullable_variant(&self) -> Option<&AvroSchema> {
        if self.variants().len() == 2
            && self
                .variants()
                .iter()
                .inspect(|variant| debug!(?variant))
                .any(|schema| matches!(schema, AvroSchema::Null))
        {
            self.variants()
                .iter()
                .find(|schema| !matches!(schema, AvroSchema::Null))
                .inspect(|schema| debug!(?schema))
        } else {
            None
        }
    }
}

fn append<'a>(path: &[&'a str], name: &'a str) -> Vec<&'a str> {
    let mut path = Vec::from(path);
    path.push(name);
    path
}

impl Schema {
    fn new_list_field(&self, path: &[&str], data_type: DataType) -> Field {
        self.new_field(path, ARROW_LIST_FIELD_NAME, data_type)
    }

    fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        self.new_nullable_field(path, name, data_type, NULLABLE)
    }

    fn new_nullable_field(
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

    fn schema_data_type(&self, path: &[&str], schema: &AvroSchema) -> Result<DataType> {
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

            AvroSchema::BigDecimal => Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from(
                    "BigDecimal Avro logical type is not mapped to an Arrow DataType",
                ),
            }),

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

            AvroSchema::Ref { name } => Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: format!(
                    "Avro schema reference {name:?} cannot be resolved to an Arrow DataType"
                ),
            }),
        }
    }

    fn schema_array_builder(
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
                    Err(Error::NotImplemented {
                        kind: "avro_to_arrow",
                        detail: format!(
                            "Avro union without a single nullable variant is not supported: {schema:?}"
                        ),
                    })
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
                .map(|precision| {
                    if precision <= 16 {
                        Box::new(Decimal128Builder::new()) as Box<dyn ArrayBuilder>
                    } else {
                        Box::new(Decimal256Builder::new()) as Box<dyn ArrayBuilder>
                    }
                })
                .map_err(Into::into),

            AvroSchema::BigDecimal => Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: String::from("BigDecimal Avro logical type has no Arrow array builder"),
            }),
            AvroSchema::Date => Ok(Box::new(Date32Builder::new())),
            AvroSchema::TimeMillis => Ok(Box::new(Time32MillisecondBuilder::new())),
            AvroSchema::TimeMicros => Ok(Box::new(Time64MicrosecondBuilder::new())),
            AvroSchema::TimestampMillis => Ok(Box::new(TimestampMillisecondBuilder::new())),
            AvroSchema::TimestampMicros => Ok(Box::new(TimestampMicrosecondBuilder::new())),
            AvroSchema::TimestampNanos => Ok(Box::new(TimestampNanosecondBuilder::new())),
            AvroSchema::LocalTimestampMillis => Ok(Box::new(Time32MillisecondBuilder::new())),
            AvroSchema::LocalTimestampMicros => Ok(Box::new(Time64MicrosecondBuilder::new())),
            AvroSchema::LocalTimestampNanos => Ok(Box::new(Time64NanosecondBuilder::new())),

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

            AvroSchema::Ref { name } => Err(Error::NotImplemented {
                kind: "avro_to_arrow",
                detail: format!("Avro schema reference {name:?} has no Arrow array builder"),
            }),
        }
    }
}

#[derive(Default)]
struct RecordBuilder(Vec<Box<dyn ArrayBuilder>>);

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

mod lists;
mod structs;
mod values;

use values::append_value;

fn process<'a, T>(
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

impl AsArrow for Schema {
    #[instrument(skip(self, batch), ret)]
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: LakeHouseType,
    ) -> Result<RecordBatch> {
        debug!(ids = ?self.ids);

        let schema = ArrowSchema::try_from(self)?;
        debug!(?schema);

        let mut record_builder = RecordBuilder::try_from(self)?;

        for record in &batch.records {
            debug!(?record);

            let mut builders = record_builder.0.iter_mut();

            process(self.key.as_ref(), record.key.clone(), &mut builders)?;

            process(self.value.as_ref(), record.value.clone(), &mut builders)?;

            process(
                self.meta.as_ref(),
                self.meta
                    .as_ref()
                    .map(|schema| {
                        schema_write(
                            schema,
                            r(
                                schema,
                                DateTime::from_timestamp_millis(
                                    batch.base_timestamp + record.timestamp_delta,
                                )
                                .map_or(
                                    [
                                        ("partition", Value::Int(partition)),
                                        (
                                            "timestamp",
                                            Value::Long(
                                                (batch.base_timestamp + record.timestamp_delta)
                                                    * 1_000,
                                            ),
                                        ),
                                        ("year", Value::Int(0)),
                                        ("month", Value::Int(0)),
                                        ("day", Value::Int(0)),
                                    ],
                                    |date_time| {
                                        [
                                            ("partition", Value::Int(partition)),
                                            (
                                                "timestamp",
                                                Value::Long(
                                                    (batch.base_timestamp + record.timestamp_delta)
                                                        * 1_000,
                                                ),
                                            ),
                                            ("year", Value::Int(date_time.date_naive().year())),
                                            (
                                                "month",
                                                Value::Int(date_time.date_naive().month() as i32),
                                            ),
                                            (
                                                "day",
                                                Value::Int(date_time.date_naive().day() as i32),
                                            ),
                                        ]
                                    },
                                ),
                            )
                            .into(),
                        )
                    })
                    .transpose()?,
                &mut builders,
            )?;
        }

        debug!(
            rows = ?record_builder.0.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
        );

        RecordBatch::try_new(
            schema.into(),
            record_builder
                .0
                .iter_mut()
                .map(|builder| builder.finish())
                .collect(),
        )
        .map_err(Into::into)
    }
}

impl TryFrom<&Schema> for Fields {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        schema
            .complete
            .as_ref()
            .map_or(Ok(vec![]), |complete| {
                complete
                    .fields
                    .iter()
                    .inspect(|field| debug!(?field))
                    .map(|field| {
                        schema
                            .schema_data_type(&[&field.name], &field.schema)
                            .map(|data_type| schema.new_field(&[], &field.name, data_type))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .map(Into::into)
    }
}

impl TryFrom<&Schema> for ArrowSchema {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        Fields::try_from(schema)
            .inspect(|fields| debug!(?fields))
            .map(ArrowSchema::new)
    }
}

#[cfg(test)]
mod tests;
