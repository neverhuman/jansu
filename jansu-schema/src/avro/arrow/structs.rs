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

use std::iter::zip;

use apache_avro::{
    schema::{RecordSchema, Schema as AvroSchema},
    types::Value,
};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder, NullBuilder, StringBuilder,
    StructBuilder, Time32MillisecondBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
};
use tracing::{debug, error};

use crate::{Error, Result};

use super::lists::{append_list_builder, append_map_builder};

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

            (AvroSchema::Union(union_schema), Value::Union(_, _value)) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!(
                        "Avro union struct field {name:?} is not supported: {union_schema:?}"
                    ),
                });
            }

            (AvroSchema::Record(schema), Value::Record(items)) => builder
                .field_builder::<StructBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .and_then(|builder| append_struct_builder(schema, items, builder))?,

            (AvroSchema::Fixed(fixed_schema), _) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!(
                        "Avro fixed struct field {name:?} is not supported: {fixed_schema:?}"
                    ),
                });
            }
            (AvroSchema::Decimal(decimal_schema), _) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!(
                        "Avro decimal struct field {name:?} is not supported: {decimal_schema:?}"
                    ),
                });
            }
            (AvroSchema::BigDecimal, _) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!("Avro BigDecimal struct field {name:?} is not supported"),
                });
            }

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

            (AvroSchema::LocalTimestampMillis, _) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!(
                        "Avro LocalTimestampMillis struct field {name:?} is not supported"
                    ),
                });
            }
            (AvroSchema::LocalTimestampMicros, _) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!(
                        "Avro LocalTimestampMicros struct field {name:?} is not supported"
                    ),
                });
            }
            (AvroSchema::LocalTimestampNanos, _) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!(
                        "Avro LocalTimestampNanos struct field {name:?} is not supported"
                    ),
                });
            }
            (AvroSchema::Duration, _) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!(
                        "Avro Duration struct field {name:?} is not supported"
                    ),
                });
            }
            (AvroSchema::Ref { name: ref_name }, _) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!(
                        "Avro schema reference {ref_name:?} struct field {name:?} cannot be resolved"
                    ),
                });
            }
            (schema, value) => {
                return Err(Error::NotImplemented {
                    kind: "avro_to_arrow",
                    detail: format!(
                        "unsupported Avro struct field {name:?} for schema {schema:?} value {value:?}"
                    ),
                });
            }
        }
    }

    builder.append(true);
    Ok(())
}
