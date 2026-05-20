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

//! AVRO JSON value conversion helpers

use std::collections::HashMap;

use apache_avro::{
    Days, Decimal, Duration as AvroDuration, Millis, Months, schema::Schema as AvroSchema,
    types::Value,
};
use chrono::NaiveDateTime;
use serde_json::Value as JsonValue;
use tracing::debug;
use uuid::Uuid;

use crate::{Error, Result};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
pub(super) fn field_ids(schema: &AvroSchema) -> HashMap<String, i32> {
    use crate::ARROW_LIST_FIELD_NAME;

    fn field_ids_with_path(
        path: &[&str],
        schema: &AvroSchema,
        id: &mut i32,
    ) -> HashMap<String, i32> {
        debug!(?path, ?schema, id);

        let mut ids = HashMap::new();

        match schema {
            AvroSchema::Array(inner) => {
                let mut path = Vec::from(path);
                path.push(ARROW_LIST_FIELD_NAME);
                _ = ids.insert(path.join("."), *id);
                *id += 1;

                ids.extend(field_ids_with_path(&path[..], &inner.items, id));
            }

            AvroSchema::Map(inner) => {
                let mut path = Vec::from(path);
                path.push("entries");
                _ = ids.insert(path.join("."), *id);
                *id += 1;

                {
                    let mut path = path.clone();
                    path.push("keys");
                    _ = ids.insert(path.join("."), *id);
                    *id += 1;
                }

                {
                    let mut path = path.clone();
                    path.push("values");
                    _ = ids.insert(path.join("."), *id);
                    *id += 1;

                    ids.extend(field_ids_with_path(&path[..], &inner.types, id))
                }
            }

            AvroSchema::Record(inner) => {
                for field in inner.fields.iter() {
                    let mut path = Vec::from(path);
                    path.push(field.name.as_str());

                    _ = ids.insert(path.join("."), *id);
                    *id += 1;
                }

                for field in inner.fields.iter() {
                    let mut path = Vec::from(path);
                    path.push(field.name.as_str());
                    ids.extend(field_ids_with_path(&path[..], &field.schema, id))
                }
            }

            _ => (),
        }

        ids
    }

    field_ids_with_path(&[], schema, &mut 1)
}

pub(super) fn from_json(schema: &AvroSchema, json: &JsonValue) -> Result<Value> {
    debug!(?schema, ?json);

    match (schema, json) {
        (AvroSchema::Null, JsonValue::Null) => Ok(Value::Null),

        (AvroSchema::Boolean, JsonValue::Bool(value)) => Ok(Value::Boolean(*value)),

        (AvroSchema::Int, JsonValue::Number(value)) => value
            .as_i64()
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                Box::new(json.to_owned()),
            ))
            .and_then(|value| i32::try_from(value).map_err(Into::into))
            .map(Value::Int)
            .inspect_err(|err| debug!(?schema, ?json, ?err)),

        (AvroSchema::Long, JsonValue::Number(value)) => value
            .as_i64()
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                Box::new(json.to_owned()),
            ))
            .map(Value::Long),

        (AvroSchema::Double, JsonValue::Number(value)) => value
            .as_f64()
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                Box::new(json.to_owned()),
            ))
            .map(Value::Double)
            .inspect_err(|err| debug!(?schema, ?json, ?err)),

        (AvroSchema::Float, JsonValue::Number(value)) => value
            .as_f64()
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                Box::new(json.to_owned()),
            ))
            .map(|double| double as f32)
            .map(Value::Float)
            .inspect_err(|err| debug!(?schema, ?json, ?err)),

        (AvroSchema::Uuid, JsonValue::String(value)) => {
            Uuid::parse_str(value).map_err(Into::into).map(Value::Uuid)
        }

        (AvroSchema::Bytes, JsonValue::String(value)) => {
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }

        (AvroSchema::TimestampMillis, JsonValue::String(value)) => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|date_time| date_time.and_utc().timestamp_millis())
                .map(Value::TimestampMillis)
                .inspect_err(|err| debug!(?err, value))
                .map_err(Into::into)
        }

        (AvroSchema::TimestampMicros, JsonValue::String(value)) => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|date_time| date_time.and_utc().timestamp_micros())
                .map(Value::TimestampMicros)
                .inspect_err(|err| debug!(?err, value))
                .map_err(Into::into)
        }

        (AvroSchema::TimestampNanos, JsonValue::String(value)) => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .inspect_err(|err| debug!(?err, value))
                .map_err(Into::into)
                .and_then(|date_time| {
                    date_time
                        .and_utc()
                        .timestamp_nanos_opt()
                        .ok_or(Error::JsonToAvro(
                            Box::new(schema.to_owned()),
                            Box::new(json.to_owned()),
                        ))
                })
                .map(Value::TimestampNanos)
        }

        (AvroSchema::Enum(inner), JsonValue::String(value)) => inner
            .symbols
            .iter()
            .enumerate()
            .find(|(_, symbol)| *symbol == value)
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                Box::new(json.to_owned()),
            ))
            .and_then(|(index, symbol)| {
                u32::try_from(index)
                    .map(|index| (index, symbol))
                    .map_err(Into::into)
            })
            .map(|(index, symbol)| Value::Enum(index, symbol.to_owned())),

        (AvroSchema::String, JsonValue::String(value)) => Ok(Value::String(value.to_owned())),

        (AvroSchema::Array(schema), JsonValue::Array(values)) => values
            .iter()
            .map(|value| from_json(schema.items.as_ref(), value))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array)
            .inspect_err(|err| debug!(?schema, ?json, ?err)),

        (AvroSchema::Map(inner), JsonValue::Object(values)) => values
            .iter()
            .map(|(k, v)| from_json(inner.types.as_ref(), v).map(|v| (k.to_owned(), v)))
            .collect::<Result<HashMap<_, _>>>()
            .map(Value::Map),

        (AvroSchema::Record(record), JsonValue::Object(value)) => record
            .fields
            .iter()
            .map(|field| {
                value
                    .get(&field.name)
                    .ok_or(Error::JsonToAvroFieldNotFound {
                        schema: Box::new(schema.to_owned()),
                        value: Box::new(json.to_owned()),
                        field: field.name.clone(),
                    })
                    .and_then(|value| from_json(&field.schema, value))
                    .inspect(|value| debug!(name = ?field.name, ?value))
                    .map(|value| (field.name.clone(), value))
            })
            .collect::<Result<Vec<_>>>()
            .map(Value::Record)
            .inspect_err(|err| debug!(%err)),

        (schema, value) => Err(Error::JsonToAvro(
            Box::new(schema.to_owned()),
            Box::new(value.to_owned()),
        )),
    }
}

pub(super) fn generated_value(schema: &AvroSchema) -> Result<Value> {
    match schema {
        AvroSchema::Null => Ok(Value::Null),
        AvroSchema::Boolean => Ok(Value::Boolean(false)),
        AvroSchema::Int => Ok(Value::Int(0)),
        AvroSchema::Long => Ok(Value::Long(0)),
        AvroSchema::Float => Ok(Value::Float(0.0)),
        AvroSchema::Double => Ok(Value::Double(0.0)),
        AvroSchema::Bytes => Ok(Value::Bytes(vec![])),
        AvroSchema::String => Ok(Value::String(String::new())),
        AvroSchema::Array(_) => Ok(Value::Array(vec![])),
        AvroSchema::Map(_) => Ok(Value::Map(HashMap::new())),
        AvroSchema::Union(union) => {
            let variant = union
                .variants()
                .iter()
                .enumerate()
                .find(|(_, schema)| !matches!(schema, AvroSchema::Null))
                .or_else(|| union.variants().iter().enumerate().next());

            match variant {
                Some((index, schema)) => u32::try_from(index)
                    .map_err(Into::into)
                    .and_then(|index| generated_value(schema).map(|value| (index, value)))
                    .map(|(index, value)| Value::Union(index, Box::new(value))),
                None => Err(Error::Message("empty Avro union".into())),
            }
        }
        AvroSchema::Record(record) => record
            .fields
            .iter()
            .map(|field| {
                match field.default.as_ref() {
                    Some(default) => from_json(&field.schema, default),
                    None => generated_value(&field.schema),
                }
                .map(|value| (field.name.clone(), value))
            })
            .collect::<Result<Vec<_>>>()
            .map(Value::Record),
        AvroSchema::Enum(inner) => match inner
            .default
            .as_ref()
            .or_else(|| inner.symbols.first())
            .cloned()
        {
            Some(symbol) => Ok(Value::Enum(0, symbol)),
            None => Err(Error::Message(format!(
                "Avro enum {} has no symbols",
                inner.name.name
            ))),
        },
        AvroSchema::Fixed(inner) => Ok(Value::Fixed(inner.size, vec![0; inner.size])),
        AvroSchema::Decimal(decimal) => {
            let len = match decimal.inner.as_ref() {
                AvroSchema::Fixed(fixed) => fixed.size,
                _ => 1,
            };
            Ok(Value::Decimal(Decimal::from(vec![0; len])))
        }
        AvroSchema::BigDecimal => Err(Error::Message(
            "Avro big-decimal generation requires an explicit value".into(),
        )),
        AvroSchema::Uuid => Ok(Value::Uuid(Uuid::nil())),
        AvroSchema::Date => Ok(Value::Date(0)),
        AvroSchema::TimeMillis => Ok(Value::TimeMillis(0)),
        AvroSchema::TimeMicros => Ok(Value::TimeMicros(0)),
        AvroSchema::TimestampMillis => Ok(Value::TimestampMillis(0)),
        AvroSchema::TimestampMicros => Ok(Value::TimestampMicros(0)),
        AvroSchema::TimestampNanos => Ok(Value::TimestampNanos(0)),
        AvroSchema::LocalTimestampMillis => Ok(Value::LocalTimestampMillis(0)),
        AvroSchema::LocalTimestampMicros => Ok(Value::LocalTimestampMicros(0)),
        AvroSchema::LocalTimestampNanos => Ok(Value::LocalTimestampNanos(0)),
        AvroSchema::Duration => Ok(Value::Duration(AvroDuration::new(
            Months::new(0),
            Days::new(0),
            Millis::new(0),
        ))),
        AvroSchema::Ref { name } => Err(Error::Message(format!(
            "unresolved Avro schema reference: {}",
            name.name
        ))),
    }
}
