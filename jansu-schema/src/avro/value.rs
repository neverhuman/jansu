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

//! AVRO decode, validate and Avro-to-JSON value helpers

use apache_avro::{Reader, schema::Schema as AvroSchema, types::Value};
use bytes::Bytes;
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};
use serde_json::{Map, Number, Value as JsonValue};

use jansu_sans_io::ErrorCode;
use tracing::debug;

use crate::{Error, Result};

pub(super) fn decode(
    validator: Option<&AvroSchema>,
    encoded: Option<Bytes>,
) -> Result<Option<Value>> {
    debug!(?validator, ?encoded);
    validator.map_or(Ok(None), |schema| {
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            Reader::with_schema(schema, &encoded[..])
                .and_then(|reader| reader.into_iter().next().transpose())
                .inspect(|value| debug!(?value))
                .inspect_err(|err| debug!(?err))
                .map_err(|_| Error::Api(ErrorCode::InvalidRecord))
                .and_then(|value| value.ok_or(Error::Api(ErrorCode::InvalidRecord)))
                .map(Some)
        })
    })
}

pub(super) fn validate(validator: Option<&AvroSchema>, encoded: Option<Bytes>) -> Result<()> {
    decode(validator, encoded).and(Ok(()))
}

pub(super) fn json_value(value: Value) -> Result<JsonValue> {
    match value {
        Value::Null => Ok(JsonValue::Null),

        Value::Boolean(inner) => Ok(JsonValue::Bool(inner)),

        Value::Int(inner) => Ok(JsonValue::Number(Number::from(inner))),

        Value::Long(inner) => Ok(JsonValue::Number(Number::from(inner))),

        Value::Float(inner) => Number::from_f64(inner as f64)
            .ok_or(Error::AvroToJson(value.to_owned()))
            .map(JsonValue::Number),

        Value::Double(inner) => Number::from_f64(inner)
            .ok_or(Error::AvroToJson(value.to_owned()))
            .map(JsonValue::Number),

        Value::Bytes(inner) => Ok(JsonValue::String(String::from(String::from_utf8_lossy(
            &inner[..],
        )))),

        Value::String(inner) | Value::Enum(_, inner) => Ok(JsonValue::String(inner)),

        Value::Fixed(_, inner) => Ok(JsonValue::String(String::from_utf8_lossy(&inner).into())),

        Value::Union(_, value) => json_value(*value),

        Value::Array(values) => values
            .into_iter()
            .map(json_value)
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array),

        Value::Map(inner) => inner
            .into_iter()
            .map(|(k, v)| json_value(v).map(|v| (k, v)))
            .collect::<Result<Vec<_>>>()
            .map(Map::from_iter)
            .map(JsonValue::Object),

        Value::Record(inner) => inner
            .into_iter()
            .map(|(k, v)| json_value(v).map(|v| (k, v)))
            .collect::<Result<Vec<_>>>()
            .map(Map::from_iter)
            .map(JsonValue::Object),

        Value::Date(days) => NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|epoch| epoch.checked_add_signed(Duration::days(i64::from(days))))
            .map(|date| JsonValue::String(date.to_string()))
            .ok_or(Error::AvroToJson(value.to_owned())),

        Value::Decimal(decimal) => {
            Vec::<u8>::try_from(&decimal)
                .map_err(Error::from)
                .map(|bytes| {
                    JsonValue::Array(
                        bytes
                            .into_iter()
                            .map(|byte| JsonValue::Number(Number::from(byte)))
                            .collect(),
                    )
                })
        }
        Value::BigDecimal(big_decimal) => Ok(JsonValue::String(big_decimal.to_string())),

        Value::TimeMillis(millis) => time_json(
            i64::from(millis).div_euclid(1_000),
            u32::try_from(i64::from(millis).rem_euclid(1_000) * 1_000_000)?,
            value,
        ),
        Value::TimeMicros(micros) => time_json(
            micros.div_euclid(1_000_000),
            u32::try_from(micros.rem_euclid(1_000_000) * 1_000)?,
            value,
        ),

        Value::TimestampMillis(millis) => DateTime::<Utc>::from_timestamp_millis(millis)
            .map(|timestamp| JsonValue::String(timestamp.to_rfc3339()))
            .ok_or(Error::AvroToJson(value.to_owned())),
        Value::TimestampMicros(micros) => DateTime::<Utc>::from_timestamp_micros(micros)
            .map(|timestamp| JsonValue::String(timestamp.to_rfc3339()))
            .ok_or(Error::AvroToJson(value.to_owned())),
        Value::TimestampNanos(nanos) => timestamp_nanos_json(nanos, value),

        Value::LocalTimestampMillis(millis) => DateTime::<Utc>::from_timestamp_millis(millis)
            .map(|timestamp| JsonValue::String(timestamp.naive_utc().to_string()))
            .ok_or(Error::AvroToJson(value.to_owned())),
        Value::LocalTimestampMicros(micros) => DateTime::<Utc>::from_timestamp_micros(micros)
            .map(|timestamp| JsonValue::String(timestamp.naive_utc().to_string()))
            .ok_or(Error::AvroToJson(value.to_owned())),
        Value::LocalTimestampNanos(nanos) => timestamp_nanos_json(nanos, value),

        Value::Duration(duration) => Ok(JsonValue::Array(
            <[u8; 12]>::from(duration)
                .into_iter()
                .map(|byte| JsonValue::Number(Number::from(byte)))
                .collect(),
        )),

        Value::Uuid(uuid) => json_value(Value::String(uuid.to_string())),
    }
}

pub(super) fn time_json(seconds: i64, nanos: u32, original: Value) -> Result<JsonValue> {
    u32::try_from(seconds)
        .ok()
        .and_then(|seconds| NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos))
        .map(|time| JsonValue::String(time.to_string()))
        .ok_or(Error::AvroToJson(original))
}

pub(super) fn timestamp_nanos_json(nanos: i64, original: Value) -> Result<JsonValue> {
    DateTime::<Utc>::from_timestamp(
        nanos.div_euclid(1_000_000_000),
        u32::try_from(nanos.rem_euclid(1_000_000_000))?,
    )
    .map(|timestamp| JsonValue::String(timestamp.to_rfc3339()))
    .ok_or(Error::AvroToJson(original))
}

#[doc(hidden)]
pub fn r<'a>(
    schema: &AvroSchema,
    fields: impl IntoIterator<Item = (&'a str, Value)>,
) -> apache_avro::types::Record<'_> {
    apache_avro::types::Record::new(schema)
        .map(|mut record| {
            for (name, value) in fields {
                record.put(name, value);
            }
            record
        })
        .unwrap()
}

#[doc(hidden)]
pub fn schema_write(schema: &AvroSchema, value: Value) -> Result<Bytes> {
    debug!(?schema, ?value);
    let mut writer = apache_avro::Writer::new(schema, vec![]);
    _ = writer.append(value)?;
    writer.into_inner().map(Bytes::from).map_err(Into::into)
}
