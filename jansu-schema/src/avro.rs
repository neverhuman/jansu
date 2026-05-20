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

//! AVRO schema

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use std::collections::HashMap;

use apache_avro::schema::Schema as AvroSchema;
use bytes::Bytes;

use jansu_sans_io::record::inflated::Batch;
use serde_json::{Map, Value as JsonValue};
use tracing::{debug, info};

use crate::{AsJsonValue, AsKafkaRecord, Generator, Result, Validator};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use apache_avro::schema::RecordSchema;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
mod arrow;

mod convert;
mod value;

use convert::{from_json, generated_value};
use value::{decode, json_value, validate};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use convert::field_ids;

#[doc(hidden)]
pub use value::{r, schema_write};

mod parse;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum MessageKind {
    Key,
    Meta,
    Value,
}

impl AsRef<str> for MessageKind {
    fn as_ref(&self) -> &str {
        match self {
            MessageKind::Key => "key",
            MessageKind::Meta => "meta",
            MessageKind::Value => "value",
        }
    }
}

/// AVRO Schema
#[derive(Clone, Debug, Default)]
pub struct Schema {
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    complete: Option<RecordSchema>,
    pub(crate) key: Option<AvroSchema>,
    pub(crate) value: Option<AvroSchema>,
    pub(crate) meta: Option<AvroSchema>,

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    ids: HashMap<String, i32>,
}

impl Schema {
    pub fn key(&self) -> Option<&AvroSchema> {
        self.key.as_ref()
    }

    pub fn value(&self) -> Option<&AvroSchema> {
        self.value.as_ref()
    }

    pub fn meta(&self) -> Option<&AvroSchema> {
        self.meta.as_ref()
    }
}

impl Schema {
    fn to_json_value(
        &self,
        message_kind: MessageKind,
        schema: Option<&AvroSchema>,
        encoded: Option<Bytes>,
    ) -> Result<(String, JsonValue)> {
        decode(schema, encoded).and_then(|decoded| {
            decoded.map_or(
                Ok((message_kind.as_ref().to_owned(), JsonValue::Null)),
                |value| json_value(value).map(|value| (message_kind.as_ref().to_owned(), value)),
            )
        })
    }
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(self.key.as_ref(), record.key.clone())
                .and(validate(self.value.as_ref(), record.value.clone()))
                .inspect_err(|err| info!(?err, ?batch))?
        }

        Ok(())
    }
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &JsonValue) -> Result<jansu_sans_io::record::Builder> {
        let mut builder = jansu_sans_io::record::Record::builder();

        if let Some(value) = value.get(MessageKind::Key.as_ref()) {
            debug!(?value);

            if let Some(ref schema) = self.key {
                builder = builder.key(
                    from_json(schema, value)
                        .and_then(|value| schema_write(schema, value))
                        .map(Into::into)?,
                );
            }
        }

        if let Some(value) = value.get(MessageKind::Value.as_ref()) {
            debug!(?value);

            if let Some(ref schema) = self.value {
                builder = builder.value(
                    from_json(schema, value)
                        .and_then(|value| schema_write(schema, value))
                        .map(Into::into)?,
                );
            }
        }

        Ok(builder)
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<jansu_sans_io::record::Builder> {
        let mut builder = jansu_sans_io::record::Record::builder();

        if let Some(schema) = self.key.as_ref() {
            builder = builder.key(schema_write(schema, generated_value(schema)?)?.into());
        }

        if let Some(schema) = self.value.as_ref() {
            builder = builder.value(schema_write(schema, generated_value(schema)?)?.into());
        }

        Ok(builder)
    }
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<JsonValue> {
        Ok(JsonValue::Array(
            batch
                .records
                .iter()
                .map(|record| {
                    JsonValue::Object(Map::from_iter(
                        self.to_json_value(MessageKind::Key, self.key.as_ref(), record.key.clone())
                            .into_iter()
                            .chain(self.to_json_value(
                                MessageKind::Value,
                                self.value.as_ref(),
                                record.value.clone(),
                            )),
                    ))
                })
                .collect::<Vec<_>>(),
        ))
    }
}

#[cfg(test)]
mod tests;
