// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

//! JSON schema

use std::collections::BTreeMap;

use crate::{AsJsonValue, AsKafkaRecord, Error, Generator, Result, Validator};

use bytes::Bytes;

use serde_json::{Map, Value};

use jansu_sans_io::record::inflated::Batch;
use tracing::{debug, instrument};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
mod arrow;

mod codec;
use codec::{decode_json_record_value, field_ids, generate_json_value, validate};

#[derive(Debug, Default)]
pub struct Schema {
    key: Option<jsonschema::Validator>,
    value: Option<jsonschema::Validator>,
    key_schema: Option<Value>,
    value_schema: Option<Value>,

    #[allow(dead_code)]
    ids: BTreeMap<String, i32>,
}

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

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(encoded: Bytes) -> Result<Self, Self::Error> {
        debug!(encoded = &encoded[..]);
        const PROPERTIES: &str = "properties";

        let mut schema = serde_json::from_slice::<Value>(&encoded[..])?;

        let key = schema
            .get(PROPERTIES)
            .and_then(|properties| properties.get(MessageKind::Key.as_ref()))
            .inspect(|key| debug!(?key))
            .and_then(|key| jsonschema::validator_for(key).ok());
        let key_schema = schema
            .get(PROPERTIES)
            .and_then(|properties| properties.get(MessageKind::Key.as_ref()))
            .cloned();

        let value = schema
            .get(PROPERTIES)
            .and_then(|properties| properties.get(MessageKind::Value.as_ref()))
            .inspect(|value| debug!(?value))
            .and_then(|value| jsonschema::validator_for(value).ok());
        let value_schema = schema
            .get(PROPERTIES)
            .and_then(|properties| properties.get(MessageKind::Value.as_ref()))
            .cloned();

        let meta =
            serde_json::from_slice::<Value>(&Bytes::from_static(include_bytes!("meta.json")))
                .inspect(|meta| debug!(%meta))?;

        _ = schema
            .get_mut(PROPERTIES)
            .and_then(|properties| properties.as_object_mut())
            .inspect(|properties| debug!(?properties))
            .and_then(|object| object.insert(MessageKind::Meta.as_ref().to_owned(), meta));

        let ids = field_ids(&schema);
        debug!(?ids);

        Ok(Self {
            key,
            value,
            key_schema,
            value_schema,
            ids,
        })
    }
}

impl Validator for Schema {
    #[instrument(skip(self, batch), ret)]
    fn validate(&self, batch: &Batch) -> Result<()> {
        for record in &batch.records {
            debug!(?record);

            validate(self.key.as_ref(), record.key.clone())
                .and(validate(self.value.as_ref(), record.value.clone()))?
        }

        Ok(())
    }
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<jansu_sans_io::record::Builder> {
        let mut builder = jansu_sans_io::record::Record::builder();

        if let Some(value) = value.get(MessageKind::Key.as_ref()) {
            debug!(?value);

            if self.key.is_some() {
                builder = builder.key(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?);
            }
        }

        if let Some(value) = value.get(MessageKind::Value.as_ref()) {
            debug!(?value);

            if self.value.is_some() {
                builder =
                    builder.value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?);
            }
        }

        Ok(builder)
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<jansu_sans_io::record::Builder> {
        let mut builder = jansu_sans_io::record::Record::builder();

        if let Some(schema) = self.key_schema.as_ref() {
            builder = builder.key(
                serde_json::to_vec(&generate_json_value(schema))
                    .map(Bytes::from)
                    .map(Into::into)?,
            );
        }

        if let Some(schema) = self.value_schema.as_ref() {
            builder = builder.value(
                serde_json::to_vec(&generate_json_value(schema))
                    .map(Bytes::from)
                    .map(Into::into)?,
            );
        }

        Ok(builder)
    }
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<Value> {
        Ok(Value::Array(
            batch
                .records
                .iter()
                .map(|record| {
                    let key = decode_json_record_value(record.key.clone())?;
                    let value = decode_json_record_value(record.value.clone())?;

                    Ok(Value::Object(Map::from_iter([
                        (MessageKind::Key.as_ref().to_owned(), key),
                        (MessageKind::Value.as_ref().to_owned(), value),
                    ])))
                })
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

#[cfg(test)]
mod tests;
