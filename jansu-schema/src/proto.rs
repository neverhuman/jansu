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

//! Protocol Buffer message schema

use crate::{AsJsonValue, AsKafkaRecord, Error, Generator, Result, Validator};

use bytes::Bytes;

use jansu_sans_io::record::inflated::Batch;
use protobuf::reflect::FileDescriptor;
use protobuf_json_mapping::print_to_string;
use serde_json::{Map, Value};
use tracing::{debug, error};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
mod arrow;

mod codec;
mod config;
mod generate;
mod schema_methods;

use codec::{META_FILE_DESCRIPTOR, decode, make_fd, message_to_bytes, validate};
use config::FieldGeneratorConfiguration;
use generate::MessageGenerator;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MessageKind {
    Key,
    Meta,
    Value,
}

impl AsRef<str> for MessageKind {
    fn as_ref(&self) -> &str {
        match self {
            MessageKind::Key => "Key",
            MessageKind::Meta => "Meta",
            MessageKind::Value => "Value",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Schema {
    file_descriptors: Vec<FileDescriptor>,
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<jansu_sans_io::record::Builder> {
        debug!(?value);

        let mut builder = jansu_sans_io::record::Record::builder();

        if let Some(value) = value.get("key") {
            debug!(?value);

            if let Some(encoded) = self.message_value_as_bytes(MessageKind::Key, value)? {
                builder = builder.key(encoded.into());
            }
        };

        if let Some(value) = value.get("value") {
            debug!(?value);

            if let Some(encoded) = self.message_value_as_bytes(MessageKind::Value, value)? {
                builder = builder.value(encoded.into());
            }
        };

        Ok(builder)
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<jansu_sans_io::record::Builder> {
        let mut builder = jansu_sans_io::record::Record::builder();

        if let Some(generated) = self.generate_message_kind(MessageKind::Key)? {
            builder = builder.key(generated.into());
        }

        if let Some(generated) = self.generate_message_kind(MessageKind::Value)? {
            builder = builder.value(generated.into());
        }

        Ok(builder)
    }
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(
                self.message_by_package_relative_name(MessageKind::Key),
                record.key.clone(),
            )
            .and(validate(
                self.message_by_package_relative_name(MessageKind::Value),
                record.value.clone(),
            ))
            .inspect_err(|err| error!(?err))?
        }

        Ok(())
    }
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(proto: Bytes) -> Result<Self, Self::Error> {
        make_fd(proto)
            .map(|mut protos| {
                debug!(
                    protos = ?protos
                        .iter()
                        .flat_map(|proto| {
                            proto
                                .messages()
                                .map(|message| message.name_to_package().to_owned())
                        })
                        .collect::<Vec<_>>()
                );

                if let Some(mut meta) = META_FILE_DESCRIPTOR.clone() {
                    debug!(
                        meta = ?meta
                            .iter()
                            .flat_map(|proto| {
                                proto
                                    .messages()
                                    .map(|message| message.name_to_package().to_owned())
                            })
                            .collect::<Vec<_>>()
                    );

                    protos.append(&mut meta);
                }

                protos
            })
            .map(|file_descriptors| Self { file_descriptors })
    }
}

impl Schema {
    fn to_json_value(
        &self,
        message_kind: MessageKind,
        encoded: Option<Bytes>,
    ) -> Result<(String, Value)> {
        decode(self.message_by_package_relative_name(message_kind), encoded)
            .inspect(|decoded| debug!(?decoded))
            .and_then(|decoded| {
                decoded.map_or(
                    Ok((message_kind.as_ref().to_lowercase(), Value::Null)),
                    |message| {
                        print_to_string(message.as_ref())
                            .inspect(|s| debug!(s))
                            .map_err(Into::into)
                            .and_then(|s| serde_json::from_str::<Value>(&s).map_err(Into::into))
                            .map(|value| (message_kind.as_ref().to_lowercase(), value))
                            .inspect(|(k, v)| debug!(k, ?v))
                    },
                )
            })
    }
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<Value> {
        Ok(Value::Array(
            batch
                .records
                .iter()
                .inspect(|record| debug!(?record))
                .map(|record| {
                    Value::Object(Map::from_iter(
                        self.to_json_value(MessageKind::Key, record.key.clone())
                            .into_iter()
                            .chain(self.to_json_value(MessageKind::Value, record.value.clone())),
                    ))
                })
                .collect::<Vec<_>>(),
        ))
    }
}

#[cfg(test)]
mod tests;
