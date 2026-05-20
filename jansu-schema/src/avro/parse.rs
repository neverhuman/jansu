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

//! AVRO schema parsing from JSON definitions

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use std::collections::HashMap;

use apache_avro::schema::Schema as AvroSchema;
use bytes::Bytes;
use serde_json::{Map, Value as JsonValue};
use tracing::{debug, error};

use crate::{Error, Result};

use super::{MessageKind, Schema};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use super::field_ids;

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(encoded: Bytes) -> Result<Self, Self::Error> {
        const FIELDS: &str = "fields";

        let meta = serde_json::from_slice::<JsonValue>(&Bytes::from_static(include_bytes!(
            "../meta.avsc"
        )))
        .inspect(|meta| debug!(%meta))
        .map(|mut meta| meta[FIELDS].take())
        .inspect(|meta| debug!(%meta))?;

        serde_json::from_slice::<JsonValue>(&encoded[..])
            .map(|mut schema| {
                _ = schema
                    .get_mut(FIELDS)
                    .and_then(|fields| fields.as_object_mut())
                    .and_then(|object| object.insert(MessageKind::Meta.as_ref().to_owned(), meta));
                schema
            })
            .map_err(Into::into)
            .map(Self::from)
    }
}

impl From<JsonValue> for Schema {
    fn from(mut schema: JsonValue) -> Self {
        debug!(%schema);

        const FIELDS: &str = "fields";

        let meta = serde_json::from_slice::<JsonValue>(&Bytes::from_static(include_bytes!(
            "../meta.avsc"
        )))
        .inspect(|meta| debug!(%meta))
        .ok();

        let schema = {
            if let Some(meta) = meta
                && let Some(fields) = schema.get_mut(FIELDS)
                && let Some(array) = fields.as_array_mut()
            {
                array.push(JsonValue::Object(Map::from_iter([
                    ("name".into(), MessageKind::Meta.as_ref().into()),
                    ("type".into(), meta),
                ])))
            }

            debug!(%schema);

            schema
        };

        schema
            .get(FIELDS)
            .inspect(|fields| debug!(?fields))
            .and_then(|fields| fields.as_array())
            .inspect(|fields| debug!(?fields))
            .map_or(
                Self {
                    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
                    complete: None,
                    key: None,
                    value: None,
                    meta: None,
                    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
                    ids: HashMap::new(),
                },
                |fields| {
                    if let Ok(schema) =
                        AvroSchema::parse(&schema).inspect_err(|err| error!(?err, ?schema))
                    {
                        #[cfg(not(any(
                            feature = "parquet",
                            feature = "iceberg",
                            feature = "delta"
                        )))]
                        let _ = schema;

                        Self {
                            #[cfg(any(
                                feature = "parquet",
                                feature = "iceberg",
                                feature = "delta"
                            ))]
                            ids: field_ids(&schema),

                            #[cfg(any(
                                feature = "parquet",
                                feature = "iceberg",
                                feature = "delta"
                            ))]
                            complete: if let AvroSchema::Record(record) = schema {
                                Some(record)
                            } else {
                                None
                            },

                            key: fields
                                .iter()
                                .find(|field| {
                                    field
                                        .get("name")
                                        .is_some_and(|name| name == MessageKind::Key.as_ref())
                                })
                                .inspect(|value| debug!(?value))
                                .and_then(|schema| {
                                    AvroSchema::parse(schema)
                                        .inspect_err(|err| error!(?err, ?schema))
                                        .ok()
                                }),

                            value: fields
                                .iter()
                                .find(|field| {
                                    field
                                        .get("name")
                                        .is_some_and(|name| name == MessageKind::Value.as_ref())
                                })
                                .inspect(|value| debug!(?value))
                                .and_then(|schema| {
                                    AvroSchema::parse(schema)
                                        .inspect_err(|err| error!(?err, ?schema))
                                        .ok()
                                }),

                            meta: fields
                                .iter()
                                .find(|field| {
                                    field
                                        .get("name")
                                        .is_some_and(|name| name == MessageKind::Meta.as_ref())
                                })
                                .inspect(|value| debug!(?value))
                                .and_then(|schema| {
                                    AvroSchema::parse(schema)
                                        .inspect_err(|err| error!(?err, ?schema))
                                        .ok()
                                }),
                        }
                    } else {
                        Self {
                            #[cfg(any(
                                feature = "parquet",
                                feature = "iceberg",
                                feature = "delta"
                            ))]
                            complete: None,
                            key: None,
                            value: None,
                            meta: None,
                            #[cfg(any(
                                feature = "parquet",
                                feature = "iceberg",
                                feature = "delta"
                            ))]
                            ids: HashMap::new(),
                        }
                    }
                },
            )
    }
}
