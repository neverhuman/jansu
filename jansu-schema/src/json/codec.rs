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

//! JSON schema encode, decode and field identification

use std::collections::BTreeMap;

use bytes::Bytes;
use serde_json::{Map, Number, Value};

use jansu_sans_io::ErrorCode;
use tracing::{debug, instrument, warn};

use crate::{ARROW_LIST_FIELD_NAME, Error, Result};

use super::MessageKind;

pub(super) fn validate(
    validator: Option<&jsonschema::Validator>,
    encoded: Option<Bytes>,
) -> Result<()> {
    debug!(validator = ?validator, ?encoded);

    validator
        .map_or(Ok(()), |validator| {
            encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
                serde_json::from_reader(&encoded[..])
                    .map_err(|err| {
                        warn!(?err, ?encoded);
                        Error::Api(ErrorCode::InvalidRecord)
                    })
                    .inspect(|instance| debug!(?instance))
                    .and_then(|instance| {
                        validator
                            .validate(&instance)
                            .inspect_err(|err| warn!(?err, ?validator, %instance))
                            .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
                    })
            })
        })
        .inspect(|r| debug!(?r))
        .inspect_err(|err| warn!(?err))
}

pub(super) fn decode_json_record_value(encoded: Option<Bytes>) -> Result<Value> {
    encoded.map_or(Ok(Value::Null), |encoded| {
        serde_json::from_slice(&encoded[..]).map_err(Into::into)
    })
}

pub(super) fn generate_json_value(schema: &Value) -> Value {
    if let Some(default) = schema.get("default") {
        return default.clone();
    }

    if let Some(r#const) = schema.get("const") {
        return r#const.clone();
    }

    if let Some(first_enum) = schema
        .get("enum")
        .and_then(|items| items.as_array())
        .and_then(|items| items.first())
    {
        return first_enum.clone();
    }

    match schema.get("type") {
        Some(Value::Array(types)) => types
            .iter()
            .find(|candidate| candidate.as_str() != Some("null"))
            .map_or(Value::Null, generate_json_value),

        Some(Value::Object(nested)) => generate_json_value(&Value::Object(nested.clone())),

        Some(Value::String(kind)) => match kind.as_str() {
            "null" => Value::Null,
            "boolean" => Value::Bool(false),
            "integer" => Value::Number(Number::from(0)),
            "number" => Value::Number(Number::from(0)),
            "string" => Value::String(String::new()),
            "array" => schema
                .get("items")
                .map(generate_json_value)
                .map(|value| Value::Array(vec![value]))
                .unwrap_or_else(|| Value::Array(vec![])),
            "object" => Value::Object(Map::from_iter(
                schema
                    .get("properties")
                    .and_then(|properties| properties.as_object())
                    .into_iter()
                    .flat_map(|properties| properties.iter())
                    .map(|(name, schema)| (name.clone(), generate_json_value(schema))),
            )),
            _ => Value::Null,
        },

        _ => {
            if let Some(properties) = schema.get("properties").and_then(|value| value.as_object()) {
                Value::Object(Map::from_iter(
                    properties
                        .iter()
                        .map(|(name, schema)| (name.clone(), generate_json_value(schema))),
                ))
            } else {
                Value::Null
            }
        }
    }
}

#[instrument(skip(schema), ret)]
pub(super) fn field_ids(schema: &Value) -> BTreeMap<String, i32> {
    fn field_ids_with_path(path: &[&str], schema: &Value, id: &mut i32) -> BTreeMap<String, i32> {
        debug!(?path, %schema, id);

        let mut ids = BTreeMap::new();

        match schema.get("type").and_then(|r#type| r#type.as_str()) {
            Some("object") => {
                if let Some(properties) = schema
                    .get("properties")
                    .and_then(|properties| properties.as_object())
                {
                    for (k, v) in properties {
                        let mut path = Vec::from(path);
                        path.push(k);

                        _ = ids.insert(path.join("."), *id);
                        *id += 1;

                        ids.extend(field_ids_with_path(&path[..], v, id))
                    }
                }
            }

            Some("array") => {
                let mut path = Vec::from(path);
                path.push(ARROW_LIST_FIELD_NAME);
                _ = ids.insert(path.join("."), *id);
                *id += 1;

                if let Some(items) = schema.get("items") {
                    debug!(?items);

                    ids.extend(field_ids_with_path(&path[..], items, id))
                }
            }

            None | Some(_) => (),
        }

        ids
    }

    let mut ids = BTreeMap::new();
    let mut id = 1;
    let kinds = [MessageKind::Meta, MessageKind::Key, MessageKind::Value];

    for kind in kinds {
        if schema
            .get("properties")
            .and_then(|schema| schema.get(kind.as_ref()))
            .inspect(|schema| debug!(?kind, ?schema))
            .is_some()
        {
            _ = ids.insert(kind.as_ref().into(), id);
            id += 1;
        }
    }

    for kind in kinds {
        if let Some(schema) = schema
            .get("properties")
            .and_then(|schema| schema.get(kind.as_ref()))
            .inspect(|schema| debug!(?kind, ?schema))
        {
            ids.extend(field_ids_with_path(&[kind.as_ref()], schema, &mut id));
        }
    }

    ids
}
