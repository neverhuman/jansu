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

//! JSON schema Arrow data type and builder inference

use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, NullBuilder,
        StringBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, FieldRef, Fields},
};
use serde_json::Value;
use tracing::{debug, error};

use crate::{ARROW_LIST_FIELD_NAME, Error, Result, json::Schema};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::{NULLABLE, append_path, sort_dedup};

impl Schema {
    pub(super) fn new_list_field(&self, path: &[&str], data_type: DataType) -> Field {
        self.new_field(path, ARROW_LIST_FIELD_NAME, data_type)
    }

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    pub(super) fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        debug!(?path, name, ?data_type, ids = ?self.ids);

        let path = {
            let mut path = Vec::from(path);
            path.push(name);
            path.join(".")
        };

        Field::new(name.to_owned(), data_type, NULLABLE).with_metadata(
            self.ids
                .get(path.as_str())
                .inspect(|field_id| debug!(?path, field_id))
                .map(|field_id| (PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string()))
                .into_iter()
                .collect(),
        )
    }

    #[cfg(not(any(feature = "parquet", feature = "iceberg", feature = "delta")))]
    pub(super) fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        debug!(?path, name, ?data_type, ids = ?self.ids);

        Field::new(name.to_owned(), data_type, NULLABLE)
    }

    pub(super) fn data_type(&self, path: &[&str], value: &Value) -> Result<DataType> {
        match value {
            Value::Null => Ok(DataType::Null),

            Value::Bool(_) => Ok(DataType::Boolean),

            Value::Number(value) => {
                if value.is_i64() | value.is_u64() {
                    Ok(DataType::Int64)
                } else {
                    Ok(DataType::Float64)
                }
            }

            Value::String(_) => Ok(DataType::Utf8),

            Value::Array(values) => self.common_data_type(path, values).map(|data_type| {
                DataType::List(FieldRef::new(self.new_list_field(path, data_type)))
            }),

            Value::Object(object) => object
                .iter()
                .map(|(k, v)| {
                    let child_path = {
                        let mut path = Vec::from(path);
                        path.push(k.as_str());
                        path
                    };

                    self.data_type(&child_path[..], v)
                        .map(|data_type| self.new_field(path, k, data_type))
                })
                .collect::<Result<Vec<_>>>()
                .map(Fields::from)
                .map(DataType::Struct),
        }
        .inspect(|data_type| debug!(?path, ?value, ?data_type))
        .inspect_err(|err| error!(?err, ?value))
    }

    pub(super) fn common_data_type(&self, path: &[&str], values: &[Value]) -> Result<DataType> {
        debug!(?path, ?values);

        values
            .iter()
            .map(|value| self.data_type(path, value))
            .inspect(|data_type| debug!(?data_type))
            .collect::<Result<Vec<_>>>()
            .map(sort_dedup)
            .inspect(|data_types| debug!(?data_types))
            .and_then(|mut data_types| {
                if data_types.len() > 1 {
                    Err(Error::NoCommonType(data_types))
                } else if let Some(data_type) = data_types.pop() {
                    Ok(data_type)
                } else {
                    Ok(DataType::Null)
                }
            })
            .inspect(|data_type| debug!(?path, ?values, ?data_type))
            .inspect_err(|err| error!(?err, ?values))
    }

    pub(super) fn data_type_builder(
        &self,
        path: &[&str],
        data_type: &DataType,
    ) -> Result<Box<dyn ArrayBuilder>> {
        debug!(path = path.join("."), ?data_type);

        match data_type {
            DataType::Null => Ok(Box::new(NullBuilder::new())),
            DataType::Boolean => Ok(Box::new(BooleanBuilder::new())),
            DataType::UInt64 => Ok(Box::new(Int64Builder::new())),
            DataType::Int64 => Ok(Box::new(Int64Builder::new())),
            DataType::Float64 => Ok(Box::new(Float64Builder::new())),
            DataType::Utf8 => Ok(Box::new(StringBuilder::new())),

            DataType::List(element) => {
                debug!(?element);

                Ok(Box::new(
                    ListBuilder::new(self.data_type_builder(
                        &append_path(path, ARROW_LIST_FIELD_NAME)[..],
                        element.data_type(),
                    )?)
                    .with_field(self.new_list_field(path, element.data_type().to_owned())),
                ) as Box<dyn ArrayBuilder>)
            }

            DataType::Struct(fields) => {
                debug!(?fields);

                Ok(Box::new(StructBuilder::new(
                    fields.to_owned(),
                    fields
                        .iter()
                        .map(|field| {
                            self.data_type_builder(
                                &append_path(path, field.name())[..],
                                field.data_type(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?,
                )))
            }

            other => Err(Error::Message(format!(
                "JSON Arrow builder cannot convert inferred type {other:?}"
            ))),
        }
    }
}
