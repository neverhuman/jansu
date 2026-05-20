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

//! JSON schema Arrow builder append helpers

use std::{any::type_name_of_val, sync::Arc};

use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, NullBuilder,
        StringBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, Fields},
};
use serde_json::{Map, Value};
use tracing::{debug, error};

use crate::{Error, Result};

pub(super) fn append_path<'a>(path: &[&'a str], name: &'a str) -> Vec<&'a str> {
    let mut path = Vec::from(path);
    path.push(name);
    path
}

pub(super) fn append_list_builder(
    element: Arc<Field>,
    items: Vec<Value>,
    builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    let values = builder.values().as_any_mut();

    for item in items {
        match (element.data_type(), item) {
            (_, Value::Null) => values
                .downcast_mut::<NullBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_null())?,

            (_, Value::Bool(value)) => values
                .downcast_mut::<BooleanBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_value(value))?,

            (DataType::Int64, Value::Number(value)) if value.is_u64() => values
                .downcast_mut::<Int64Builder>()
                .ok_or(Error::Downcast)
                .map(|builder| {
                    if let Some(value) = value.as_u64() {
                        builder.append_value(value as i64)
                    } else {
                        builder.append_null()
                    }
                })
                .inspect_err(|err| error!(?value, ?err))?,

            (DataType::Int64, Value::Number(value)) if value.is_i64() => values
                .downcast_mut::<Int64Builder>()
                .ok_or(Error::Downcast)
                .map(|builder| {
                    if let Some(value) = value.as_i64() {
                        builder.append_value(value)
                    } else {
                        builder.append_null()
                    }
                })
                .inspect_err(|err| error!(?value, ?err))?,

            (DataType::Float64, Value::Number(value)) if value.is_f64() => values
                .downcast_mut::<Float64Builder>()
                .ok_or(Error::Downcast)
                .map(|builder| {
                    if let Some(value) = value.as_f64() {
                        builder.append_value(value)
                    } else {
                        builder.append_null()
                    }
                })
                .inspect_err(|err| error!(?value, ?err))?,

            (_, Value::String(value)) => values
                .downcast_mut::<StringBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_value(value))?,

            (DataType::List(element), Value::Array(items)) => values
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| error!(?err, ?element, ?items))
                .and_then(|builder| append_list_builder(element.to_owned(), items, builder))?,

            (DataType::Struct(fields), Value::Object(object)) => values
                .downcast_mut::<StructBuilder>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| error!(?err, ?fields, ?object))
                .and_then(|builder| append_struct_builder(fields, object, builder))?,

            (data_type, value) => Err(Error::UnsupportedSchemaRuntimeValue(
                data_type.to_owned(),
                value,
            ))?,
        }
    }

    builder.append(true);

    Ok(())
}

pub(super) fn append_struct_builder(
    fields: &Fields,
    mut object: Map<String, Value>,
    builder: &mut StructBuilder,
) -> Result<()> {
    debug!(?fields, ?object);

    for (index, field) in fields.iter().enumerate() {
        if let Some(value) = object.remove(field.name()) {
            match (field.data_type(), value) {
                (_, Value::Null) => builder
                    .field_builder::<NullBuilder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_null())
                    .inspect_err(|err| error!(?err))?,

                (_, Value::Bool(value)) => builder
                    .field_builder::<BooleanBuilder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_value(value))
                    .inspect_err(|err| error!(?err))?,

                (DataType::Int64, Value::Number(value)) if value.is_u64() => builder
                    .field_builder::<Int64Builder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| {
                        if let Some(value) = value.as_u64() {
                            builder.append_value(value as i64)
                        } else {
                            builder.append_null()
                        }
                    })
                    .inspect_err(|err| error!(?field, ?value, ?err))?,

                (DataType::Int64, Value::Number(value)) if value.is_i64() => builder
                    .field_builder::<Int64Builder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| {
                        if let Some(value) = value.as_i64() {
                            builder.append_value(value)
                        } else {
                            builder.append_null()
                        }
                    })?,

                (DataType::Float64, Value::Number(value)) if value.is_f64() => builder
                    .field_builder::<Float64Builder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| {
                        if let Some(value) = value.as_f64() {
                            builder.append_value(value)
                        } else {
                            builder.append_null()
                        }
                    })?,

                (DataType::Utf8, Value::String(value)) => builder
                    .field_builder::<StringBuilder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_value(value))
                    .inspect_err(|err| error!(?err))?,

                (DataType::List(element), Value::Array(items)) => builder
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
                    .ok_or(Error::Downcast)
                    .and_then(|builder| append_list_builder(element.to_owned(), items, builder))
                    .inspect_err(|err| error!(?err))?,

                (DataType::Struct(fields), Value::Object(object)) => builder
                    .field_builder::<StructBuilder>(index)
                    .ok_or(Error::Downcast)
                    .and_then(|builder| append_struct_builder(fields, object, builder))
                    .inspect_err(|err| error!(?err))?,

                (data_type, value) => Err(Error::UnsupportedSchemaRuntimeValue(
                    data_type.to_owned(),
                    value,
                ))?,
            }
        }
    }

    builder.append(true);

    Ok(())
}

pub(super) fn append(field: &Field, value: Value, builder: &mut dyn ArrayBuilder) -> Result<()> {
    debug!(?field, ?value, builder = type_name_of_val(builder));

    match (field.data_type(), value) {
        (DataType::Null, _) => builder
            .as_any_mut()
            .downcast_mut::<NullBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (DataType::Boolean, Value::Bool(value)) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (DataType::Int64, Value::Number(value)) if value.is_u64() => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| {
                if let Some(value) = value.as_u64() {
                    builder.append_value(value as i64)
                } else {
                    builder.append_null()
                }
            })
            .inspect_err(|err| error!(?field, ?value, ?err)),

        (DataType::Int64, Value::Number(value)) if value.is_i64() => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| {
                if let Some(value) = value.as_i64() {
                    builder.append_value(value)
                } else {
                    builder.append_null()
                }
            }),

        (DataType::Float64, Value::Number(value)) if value.is_f64() => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| {
                if let Some(value) = value.as_f64() {
                    builder.append_value(value)
                } else {
                    builder.append_null()
                }
            }),

        (DataType::Utf8, Value::String(value)) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (DataType::List(element), Value::Array(items)) => builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?element, ?items))
            .and_then(|builder| append_list_builder(element.to_owned(), items, builder)),

        (DataType::Struct(fields), Value::Object(object)) => builder
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?fields, ?object))
            .and_then(|builder| append_struct_builder(fields, object, builder)),

        (data_type, value) => Err(Error::UnsupportedSchemaRuntimeValue(
            data_type.to_owned(),
            value,
        ))?,
    }
}
