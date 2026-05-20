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

//! Protobuf field generator configuration

use std::{collections::BTreeMap, ops::Deref, ops::RangeInclusive};

use bytes::Bytes;
use protobuf::{
    MessageDyn, UnknownValueRef,
    descriptor::FieldDescriptorProto,
    reflect::{EnumDescriptor, MessageDescriptor, ReflectValueRef, RuntimeFieldType},
};
use tracing::debug;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub(super) enum FieldGeneratorConfiguration {
    Bool(bool),
    Bytes(Bytes),
    F32(f32),
    F64(f64),
    I32(i32),
    I64(i64),
    List(Vec<FieldGeneratorConfiguration>),
    Message(BTreeMap<String, FieldGeneratorConfiguration>),
    String(String),
    U32(u32),
    U64(u64),
}

impl Default for FieldGeneratorConfiguration {
    fn default() -> Self {
        Self::Message(Default::default())
    }
}

impl FieldGeneratorConfiguration {
    fn empty_message() -> Self {
        Self::Message(BTreeMap::new())
    }

    pub(super) fn with_field_generator(
        field: &FieldDescriptorProto,
        generator: &MessageDescriptor,
    ) -> FieldGeneratorConfiguration {
        debug!(field = field.name(), generator = generator.full_name(),);

        let configuration = field
            .options
            .special_fields
            .unknown_fields()
            .iter()
            .find_map(|(id, unknown)| {
                if id != 51215 {
                    None
                } else if let UnknownValueRef::LengthDelimited(items) = unknown {
                    let mut message = generator.new_instance();

                    _ = message
                        .merge_from_bytes_dyn(items)
                        .inspect_err(|err| debug!(?err))
                        .ok();

                    Some(Self::from(message.as_ref()))
                } else {
                    None
                }
            });

        match configuration {
            Some(configuration) => configuration,
            None => Self::empty_message(),
        }
    }

    pub(super) fn skip(&self) -> bool {
        match self.get("skip").cloned().and_then(|value| value.as_bool()) {
            Some(skip) => {
                debug!(?skip);
                skip
            }
            None => false,
        }
    }

    pub(super) fn script(&self) -> Option<&str> {
        self.get("script")
            .inspect(|script| debug!(?script))
            .and_then(|value| value.as_str())
            .inspect(|script| debug!(?script))
            .or(self.repeated_script())
    }

    pub(super) fn repeated_range(&self) -> Option<RangeInclusive<u32>> {
        self.get("repeated")
            .inspect(|repeated| debug!(?repeated))
            .and_then(|repeated| {
                repeated
                    .get("range")
                    .inspect(|range| debug!(?range))
                    .and_then(|range| {
                        range
                            .get("min")
                            .inspect(|min| debug!(?min))
                            .and_then(|min| min.as_u32())
                            .and_then(|min| {
                                range
                                    .get("max")
                                    .inspect(|max| debug!(?max))
                                    .and_then(|max| max.as_u32())
                                    .map(|max| min..=max)
                            })
                    })
            })
    }

    pub(super) fn repeated_len(&self) -> Option<u32> {
        self.get("repeated")
            .and_then(|repeated| repeated.get("len"))
            .and_then(|len| len.as_u32())
    }

    pub(super) fn repeated_script(&self) -> Option<&str> {
        self.get("repeated")
            .and_then(|repeated| repeated.get("script"))
            .and_then(|script| script.as_str())
    }

    fn get(&self, key: &str) -> Option<&FieldGeneratorConfiguration> {
        if let Self::Message(message) = self {
            message.get(key)
        } else {
            None
        }
    }

    fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(flag) = self {
            Some(*flag)
        } else {
            None
        }
    }

    fn as_str(&self) -> Option<&str> {
        if let Self::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_u32(&self) -> Option<u32> {
        if let Self::U32(value) = self {
            Some(*value)
        } else {
            None
        }
    }
}

impl From<EnumDescriptor> for FieldGeneratorConfiguration {
    fn from(value: EnumDescriptor) -> Self {
        value
            .values()
            .next()
            .map(|value| Self::String(value.name().to_owned()))
            .unwrap_or_else(|| Self::String(String::new()))
    }
}

impl<'a> From<ReflectValueRef<'a>> for FieldGeneratorConfiguration {
    fn from(value: ReflectValueRef<'a>) -> Self {
        match value {
            ReflectValueRef::U32(value) => Self::U32(value),
            ReflectValueRef::U64(value) => Self::U64(value),
            ReflectValueRef::I32(value) => Self::I32(value),
            ReflectValueRef::I64(value) => Self::I64(value),
            ReflectValueRef::F32(value) => Self::F32(value),
            ReflectValueRef::F64(value) => Self::F64(value),
            ReflectValueRef::Bool(value) => Self::Bool(value),
            ReflectValueRef::String(value) => Self::String(value.to_owned()),
            ReflectValueRef::Bytes(items) => Self::Bytes(Bytes::copy_from_slice(items)),
            ReflectValueRef::Enum(enum_descriptor, value) => enum_descriptor
                .value_by_number(value)
                .map(|value| Self::String(value.name().to_owned()))
                .unwrap_or_else(|| Self::from(enum_descriptor)),
            ReflectValueRef::Message(message_ref) => Self::from(message_ref.deref()),
        }
    }
}

fn reflect_value_key(value: ReflectValueRef<'_>) -> String {
    match value {
        ReflectValueRef::U32(value) => value.to_string(),
        ReflectValueRef::U64(value) => value.to_string(),
        ReflectValueRef::I32(value) => value.to_string(),
        ReflectValueRef::I64(value) => value.to_string(),
        ReflectValueRef::F32(value) => value.to_string(),
        ReflectValueRef::F64(value) => value.to_string(),
        ReflectValueRef::Bool(value) => value.to_string(),
        ReflectValueRef::String(value) => value.to_owned(),
        ReflectValueRef::Bytes(items) => String::from_utf8_lossy(items).into(),
        ReflectValueRef::Enum(enum_descriptor, value) => enum_descriptor
            .value_by_number(value)
            .map(|value| value.name().to_owned())
            .unwrap_or_else(|| value.to_string()),
        ReflectValueRef::Message(message_ref) => message_ref.to_string(),
    }
}

impl From<&dyn MessageDyn> for FieldGeneratorConfiguration {
    fn from(message: &dyn MessageDyn) -> Self {
        debug!(%message);

        Self::Message(
            message
                .descriptor_dyn()
                .fields()
                .inspect(|field| debug!(field = field.name()))
                .filter_map(|field| match field.runtime_field_type() {
                    RuntimeFieldType::Singular(singular) => {
                        debug!(?singular);
                        field
                            .get_singular(message)
                            .inspect(|value| debug!(field = field.name(), ?value))
                            .map(|value| (field.name().to_owned(), Self::from(value)))
                    }

                    RuntimeFieldType::Repeated(repeated) => {
                        debug!(?repeated);
                        Some((
                            field.name().to_owned(),
                            Self::List(
                                field
                                    .get_repeated(message)
                                    .into_iter()
                                    .map(Self::from)
                                    .inspect(|configuration| debug!(?configuration))
                                    .collect::<Vec<_>>(),
                            ),
                        ))
                    }

                    RuntimeFieldType::Map(_, _) => Some((
                        field.name().to_owned(),
                        Self::Message(
                            field
                                .get_map(message)
                                .into_iter()
                                .map(|(key, value)| (reflect_value_key(key), Self::from(value)))
                                .collect::<BTreeMap<String, FieldGeneratorConfiguration>>(),
                        ),
                    )),
                })
                .inspect(|(field, value)| debug!(field, ?value))
                .collect::<BTreeMap<String, FieldGeneratorConfiguration>>(),
        )
    }
}
