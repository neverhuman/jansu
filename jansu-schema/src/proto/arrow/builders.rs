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

//! Protobuf schema Arrow builder inference

use std::collections::BTreeMap;

use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        LargeBinaryBuilder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, FieldRef, Fields},
};
use protobuf::reflect::{MessageDescriptor, RuntimeFieldType, RuntimeType};
use tracing::debug;

use crate::{
    ARROW_LIST_FIELD_NAME,
    proto::{MessageKind, Schema},
};

use super::{GOOGLE_PROTOBUF_TIMESTAMP, NULLABLE, SORTED_MAP_KEYS, append};

impl Schema {
    pub(super) fn message_by_package_relative_name_array_builder(
        &self,
        ids: &BTreeMap<String, i32>,
        message_kind: MessageKind,
    ) -> Option<Box<dyn ArrayBuilder>> {
        debug!(?message_kind);
        self.message_by_package_relative_name(message_kind)
            .map(|descriptor| {
                self.message_descriptor_to_array_builder(
                    ids,
                    &[&message_kind.as_ref().to_lowercase()],
                    &descriptor,
                )
            })
    }

    pub(super) fn message_descriptor_to_array_builder(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Box<dyn ArrayBuilder> {
        debug!(?path, descriptor = descriptor.name());
        let fields = self.message_descriptor_to_fields(ids, path, descriptor);
        let builders = self.message_descriptor_array_builders(ids, path, descriptor);

        Box::new(StructBuilder::new(fields, builders)) as Box<dyn ArrayBuilder>
    }

    pub(super) fn runtime_type_to_array_builder(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        runtime_type: &RuntimeType,
    ) -> Box<dyn ArrayBuilder> {
        debug!(?path, ?runtime_type, ?ids);

        match runtime_type {
            RuntimeType::U32 | RuntimeType::I32 | RuntimeType::Enum(_) => {
                Box::new(Int32Builder::new())
            }
            RuntimeType::U64 | RuntimeType::I64 => Box::new(Int64Builder::new()),
            RuntimeType::F32 => Box::new(Float32Builder::new()),
            RuntimeType::F64 => Box::new(Float64Builder::new()),
            RuntimeType::Bool => Box::new(BooleanBuilder::new()),
            RuntimeType::String => Box::new(StringBuilder::new()),
            RuntimeType::VecU8 => Box::new(LargeBinaryBuilder::new()),

            RuntimeType::Message(descriptor) => {
                if descriptor.full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                    Box::new(TimestampMicrosecondBuilder::new())
                } else {
                    let (fields, builders) = descriptor
                        .fields()
                        .map(|field| match field.runtime_field_type() {
                            RuntimeFieldType::Singular(ref singular) => {
                                debug!(
                                    descriptor = descriptor.name(),
                                    field_name = field.name(),
                                    ?singular
                                );

                                (
                                    self.new_nullable_field(
                                        ids,
                                        path,
                                        field.name(),
                                        self.runtime_type_to_data_type(
                                            ids,
                                            &append(path, field.name())[..],
                                            singular,
                                        ),
                                        !field.is_required(),
                                    ),
                                    self.runtime_type_to_array_builder(ids, path, singular),
                                )
                            }

                            RuntimeFieldType::Repeated(ref repeated) => {
                                debug!(
                                    descriptor = descriptor.name(),
                                    field_name = field.name(),
                                    ?repeated
                                );

                                (
                                    self.new_nullable_field(
                                        ids,
                                        path,
                                        field.name(),
                                        {
                                            let path = &append(path, field.name())[..];

                                            DataType::List(FieldRef::new(self.new_list_field(
                                                ids,
                                                path,
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                    repeated,
                                                ),
                                            )))
                                        },
                                        !field.is_required(),
                                    ),
                                    {
                                        let path = &append(path, field.name())[..];

                                        Box::new(
                                            ListBuilder::new(self.runtime_type_to_array_builder(
                                                ids,
                                                &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                repeated,
                                            ))
                                            .with_field(self.new_list_field(
                                                ids,
                                                path,
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                    repeated,
                                                ),
                                            )),
                                        )
                                            as Box<dyn ArrayBuilder>
                                    },
                                )
                            }

                            RuntimeFieldType::Map(ref key, ref value) => {
                                debug!(
                                    descriptor = descriptor.name(),
                                    field_name = field.name(),
                                    ?key,
                                    ?value
                                );

                                (
                                    self.new_nullable_field(
                                        ids,
                                        path,
                                        field.name(),
                                        {
                                            let path = &append(path, field.name())[..];

                                            DataType::Map(
                                                FieldRef::new(self.new_nullable_field(
                                                    ids,
                                                    path,
                                                    "entries",
                                                    DataType::Struct({
                                                        let path = &append(path, "entries")[..];

                                                        Fields::from_iter([
                                                            self.new_nullable_field(
                                                                ids,
                                                                path,
                                                                "keys",
                                                                self.runtime_type_to_data_type(
                                                                    ids,
                                                                    &append(path, "keys")[..],
                                                                    key,
                                                                ),
                                                                !NULLABLE,
                                                            ),
                                                            self.new_field(
                                                                ids,
                                                                path,
                                                                "values",
                                                                self.runtime_type_to_data_type(
                                                                    ids,
                                                                    &append(path, "values")[..],
                                                                    value,
                                                                ),
                                                            ),
                                                        ])
                                                    }),
                                                    !NULLABLE,
                                                )),
                                                SORTED_MAP_KEYS,
                                            )
                                        },
                                        !field.is_required(),
                                    ),
                                    Box::new(MapBuilder::new(
                                        None,
                                        self.runtime_type_to_array_builder(ids, path, key),
                                        self.runtime_type_to_array_builder(ids, path, value),
                                    )) as Box<dyn ArrayBuilder>,
                                )
                            }
                        })
                        .collect::<(Vec<_>, Vec<_>)>();

                    Box::new(StructBuilder::new(fields, builders))
                }
            }
        }
    }

    pub(super) fn message_descriptor_array_builders(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Vec<Box<dyn ArrayBuilder>> {
        debug!(?path, descriptor = descriptor.full_name(), ?ids);

        descriptor
            .fields()
            .map(|field| {
                let inside = &append(path, field.name())[..];

                match field.runtime_field_type() {
                    RuntimeFieldType::Singular(ref singular) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?singular,
                            ?inside
                        );
                        self.runtime_type_to_array_builder(ids, inside, singular)
                    }

                    RuntimeFieldType::Repeated(ref repeated) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?repeated,
                            ?inside
                        );
                        Box::new(
                            ListBuilder::new(self.runtime_type_to_array_builder(
                                ids,
                                &append(inside, ARROW_LIST_FIELD_NAME)[..],
                                repeated,
                            ))
                            .with_field(self.new_list_field(
                                ids,
                                inside,
                                self.runtime_type_to_data_type(
                                    ids,
                                    &append(inside, ARROW_LIST_FIELD_NAME)[..],
                                    repeated,
                                ),
                            )),
                        )
                    }

                    RuntimeFieldType::Map(ref key, ref value) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?key,
                            ?value,
                            ?inside
                        );

                        let path = &append(inside, "entries");

                        Box::new(
                            MapBuilder::new(
                                None,
                                self.runtime_type_to_array_builder(ids, path, key),
                                self.runtime_type_to_array_builder(ids, path, value),
                            )
                            .with_keys_field(self.new_nullable_field(
                                ids,
                                &path[..],
                                "keys",
                                self.runtime_type_to_data_type(ids, &append(path, "keys")[..], key),
                                !NULLABLE,
                            ))
                            .with_values_field(self.new_field(
                                ids,
                                &path[..],
                                "values",
                                self.runtime_type_to_data_type(
                                    ids,
                                    &append(path, "values")[..],
                                    value,
                                ),
                            )),
                        )
                    }
                }
            })
            .collect::<Vec<_>>()
    }
}
