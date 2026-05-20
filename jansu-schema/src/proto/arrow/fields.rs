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

//! Protobuf schema Arrow field inference

use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use protobuf::reflect::{MessageDescriptor, RuntimeFieldType, RuntimeType};
use tracing::debug;

use crate::{
    ARROW_LIST_FIELD_NAME,
    proto::{MessageKind, Schema},
};

use super::{GOOGLE_PROTOBUF_TIMESTAMP, NULLABLE, SORTED_MAP_KEYS, append};

impl Schema {
    pub(super) fn new_list_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        data_type: DataType,
    ) -> Field {
        self.new_field(ids, path, ARROW_LIST_FIELD_NAME, data_type)
    }

    pub(super) fn new_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        name: &str,
        data_type: DataType,
    ) -> Field {
        self.new_nullable_field(ids, path, name, data_type, NULLABLE)
    }

    pub(super) fn new_nullable_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Field {
        debug!(?path, name, ?data_type, ?nullable, ?ids);

        let path = append(path, name).join(".");

        Field::new(name.to_owned(), data_type, nullable).with_metadata(
            ids.get(path.as_str())
                .inspect(|field_id| debug!(?path, field_id))
                .map(|field_id| (PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string()))
                .into_iter()
                .collect(),
        )
    }
    pub(super) fn field(
        &self,
        ids: &BTreeMap<String, i32>,
        message_kind: MessageKind,
    ) -> Option<Field> {
        debug!(?message_kind);

        self.message_by_package_relative_name(message_kind)
            .inspect(|descriptor| debug!(?descriptor))
            .map(|descriptor| {
                let name = message_kind.as_ref().to_lowercase();

                self.new_nullable_field(
                    ids,
                    &[],
                    &name,
                    DataType::Struct(Fields::from(self.message_descriptor_to_fields(
                        ids,
                        &[&name],
                        &descriptor,
                    ))),
                    NULLABLE,
                )
            })
    }

    pub(super) fn runtime_type_to_data_type(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        runtime_type: &RuntimeType,
    ) -> DataType {
        debug!(?path, ?runtime_type);

        match runtime_type {
            RuntimeType::U32 | RuntimeType::I32 | RuntimeType::Enum(_) => DataType::Int32,
            RuntimeType::U64 | RuntimeType::I64 => DataType::Int64,
            RuntimeType::F32 => DataType::Float32,
            RuntimeType::F64 => DataType::Float64,
            RuntimeType::Bool => DataType::Boolean,
            RuntimeType::String => DataType::Utf8,
            RuntimeType::VecU8 => DataType::LargeBinary,
            RuntimeType::Message(descriptor) => {
                if descriptor.full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                } else {
                    DataType::Struct(Fields::from(
                        self.message_descriptor_to_fields(ids, path, descriptor),
                    ))
                }
            }
        }
    }

    pub(super) fn message_descriptor_to_fields(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Vec<Field> {
        debug!(?path, ?ids, descriptor_full_name = ?descriptor.full_name());

        descriptor
            .fields()
            .inspect(|field| {
                debug!(
                    name = field.name(),
                    full_name = field.full_name(),
                    type_name = field.proto().type_name()
                )
            })
            .map(|field| match field.runtime_field_type() {
                RuntimeFieldType::Singular(ref singular) => {
                    debug!(
                        descriptor = descriptor.name(),
                        field_name = field.name(),
                        ?singular
                    );

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
                    )
                }

                RuntimeFieldType::Repeated(ref repeated) => {
                    debug!(
                        descriptor = descriptor.name(),
                        field_name = field.name(),
                        ?repeated
                    );

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
                    )
                }

                RuntimeFieldType::Map(ref key, ref value) => {
                    debug!(
                        descriptor = descriptor.name(),
                        field_name = field.name(),
                        ?key,
                        ?value
                    );

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
                                                    append(path, "keys").as_slice(),
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
                                                    append(path, "values").as_slice(),
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
                    )
                }
            })
            .collect::<Vec<_>>()
    }
}
