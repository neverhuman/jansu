use super::{append, ARROW_LIST_FIELD_NAME, GOOGLE_PROTOBUF_TIMESTAMP, NULLABLE, SORTED_MAP_KEYS};
use crate::proto::{MessageKind, Schema};
use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        LargeBinaryBuilder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Field, FieldRef, Fields, TimeUnit},
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use protobuf::reflect::{MessageDescriptor, RuntimeFieldType, RuntimeType};
use std::collections::BTreeMap;
use tracing::{debug, error};

impl Schema {
    fn new_list_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        data_type: DataType,
    ) -> Field {
        self.new_field(ids, path, ARROW_LIST_FIELD_NAME, data_type)
    }

    fn new_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        name: &str,
        data_type: DataType,
    ) -> Field {
        self.new_nullable_field(ids, path, name, data_type, NULLABLE)
    }

    fn new_nullable_field(
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
    pub(super) fn field(&self, ids: &BTreeMap<String, i32>, message_kind: MessageKind) -> Option<Field> {
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

    fn runtime_type_to_data_type(
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

    fn message_descriptor_to_fields(
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

    fn message_descriptor_to_array_builder(
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

    fn runtime_type_to_array_builder(
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

    fn message_descriptor_array_builders(
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
