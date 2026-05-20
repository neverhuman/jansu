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

//! Protobuf Arrow field identifier assignment

use std::collections::BTreeMap;

use protobuf::reflect::{FileDescriptor, MessageDescriptor, RuntimeFieldType, RuntimeType};
use tracing::debug;

use crate::ARROW_LIST_FIELD_NAME;

use super::{GOOGLE_PROTOBUF_TIMESTAMP, KEY, META, VALUE};

pub(super) fn field_ids(schemas: &[FileDescriptor]) -> BTreeMap<String, i32> {
    fn field_ids_with_path(
        path: &[&str],
        schemas: &[MessageDescriptor],
        id: &mut i32,
    ) -> BTreeMap<String, i32> {
        debug!(?path, ?schemas, ?id);

        let mut ids = BTreeMap::new();

        if path.is_empty() {
            for schema in schemas {
                _ = ids.insert(schema.name().to_lowercase(), *id);
                *id += 1;
            }
        }

        debug!(?ids);

        for schema in schemas {
            let name = schema.name().to_lowercase();

            let path = if path.is_empty() {
                Vec::from([&name[..]])
            } else {
                Vec::from(path)
            };

            for field in schema.fields() {
                debug!(path = ?path.join("."), field_name = ?field.name());
                let name = field.name().to_string();

                let path = {
                    let mut path = path.clone();
                    path.push(&name[..]);
                    path
                };

                _ = ids.insert(path.join("."), *id);
                *id += 1;
            }

            for field in schema.fields() {
                debug!(path = ?path.join("."), field_name = ?field.name());
                let name = field.name().to_string();

                let path = {
                    let mut path = path.clone();
                    path.push(&name[..]);
                    path
                };

                match field.runtime_field_type() {
                    RuntimeFieldType::Singular(singular) => {
                        debug!(?path, ?singular);

                        if let RuntimeType::Message(message_descriptor) = singular {
                            debug!(?path, ?message_descriptor);

                            if message_descriptor.full_name() != GOOGLE_PROTOBUF_TIMESTAMP {
                                ids.extend(field_ids_with_path(
                                    &path[..],
                                    &[message_descriptor],
                                    id,
                                ))
                            }
                        }
                    }

                    RuntimeFieldType::Repeated(repeated) => {
                        debug!(?path, ?repeated);

                        let path = {
                            let mut path = path.clone();
                            path.push(ARROW_LIST_FIELD_NAME);
                            path
                        };

                        _ = ids.insert(path.join("."), *id);
                        *id += 1;

                        if let RuntimeType::Message(message_descriptor) = repeated {
                            debug!(?path, ?message_descriptor);

                            ids.extend(field_ids_with_path(&path[..], &[message_descriptor], id))
                        }
                    }

                    RuntimeFieldType::Map(keys, values) => {
                        debug!(?path, ?keys, ?values);

                        let path = {
                            let mut path = path.clone();
                            path.push("entries");
                            path
                        };

                        _ = ids.insert(path.join("."), *id);
                        *id += 1;

                        {
                            let path = {
                                let mut path = path.clone();
                                path.push("keys");
                                path
                            };

                            _ = ids.insert(path.join("."), *id);
                            *id += 1;

                            if let RuntimeType::Message(message_descriptor) = keys {
                                debug!(?path, ?message_descriptor);

                                ids.extend(field_ids_with_path(
                                    &path[..],
                                    &[message_descriptor],
                                    id,
                                ))
                            }
                        }

                        {
                            let path = {
                                let mut path = path.clone();
                                path.push("values");
                                path
                            };

                            _ = ids.insert(path.join("."), *id);
                            *id += 1;

                            if let RuntimeType::Message(message_descriptor) = values {
                                debug!(?path, ?message_descriptor);

                                ids.extend(field_ids_with_path(
                                    &path[..],
                                    &[message_descriptor],
                                    id,
                                ))
                            }
                        }
                    }
                }
            }
        }

        debug!(?ids);
        ids
    }

    let descriptors = schemas
        .iter()
        .find_map(|fd| fd.message_by_package_relative_name(META))
        .into_iter()
        .chain(
            schemas
                .iter()
                .find_map(|fd| fd.message_by_package_relative_name(KEY))
                .into_iter()
                .chain(
                    schemas
                        .iter()
                        .find_map(|fd| fd.message_by_package_relative_name(VALUE)),
                ),
        )
        .collect::<Vec<_>>();

    field_ids_with_path(&[], &descriptors[..], &mut 1)
}
