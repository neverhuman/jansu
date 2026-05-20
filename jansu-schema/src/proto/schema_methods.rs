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

//! Protobuf schema inherent methods

use bytes::{BufMut, Bytes, BytesMut};
use fake::Fake;
use protobuf::{MessageDyn, reflect::MessageDescriptor};
use protobuf_json_mapping::parse_dyn_from_str;
use rhai::{Engine, packages::Package};
use rhai_rand::RandomPackage;
use serde_json::Value;
use tracing::{debug, error};

use crate::{Error, Result};

use super::{MessageGenerator, MessageKind, Schema, message_to_bytes};

impl Schema {
    pub(super) fn message_by_package_relative_name(
        &self,
        message_kind: MessageKind,
    ) -> Option<MessageDescriptor> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_kind.as_ref()))
    }

    pub(super) fn value_to_message(
        &self,
        message_kind: MessageKind,
        json: &Value,
    ) -> Result<Box<dyn MessageDyn>> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_kind.as_ref()))
            .ok_or(Error::Message(format!(
                "message {message_kind:?} not found"
            )))
            .and_then(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
            })
    }

    pub fn encode_from_value(&self, message_kind: MessageKind, json: &Value) -> Result<Bytes> {
        self.value_to_message(message_kind, json)
            .and_then(message_to_bytes)
    }

    pub(super) fn message_generator(&self) -> Option<MessageGenerator> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name("Generator"))
            .map(|generator_descriptor| MessageGenerator {
                generator_descriptor,
            })
    }

    pub(super) fn generate_message_kind(&self, message_kind: MessageKind) -> Result<Option<Bytes>> {
        debug!(?message_kind);

        let engine = {
            let mut engine = Engine::new();

            _ = engine
                .register_fn("first_name", || {
                    fake::faker::name::raw::FirstName(fake::locales::EN).fake::<String>()
                })
                .register_fn("last_name", || {
                    fake::faker::name::raw::LastName(fake::locales::EN).fake::<String>()
                })
                .register_fn("safe_email", || {
                    fake::faker::internet::raw::SafeEmail(fake::locales::EN).fake::<String>()
                })
                .register_fn("building_number", || {
                    fake::faker::address::raw::BuildingNumber(fake::locales::EN).fake::<String>()
                })
                .register_fn("street_name", || {
                    fake::faker::address::raw::StreetName(fake::locales::EN).fake::<String>()
                })
                .register_fn("city_name", || {
                    fake::faker::address::raw::CityName(fake::locales::EN).fake::<String>()
                })
                .register_fn("post_code", || {
                    fake::faker::address::raw::PostCode(fake::locales::EN).fake::<String>()
                })
                .register_fn("country_name", || {
                    fake::faker::address::raw::CountryName(fake::locales::EN).fake::<String>()
                })
                .register_fn("industry", || {
                    fake::faker::company::raw::Industry(fake::locales::EN).fake::<String>()
                });

            let random = RandomPackage::new();
            _ = random.register_into_engine(&mut engine);
            engine
        };

        self.message_by_package_relative_name(message_kind)
            .map_or(Ok(None), |message_descriptor| {
                self.message_generator()
                    .map_or(Ok(None), |message_generator| {
                        message_generator
                            .generate(&engine, &message_descriptor)
                            .and_then(message_to_bytes)
                            .map(Some)
                    })
            })
    }

    pub(super) fn message_value_as_bytes(
        &self,
        message_kind: MessageKind,
        json: &Value,
    ) -> Result<Option<Bytes>> {
        self.message_by_package_relative_name(message_kind)
            .map(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .inspect(|json| debug!(%json))
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
                    .inspect(|message| debug!(%message))
                    .and_then(|message| {
                        let mut w = BytesMut::new().writer();
                        message
                            .write_to_writer_dyn(&mut w)
                            .map(|()| Bytes::from(w.into_inner()))
                            .map_err(Into::into)
                    })
                    .inspect_err(|err| error!(?err))
            })
            .transpose()
    }
}
