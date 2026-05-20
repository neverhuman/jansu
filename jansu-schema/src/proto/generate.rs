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

//! Protobuf message and field value generators

use protobuf::{
    MessageDyn,
    reflect::{MessageDescriptor, ReflectValueBox, RuntimeFieldType, RuntimeType},
};
use rand::prelude::*;
use rhai::Engine;
use tracing::debug;

use crate::{Error, Result};

use super::FieldGeneratorConfiguration;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct MessageGenerator {
    pub(super) generator_descriptor: MessageDescriptor,
}

impl MessageGenerator {
    pub(super) fn generate(
        &self,
        engine: &Engine,
        message_descriptor: &MessageDescriptor,
    ) -> Result<Box<dyn MessageDyn>> {
        debug!(message_descriptor = message_descriptor.full_name());

        let mut message_dyn = message_descriptor.new_instance();

        for (field_proto, field) in message_descriptor
            .proto()
            .field
            .iter()
            .zip(message_dyn.descriptor_dyn().fields())
        {
            let field_generator = FieldGenerator {
                generator_descriptor: &self.generator_descriptor,
                configuration: FieldGeneratorConfiguration::with_field_generator(
                    field_proto,
                    &self.generator_descriptor,
                ),
            };

            if field_generator.configuration.skip() {
                continue;
            }

            match field.runtime_field_type() {
                RuntimeFieldType::Singular(ref singular) => field_generator
                    .singular_value(engine, singular)
                    .map(|value| field.set_singular_field(message_dyn.as_mut(), value))?,

                RuntimeFieldType::Repeated(ref repeated) => {
                    let mut r = field.mut_repeated(message_dyn.as_mut());
                    for element in field_generator.repeated_value(engine, repeated)? {
                        r.push(element);
                    }
                }

                RuntimeFieldType::Map(ref key, ref value) => {
                    let mut map = field.mut_map(message_dyn.as_mut());
                    map.insert(
                        default_map_key(key)?,
                        field_generator.singular_value(engine, value)?,
                    );
                }
            }
        }

        Ok(message_dyn)
    }
}

struct FieldGenerator<'a> {
    generator_descriptor: &'a MessageDescriptor,
    configuration: FieldGeneratorConfiguration,
}

impl<'a> FieldGenerator<'a> {
    fn singular_value(
        &self,
        engine: &Engine,
        runtime_type: &RuntimeType,
    ) -> Result<ReflectValueBox> {
        let mut rng = rand::rng();
        match runtime_type {
            RuntimeType::I32 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<i32>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::I64 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<i64>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::U32 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<u32>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::U64 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<u64>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::F32 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<f32>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::F64 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<f64>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::Bool => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<bool>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::String => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(String::from("abc")), |script| {
                    engine
                        .eval::<String>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::VecU8 => {
                let bytes = match self.configuration.script().inspect(|script| debug!(script)) {
                    Some(script) => engine
                        .eval::<String>(script)
                        .map(|value| value.into_bytes())
                        .inspect_err(|err| debug!(script, ?err))
                        .map_err(Error::from),
                    None => Ok(Vec::new()),
                }
                .or_else(|_: Error| {
                    if let FieldGeneratorConfiguration::Bytes(bytes) = &self.configuration {
                        Ok::<Vec<u8>, Error>(bytes.to_vec())
                    } else {
                        Ok::<Vec<u8>, Error>(Vec::new())
                    }
                })?;

                Ok(ReflectValueBox::from(bytes))
            }

            RuntimeType::Enum(descriptor) => {
                let result = match self.configuration.script().inspect(|script| debug!(script)) {
                    Some(script) => {
                        let name = engine
                            .eval::<String>(script)
                            .inspect(|name| debug!(name))
                            .inspect_err(|err| debug!(script, ?err))?;

                        match descriptor
                            .value_by_name(&name[..])
                            .inspect(|value_descriptor| debug!(?value_descriptor))
                        {
                            Some(value_descriptor) => Ok(ReflectValueBox::Enum(
                                descriptor.clone(),
                                value_descriptor.value(),
                            )),
                            None => Err(Error::Message(format!(
                                "enum {} has no value named {}",
                                descriptor.full_name(),
                                name
                            ))),
                        }
                    }
                    None => descriptor
                        .values()
                        .next()
                        .map(|value| ReflectValueBox::Enum(descriptor.clone(), value.value()))
                        .ok_or_else(|| {
                            Error::Message(format!(
                                "enum {} has no declared values",
                                descriptor.full_name()
                            ))
                        }),
                }?;

                debug!(?result);
                Ok(result)
            }

            RuntimeType::Message(message_descriptor) => {
                let generator = MessageGenerator {
                    generator_descriptor: self.generator_descriptor.to_owned(),
                };

                generator
                    .generate(engine, message_descriptor)
                    .map(ReflectValueBox::Message)
            }
        }
    }

    fn repeated_value(
        &self,
        engine: &Engine,
        runtime_type: &RuntimeType,
    ) -> Result<Vec<ReflectValueBox>> {
        let upper = self.configuration.repeated_len().unwrap_or_else(|| {
            rand::rng().random_range(self.configuration.repeated_range().unwrap_or(0..=1))
        });

        (0..upper)
            .inspect(|i| debug!(i))
            .map(|_| self.singular_value(engine, runtime_type))
            .collect::<Result<Vec<_>>>()
    }
}

fn default_map_key(runtime_type: &RuntimeType) -> Result<ReflectValueBox> {
    match runtime_type {
        RuntimeType::I32 => Ok(ReflectValueBox::from(0_i32)),
        RuntimeType::I64 => Ok(ReflectValueBox::from(0_i64)),
        RuntimeType::U32 => Ok(ReflectValueBox::from(0_u32)),
        RuntimeType::U64 => Ok(ReflectValueBox::from(0_u64)),
        RuntimeType::Bool => Ok(ReflectValueBox::from(false)),
        RuntimeType::String => Ok(ReflectValueBox::from(String::new())),
        other => Err(Error::Message(format!(
            "unsupported protobuf map key type: {other:?}"
        ))),
    }
}
