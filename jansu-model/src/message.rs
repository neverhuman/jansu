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

use crate::{
    Error, Field, Listener, MessageKind, Result, Version,
    wv::{As, AsOption, Wv},
};
use convert_case::{Case, Casing};
use serde_json::Value;
use syn::Type;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Message {
    api_key: i16,
    kind: MessageKind,
    listeners: Option<Vec<Listener>>,
    name: String,
    versions: Version,
    fields: Vec<Field>,
    common_structs: Option<Vec<CommonStruct>>,
}

impl Message {
    #[must_use]
    pub fn api_key(&self) -> i16 {
        self.api_key
    }

    #[must_use]
    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc, clippy::unwrap_used)]
    pub fn type_name(&self) -> Type {
        syn::parse_str::<Type>(&self.name).unwrap()
    }

    #[must_use]
    pub fn version(&self) -> Version {
        self.versions
    }

    #[must_use]
    pub fn listeners(&self) -> Option<&[Listener]> {
        self.listeners.as_deref()
    }

    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc, clippy::unwrap_used)]
    pub fn wrapper_new_type(&self, field: &Field) -> Type {
        syn::parse_str::<Type>(&format!("{}{}", self.name, field.name()).to_case(Case::Pascal))
            .unwrap()
    }

    #[must_use]
    pub fn common_structs(&self) -> Option<&[CommonStruct]> {
        self.common_structs.as_deref()
    }

    #[must_use]
    pub fn has_records(&self) -> bool {
        self.fields().iter().any(Field::has_records)
    }

    #[must_use]
    pub fn has_tags(&self) -> bool {
        self.fields().iter().any(Field::has_tags)
    }

    #[must_use]
    pub fn has_float(&self) -> bool {
        self.fields.iter().any(|field| field.kind().is_float())
            || self
                .common_structs()
                .is_some_and(|structures| structures.iter().any(CommonStruct::has_float))
    }
}

impl<'a> TryFrom<&Wv<'a>> for Message {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let api_key = value.as_a("apiKey")?;
        let name = value.as_a("name")?;
        let kind = value.as_a("type")?;
        let listeners = value
            .as_option("listeners")?
            .map_or(Ok(None), |maybe: &[Value]| {
                maybe
                    .iter()
                    .try_fold(Vec::new(), |mut acc, listener| {
                        Listener::try_from(listener).map(|l| {
                            acc.push(l);
                            acc
                        })
                    })
                    .map(Some)
            })?;

        let fields = value.as_a("fields").and_then(|fields: &[Value]| {
            fields.iter().try_fold(Vec::new(), |mut acc, field| {
                Field::try_from(&Wv::from(field)).map(|f| {
                    acc.push(f);
                    acc
                })
            })
        })?;

        let versions = Version::try_from(value)?;
        let common_structs =
            value
                .as_option("commonStructs")?
                .map_or(Ok(None), |maybe: &[Value]| {
                    maybe
                        .iter()
                        .try_fold(Vec::new(), |mut acc, value| {
                            CommonStruct::try_from(&Wv::from(value)).map(|field| {
                                acc.push(field);
                                acc
                            })
                        })
                        .map(Some)
                })?;

        Ok(Self {
            api_key,
            kind,
            listeners,
            name,
            versions,
            fields,
            common_structs,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommonStruct {
    name: String,
    fields: Vec<Field>,
}

impl CommonStruct {
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn type_name(&self) -> Type {
        syn::parse_str::<Type>(&self.name).unwrap_or_else(|_| panic!("not a type: {self:?}"))
    }

    #[must_use]
    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    #[must_use]
    pub fn has_float(&self) -> bool {
        self.fields.iter().any(|field| field.kind().is_float())
    }
}

impl<'a> TryFrom<&Wv<'a>> for CommonStruct {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let name = value.as_a("name")?;
        let fields = value.as_a("fields").and_then(|fields: &[Value]| {
            fields.iter().try_fold(Vec::new(), |mut acc, field| {
                Field::try_from(&Wv::from(field)).map(|f| {
                    acc.push(f);
                    acc
                })
            })
        })?;

        Ok(Self { name, fields })
    }
}
