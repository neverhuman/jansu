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

use crate::{MessageKind, PRIMITIVE, Version, VersionRange};
use tracing::debug;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HeaderMeta {
    pub name: &'static str,
    pub valid: VersionRange,
    pub flexible: VersionRange,
    pub fields: &'static [(&'static str, &'static FieldMeta)],
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Kafka API message metadata.
pub struct MessageMeta {
    /// The name of the Kafka API message.
    pub name: &'static str,
    /// The API key used by this message.
    pub api_key: i16,
    /// The version ranges for this message.
    pub version: Version,
    /// The message kind of this message.
    pub message_kind: MessageKind,
    /// The fields that this message describes.
    pub fields: &'static [(&'static str, &'static FieldMeta)],
}

impl MessageMeta {
    #[must_use]
    pub fn is_flexible(&self, version: i16) -> bool {
        self.version.flexible.within(version)
    }

    #[must_use]
    pub fn structures(&self) -> Vec<(&str, &FieldMeta)> {
        self.fields
            .iter()
            .filter(|(_, fm)| fm.is_structure())
            .flat_map(|(name, fm)| {
                debug!(name = self.name, field = ?name, kind = ?fm.kind.0);

                let mut children = fm.structures();

                if let Some(kind) = fm.kind.kind_of_sequence() {
                    if !kind.is_primitive() {
                        children.push((kind.name(), *fm));
                    }
                } else {
                    children.push((fm.kind.name(), *fm));
                }

                children
            })
            .collect()
    }

    #[must_use]
    pub fn field(&self, name: &str) -> Option<&'static FieldMeta> {
        self.fields
            .iter()
            .find(|(found, _)| name == *found)
            .map(|(_, meta)| *meta)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Kafka API message field metadata
pub struct FieldMeta {
    /// The version range of this field.
    pub version: VersionRange,
    /// The version range where this field may be null.
    pub nullable: Option<VersionRange>,
    /// The kind (type) metadata of this field.
    pub kind: KindMeta,
    /// When present the tag ID of this field.
    pub tag: Option<u32>,
    /// The range of versions where this field is tagged.
    pub tagged: Option<VersionRange>,
    /// The fields contained within this structure.
    pub fields: &'static [(&'static str, &'static FieldMeta)],
    /// Kafka `default` from the descriptor (`None` when the field has no default).
    pub default: Option<&'static str>,
}

impl FieldMeta {
    #[must_use]
    pub fn is_nullable(&self, version: i16) -> bool {
        self.nullable.is_some_and(|range| range.within(version))
    }

    #[must_use]
    pub fn is_mandatory(&self, parent: Option<VersionRange>) -> bool {
        self.version.is_mandatory(parent)
    }

    #[must_use]
    pub fn is_structure(&self) -> bool {
        self.kind
            .kind_of_sequence()
            .is_some_and(|sk| !sk.is_primitive())
            || !self.fields.is_empty()
    }

    #[must_use]
    pub fn structures(&self) -> Vec<(&str, &FieldMeta)> {
        self.fields
            .iter()
            .filter(|(_, fm)| fm.is_structure())
            .flat_map(|(_, fm)| {
                let mut children = fm.structures();

                if let Some(kind) = fm.kind.kind_of_sequence()
                    && !kind.is_primitive()
                {
                    children.push((kind.name(), *fm));
                }

                children
            })
            .collect()
    }

    pub fn field(&self, name: &str) -> Option<&FieldMeta> {
        self.fields
            .iter()
            .find(|field| name == field.0)
            .map(|(_, meta)| *meta)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KindMeta(pub &'static str);

impl KindMeta {
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.0
    }

    #[must_use]
    pub fn is_sequence(&self) -> bool {
        self.0.starts_with("[]")
    }

    #[must_use]
    pub fn is_primitive(&self) -> bool {
        PRIMITIVE.contains(&self.0)
    }

    #[must_use]
    pub fn is_string(&self) -> bool {
        self.0 == "string"
    }

    #[must_use]
    pub fn is_records(&self) -> bool {
        self.0 == "records"
    }

    #[must_use]
    pub fn kind_of_sequence(&self) -> Option<Self> {
        if self.is_sequence() {
            Some(Self(&self.0[2..]))
        } else {
            None
        }
    }
}
