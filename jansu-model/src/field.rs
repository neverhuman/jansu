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

use convert_case::{Case, Casing};
use lazy_static::lazy_static;
use proc_macro2::{Ident, Span};
use serde_json::Value;

use crate::Error;
use crate::Result;
use crate::kind::Kind;
use crate::version::VersionRange;
use crate::wv::{As, AsOption, Wv};

fn is_reserved_keyword(s: &str) -> bool {
    lazy_static! {
        static ref RESERVED: Vec<&'static str> = vec![
            "as",
            "break",
            "const",
            "continue",
            "crate",
            "else",
            "enum",
            "extern",
            "false",
            "fn",
            "for",
            "if",
            "impl",
            "in",
            "let",
            "loop",
            "match",
            "mod",
            "move",
            "mut",
            "pub",
            "ref",
            "return",
            "self",
            "Self",
            "static",
            "struct",
            "super",
            "trait",
            "true",
            "type",
            "unsafe",
            "use",
            "where",
            "while",
            // 2018 edition
            "async",
            "await",
            "dyn",
            // reserved
            "abstract",
            "become",
            "box",
            "do",
            "final",
            "macro",
            "override",
            "priv",
            "typeof",
            "unsized",
            "virtual",
            "yield",
            // reserved 2018 edition
            "try",
        ];
    }

    RESERVED.contains(&s)
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Kafka message field schema
pub struct Field {
    /// The name of this field.
    pub(crate) name: String,
    /// The Kafka type of this field.
    pub(crate) kind: Kind,
    /// A comment about this field.
    pub(crate) about: Option<String>,
    /// The version range for this field.
    pub(crate) versions: VersionRange,
    /// Whether this field can be used as the key in a map.
    pub(crate) map_key: Option<bool>,
    /// Whether this field can be null.
    pub(crate) nullable: Option<VersionRange>,
    /// The tag ID for this field
    pub(crate) tag: Option<u32>,
    /// The version range in which this field can be tagged.
    pub(crate) tagged: Option<VersionRange>,
    /// The entity type of this field.
    pub(crate) entity_type: Option<String>,
    /// The default value of this field (Kafka JSON `default` string).
    pub(crate) default: Option<String>,
    /// Any fields this field contains.
    pub(crate) fields: Option<Vec<Field>>,
}

impl Field {
    #[must_use]
    pub fn ident(&self) -> Ident {
        match self.name.to_case(Case::Snake) {
            reserved if is_reserved_keyword(&reserved) => {
                Ident::new_raw(&reserved, Span::call_site())
            }

            otherwise => Ident::new(&otherwise, Span::call_site()),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn kind(&self) -> &Kind {
        &self.kind
    }

    #[must_use]
    pub fn fields(&self) -> Option<&[Field]> {
        self.fields.as_deref()
    }

    #[must_use]
    pub fn versions(&self) -> VersionRange {
        self.versions
    }

    #[must_use]
    pub fn nullable(&self) -> Option<VersionRange> {
        self.nullable
    }

    #[must_use]
    pub fn tagged(&self) -> Option<VersionRange> {
        self.tagged
    }

    #[must_use]
    pub fn tag(&self) -> Option<u32> {
        self.tag
    }

    #[must_use]
    pub fn about(&self) -> Option<&str> {
        self.about.as_deref()
    }

    #[must_use]
    pub fn has_tags(&self) -> bool {
        self.tagged.is_some()
    }

    #[must_use]
    pub fn has_records(&self) -> bool {
        self.kind.0 == "records"
            || self
                .fields()
                .is_some_and(|fields| fields.iter().any(Field::has_records))
    }

    #[must_use]
    pub fn has_float(&self) -> bool {
        self.kind().is_float()
            || self
                .fields()
                .is_some_and(|fields| fields.iter().any(Field::has_float))
    }

    /// Kafka `default` string from the message descriptor, if present.
    #[must_use]
    pub fn kafka_default(&self) -> Option<&str> {
        self.default.as_deref()
    }
}

impl<'a> TryFrom<&Wv<'a>> for Field {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let name = value.as_a("name")?;
        let kind = value.as_a("type")?;
        let about = value.as_option("about")?;
        let versions = value.as_a("versions")?;
        let map_key = value.as_option("mapKey")?;
        let nullable = value.as_option("nullableVersions")?;
        let tag = value.as_option("tag")?;
        let tagged = value.as_option("taggedVersions")?;
        let entity_type = value.as_option("entityType")?;
        let default = value.as_option("default")?;

        let fields = value
            .as_option("fields")?
            .map_or(Ok(None), |values: &[Value]| {
                values
                    .iter()
                    .try_fold(Vec::new(), |mut acc, field| {
                        Field::try_from(&Wv::from(field)).map(|f| {
                            acc.push(f);
                            acc
                        })
                    })
                    .map(Some)
            })?;

        Ok(Field {
            name,
            kind,
            about,
            versions,
            map_key,
            nullable,
            tag,
            tagged,
            entity_type,
            default,
            fields,
        })
    }
}
