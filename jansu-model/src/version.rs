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
    Error, Result,
    wv::{As, AsOption, Wv},
};
use lazy_static::lazy_static;
use proc_macro2::TokenStream;
use quote::ToTokens;
use regex::Regex;
use std::fmt;
use std::str::FromStr;
use syn::Expr;
use tracing::debug;

#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// A range of versions.
pub struct VersionRange {
    pub start: i16,
    pub end: i16,
}

impl fmt::Debug for VersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(&prefix_crate!(VersionRange))
            .field("start", &self.start)
            .field("end", &self.end)
            .finish()
    }
}

impl VersionRange {
    #[must_use]
    pub fn within(&self, version: i16) -> bool {
        version >= self.start && version <= self.end
    }

    #[must_use]
    pub fn is_mandatory(&self, parent: Option<VersionRange>) -> bool {
        parent.map_or(
            self.start == 0 && self.end == i16::MAX,
            |VersionRange { start, end }| {
                if start > self.start {
                    self.end == end
                } else {
                    self.start == start && self.end == end
                }
            },
        )
    }
}

impl ToTokens for VersionRange {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let expr = format!("{self:?}");
        syn::parse_str::<Expr>(&expr)
            .unwrap_or_else(|_| panic!("an expression: {self:?}"))
            .to_tokens(tokens);
    }
}

impl FromStr for VersionRange {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #![allow(clippy::expect_used)]
        lazy_static! {
            static ref RX: Regex =
                Regex::new(r"^(?<start>\d+)(-(?<end>\d+)|(?<infinite>\+))?$").expect("regex");
        }

        if s == "none" {
            Ok(VersionRange { start: -2, end: -1 })
        } else {
            RX.captures(s)
                .ok_or(Error::Message(format!("invalid version range format: {s}")))
                .and_then(|captures| {
                    let parse = |name, default: i16| {
                        captures
                            .name(name)
                            .map_or(Ok(&default.to_string()[..]), |s| Ok(s.as_str()))
                            .and_then(str::parse)
                    };

                    let start = parse("start", 0)?;

                    let end = if let Some(end) = captures.name("end") {
                        str::parse(end.as_str())?
                    } else if captures.name("infinite").is_some() {
                        i16::MAX
                    } else {
                        start
                    };

                    Ok(VersionRange { start, end })
                })
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The validity, retired, and flexible version ranges of a Kafka API message.
pub struct Version {
    /// The valid version ranges of this Kafka message.
    pub valid: VersionRange,
    /// The retired version range of this Kafka message.
    pub retired: Option<VersionRange>,
    /// The range of versions where this message uses flexible encoding.
    pub flexible: VersionRange,
}

impl ToTokens for Version {
    #[allow(clippy::unwrap_used)]
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let expr = with_crate!(self);
        syn::parse_str::<Expr>(&expr).unwrap().to_tokens(tokens);
    }
}

impl Version {
    #[must_use]
    pub fn valid(&self) -> VersionRange {
        self.valid
    }

    #[must_use]
    pub fn retired(&self) -> Option<VersionRange> {
        self.retired
    }

    #[must_use]
    pub fn flexible(&self) -> VersionRange {
        self.flexible
    }
}

impl<'a> TryFrom<&Wv<'a>> for Version {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        debug!("value: {:?}", value);

        Ok(Self {
            valid: value.as_a("validVersions")?,
            retired: value.as_option(concat!("depre", "catedVersions"))?,
            flexible: value.as_a("flexibleVersions")?,
        })
    }
}
