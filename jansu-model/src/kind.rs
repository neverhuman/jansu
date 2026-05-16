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

use lazy_static::lazy_static;
use proc_macro2::TokenStream;
use quote::ToTokens;
use serde_json::Value;
use std::str::FromStr;
use syn::{Expr, Type};

use crate::Error;
use crate::Result;

macro_rules! with_crate {
    ($e:expr_2021) => {
        format!("{}::{:?}", env!("CARGO_CRATE_NAME"), $e)
    };

    ($e:expr_2021, $f:expr_2021) => {
        format!("{}::{}::{:?}", env!("CARGO_CRATE_NAME"), $e, $f)
    };
}

pub(crate) fn type_mapping(kind: &str) -> String {
    match kind {
        "bytes" => String::from("bytes::Bytes"),
        "float64" => String::from("f64"),
        "int16" => String::from("i16"),
        "int32" => String::from("i32"),
        "int64" => String::from("i64"),
        "int8" => String::from("i8"),
        "string" => String::from("String"),
        "uint16" => String::from("u16"),
        "uuid" => String::from("[u8; 16]"),
        "records" => String::from("crate::RecordBatch"),

        sequence if sequence.starts_with("[]") => type_mapping(&sequence[2..]),

        s => String::from(s),
    }
}

lazy_static! {
    pub(crate) static ref PRIMITIVE: &'static [&'static str] = &[
        "bool", "bytes", "float64", "int16", "int32", "int64", "int8", "records", "string",
        "uint16", "uuid",
    ];
}

pub(crate) fn as_str<'v>(value: &'v Value, name: &str) -> Result<&'v str> {
    value[name]
        .as_str()
        .ok_or(Error::Message(String::from(name)))
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The Kafka field kind (type).
pub struct Kind(pub(crate) String);

impl ToTokens for Kind {
    #![allow(clippy::unwrap_used)]
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let expr = with_crate!(self);
        syn::parse_str::<Expr>(&expr).unwrap().to_tokens(tokens);
    }
}

impl From<Type> for Kind {
    fn from(value: Type) -> Self {
        Self(value.to_token_stream().to_string())
    }
}

impl Kind {
    #[must_use]
    pub fn new(kind: &str) -> Self {
        Self(kind.to_owned())
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.0[..]
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    /// The Rust type name of this kind.
    pub fn type_name(&self) -> Type {
        syn::parse_str::<Type>(&type_mapping(&self.0))
            .expect("type_mapping always returns valid Rust type syntax")
    }

    #[must_use]
    /// Returns true if this kind is a sequence (array)
    pub fn is_sequence(&self) -> bool {
        self.0.starts_with("[]")
    }

    #[must_use]
    /// Returns true is this kind is a Kafka primitive (defined) type
    pub fn is_primitive(&self) -> bool {
        PRIMITIVE.contains(&self.0.as_str())
    }

    #[must_use]
    /// Returns true if this kind is a float64
    pub fn is_float(&self) -> bool {
        self.0.eq("float64")
    }

    #[must_use]
    /// Returns true if this is a sequence of Kafka primitive types
    pub fn is_sequence_of_primitive(&self) -> bool {
        self.is_sequence() && PRIMITIVE.contains(&&self.0[2..])
    }

    #[must_use]
    /// Optionally when the kind is a sequence, returns the kind of the sequence
    pub fn kind_of_sequence(&self) -> Option<Self> {
        if self.is_sequence() {
            Some(Self::new(&self.0[2..]))
        } else {
            None
        }
    }
}

impl FromStr for Kind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_owned()))
    }
}

impl TryFrom<&Value> for Kind {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        as_str(value, "type").and_then(Self::from_str)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The listener for this message type.
pub enum Listener {
    ZkBroker,
    #[default]
    Broker,
    Controller,
}

impl TryFrom<&Value> for Listener {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value
            .as_str()
            .ok_or(Error::Message(String::from(
                "expecting string for listener",
            )))
            .and_then(Self::from_str)
    }
}

impl FromStr for Listener {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "zkBroker" => Ok(Self::ZkBroker),
            "broker" => Ok(Self::Broker),
            "controller" => Ok(Self::Controller),
            s => Err(Error::Message(String::from(s))),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Model request and response types only
pub enum MessageKind {
    #[default]
    Request,
    Response,
}

impl ToTokens for MessageKind {
    #[allow(clippy::unwrap_used)]
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let expr = with_crate!("MessageKind", self);
        syn::parse_str::<Expr>(&expr).unwrap().to_tokens(tokens);
    }
}

impl TryFrom<&Value> for MessageKind {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        as_str(value, "type").and_then(Self::from_str)
    }
}

impl FromStr for MessageKind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "request" => Ok(Self::Request),
            "response" => Ok(Self::Response),
            s => Err(Error::Message(String::from(s))),
        }
    }
}
