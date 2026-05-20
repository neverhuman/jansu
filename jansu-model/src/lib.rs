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
//
//! Structures representing Kafka JSON protocol definitions.
//!
//! This crate converts Kafka JSON protocol definitions into structures
//! that can be easily used during the Jansu Sans I/O build process.

pub mod error;
pub mod wv;

pub use error::Error;

use lazy_static::lazy_static;
use serde_json::Value;

pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! prefix_crate {
    ($e:ident) => {
        format!("{}::{}", env!("CARGO_CRATE_NAME"), stringify!($e))
    };
}

macro_rules! with_crate {
    ($e:expr_2021) => {
        format!("{}::{:?}", env!("CARGO_CRATE_NAME"), $e)
    };

    ($e:expr_2021, $f:expr_2021) => {
        format!("{}::{}::{:?}", env!("CARGO_CRATE_NAME"), $e, $f)
    };
}

mod field;
mod kind;
mod listener;
mod message;
mod message_kind;
mod meta;
mod version;

pub use field::Field;
pub use kind::Kind;
pub use listener::Listener;
pub use message::{CommonStruct, Message};
pub use message_kind::MessageKind;
pub use meta::{FieldMeta, HeaderMeta, KindMeta, MessageMeta};
pub use version::{Version, VersionRange};

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
