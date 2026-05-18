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

pub mod agent;
pub mod error;
pub mod field;
pub mod kind;
pub mod message;
pub mod meta;
pub mod version;
pub mod wv;

pub use agent::AgentException;
pub use error::Error;
pub use field::Field;
pub use kind::{Kind, Listener, MessageKind};
pub use message::{CommonStruct, Message};
pub use meta::{FieldMeta, HeaderMeta, KindMeta, MessageMeta};
pub use version::{Version, VersionRange};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests;
