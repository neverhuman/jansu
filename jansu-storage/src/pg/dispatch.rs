// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Inherent `Postgres` dispatch methods, split by request family.
//!
//! Each submodule holds the real bodies behind the thin `Storage` trait
//! delegators in `storage_dispatch.rs`.

use super::*;

mod broker;
mod cluster_metadata;
mod configs;
mod describe;
mod fetch;
mod groups;
mod offset_fetch;
mod offsets;
mod producer;
mod topics;
mod txn;
