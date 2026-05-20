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

//! The [`UpdateError`] type used by conditional storage updates.

use std::sync::Arc;

use crate::{Error, Version};

/// Conditional Update Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum UpdateError<T> {
    Error(#[from] Error),

    MissingEtag,

    Outdated { current: Box<T>, version: Version },

    SerdeJson(Arc<serde_json::Error>),

    Uuid(#[from] uuid::Error),
}

#[cfg(feature = "libsql")]
impl<T> From<libsql::Error> for UpdateError<T> {
    fn from(value: libsql::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

#[cfg(feature = "turso")]
impl<T> From<turso::Error> for UpdateError<T> {
    fn from(value: turso::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl<T> From<object_store::Error> for UpdateError<T> {
    fn from(value: object_store::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

impl<T> From<serde_json::Error> for UpdateError<T> {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJson(Arc::new(value))
    }
}

#[cfg(feature = "postgres")]
impl<T> From<tokio_postgres::error::Error> for UpdateError<T> {
    fn from(value: tokio_postgres::error::Error) -> Self {
        Self::Error(Error::from(value))
    }
}
