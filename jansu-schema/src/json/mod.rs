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

//! JSON schema

use std::collections::BTreeMap;

use crate::{Error, Result};

use bytes::Bytes;

use jansu_sans_io::ErrorCode;
use tracing::{debug, warn};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
mod arrow;

mod generator;
mod kafka_record;
mod validator;

#[derive(Debug, Default)]
pub struct Schema {
    pub(crate) key: Option<jsonschema::Validator>,
    pub(crate) value: Option<jsonschema::Validator>,

    #[allow(dead_code)]
    pub(crate) ids: BTreeMap<String, i32>,
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum MessageKind {
    Key,
    Meta,
    Value,
}

impl AsRef<str> for MessageKind {
    fn as_ref(&self) -> &str {
        match self {
            MessageKind::Key => "key",
            MessageKind::Meta => "meta",
            MessageKind::Value => "value",
        }
    }
}

pub(crate) fn validate(
    validator: Option<&jsonschema::Validator>,
    encoded: Option<&Bytes>,
) -> Result<()> {
    debug!(validator = ?validator, ?encoded);

    validator
        .map_or(Ok(()), |validator| {
            encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
                serde_json::from_reader(&encoded[..])
                    .map_err(|err| {
                        warn!(?err, ?encoded);
                        Error::Api(ErrorCode::InvalidRecord)
                    })
                    .inspect(|instance| debug!(?instance))
                    .and_then(|instance| {
                        validator
                            .validate(&instance)
                            .inspect_err(|err| warn!(?err, ?validator, %instance))
                            .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
                    })
            })
        })
        .inspect(|r| debug!(?r))
        .inspect_err(|err| warn!(?err))
}
