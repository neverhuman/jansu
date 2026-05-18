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

//! JSON schema to Kafka record conversion

use bytes::Bytes;
use serde_json::Value;
use tracing::debug;

use crate::{
    AsKafkaRecord, Result,
    json::{MessageKind, Schema},
};

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<jansu_sans_io::record::Builder> {
        let mut builder = jansu_sans_io::record::Record::builder();

        if let Some(value) = value.get(MessageKind::Key.as_ref()) {
            debug!(?value);

            if self.key.is_some() {
                builder = builder.key(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?);
            }
        }

        if let Some(value) = value.get(MessageKind::Value.as_ref()) {
            debug!(?value);

            if self.value.is_some() {
                builder =
                    builder.value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?);
            }
        }

        Ok(builder)
    }
}
