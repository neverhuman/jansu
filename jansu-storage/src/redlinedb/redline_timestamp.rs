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

use super::*;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RedlineTimestamp(pub(crate) SystemTime);

impl Deref for RedlineTimestamp {
    type Target = SystemTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<SystemTime> for RedlineTimestamp {
    fn from(value: SystemTime) -> Self {
        Self(value)
    }
}

impl From<&SystemTime> for RedlineTimestamp {
    fn from(value: &SystemTime) -> Self {
        Self(*value)
    }
}

impl From<RedlineTimestamp> for SystemTime {
    fn from(value: RedlineTimestamp) -> Self {
        value.0
    }
}

impl From<RedlineTimestamp> for Value {
    fn from(value: RedlineTimestamp) -> Self {
        match to_timestamp(&value.0) {
            Ok(timestamp) => Value::Integer(timestamp),
            Err(_) => {
                tracing::warn!(timestamp = ?value.0, "timestamp conversion failed; using epoch");
                Value::Integer(0)
            }
        }
    }
}

impl redline::IntoRedlineValue for RedlineTimestamp {
    fn into_redline_value(self) -> Value {
        Value::from(self)
    }
}

impl TryFrom<Value> for RedlineTimestamp {
    type Error = Error;

    fn try_from(value: Value) -> result::Result<Self, Self::Error> {
        match value {
            Value::Integer(timestamp) => to_system_time(timestamp)
                .map_err(Into::into)
                .map(RedlineTimestamp::from),

            Value::Text(text) => match text.parse::<i64>() {
                Ok(timestamp) => to_system_time(timestamp)
                    .map_err(Into::into)
                    .map(RedlineTimestamp::from),
                Err(_) => {
                    let date_time = match chrono::DateTime::parse_from_rfc3339(&text) {
                        Ok(date_time) => Ok(date_time.to_utc()),
                        Err(rfc_err) => {
                            match NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f") {
                                Ok(date_time) => Ok(date_time.and_utc()),
                                Err(naive_err) => {
                                    debug!(?rfc_err, ?naive_err, %text, "failed to parse timestamp text");
                                    Err(naive_err)
                                }
                            }
                        }
                    };

                    date_time
                        .inspect(|dt| debug!(?dt))
                        .map(SystemTime::from)
                        .map(RedlineTimestamp::from)
                        .map_err(Into::into)
                }
            },

            Value::Real(_) | Value::Null | Value::Blob(_) => Err(Error::Message(format!(
                "unexpected timestamp value type: {value:?}"
            ))),
        }
    }
}
