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

use crate::{Error, Result};
use serde_json::Value;
use std::str::FromStr;

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
