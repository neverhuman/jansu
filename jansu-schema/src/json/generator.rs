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

//! JSON schema generator and `AsJsonValue` implementation

use jansu_sans_io::record::inflated::Batch;
use serde_json::Value;

use crate::{AsJsonValue, Error, Generator, Result, json::Schema};

impl Generator for Schema {
    fn generate(&self) -> Result<jansu_sans_io::record::Builder> {
        Err(Error::NotImplemented {
            kind: "json_generate",
            detail: String::from("Generator::generate for JSON schemas is not yet implemented"),
        })
    }
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<Value> {
        let _ = batch;
        Err(Error::NotImplemented {
            kind: "json_as_json_value",
            detail: String::from(
                "AsJsonValue::as_json_value for JSON schemas is not yet implemented",
            ),
        })
    }
}
