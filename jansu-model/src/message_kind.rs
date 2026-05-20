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

use crate::{Error, Result, as_str};
use proc_macro2::TokenStream;
use quote::ToTokens;
use serde_json::Value;
use std::str::FromStr;
use syn::Expr;

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
