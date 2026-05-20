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

use crate::{Error, Kind, MessageKind, Result, VersionRange};
use serde_json::Value;
use std::str::FromStr;

// A Wrapped JSON type
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Wv<'a>(&'a Value);

impl<'a, 'b: 'a> From<&'b Value> for Wv<'a> {
    fn from(value: &'b Value) -> Self {
        Self(value)
    }
}

pub trait As<'a, T>
where
    T: 'a,
{
    fn as_a(&'a self, name: &str) -> Result<T>;
}

impl<'a> As<'a, &'a str> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<&'a str> {
        self.as_option(name)
            .and_then(|maybe| maybe.ok_or(Error::Message(name.into())))
    }
}

impl<'a> As<'a, String> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<String> {
        self.as_option(name)
            .and_then(|maybe| maybe.ok_or(Error::Message(name.into())))
    }
}

impl<'a> As<'a, &'a [Value]> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<&'a [Value]> {
        self.0[name]
            .as_array()
            .map(|v| &v[..])
            .ok_or(Error::Message(String::from(name)))
    }
}

impl<'a> As<'a, u64> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<u64> {
        self.0[name]
            .as_u64()
            .ok_or(Error::Message(String::from(name)))
    }
}

impl<'a> As<'a, u32> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<u32> {
        As::<'a, u64>::as_a(self, name).and_then(|u| u32::try_from(u).map_err(Into::into))
    }
}

impl<'a> As<'a, u16> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<u16> {
        As::<'a, u64>::as_a(self, name).and_then(|u| u16::try_from(u).map_err(Into::into))
    }
}

impl<'a> As<'a, i64> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<i64> {
        self.0[name]
            .as_i64()
            .ok_or(Error::Message(String::from(name)))
    }
}

impl<'a> As<'a, i16> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<i16> {
        As::<'a, i64>::as_a(self, name).and_then(|u| i16::try_from(u).map_err(Into::into))
    }
}

impl<'a> As<'a, MessageKind> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<MessageKind> {
        self.as_a(name).and_then(MessageKind::from_str)
    }
}

impl<'a> As<'a, VersionRange> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<VersionRange> {
        self.as_a(name).and_then(VersionRange::from_str)
    }
}

impl<'a> As<'a, Kind> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<Kind> {
        self.as_a(name).and_then(Kind::from_str)
    }
}

pub trait AsOption<'a, T>
where
    T: 'a,
{
    fn as_option(&'a self, name: &str) -> Result<Option<T>>;
}

impl<'a> AsOption<'a, bool> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<bool>> {
        Ok(self.0[name].as_bool())
    }
}

impl<'a> AsOption<'a, u64> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u64>> {
        Ok(self.0[name].as_u64())
    }
}

impl<'a> AsOption<'a, u32> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u32>> {
        self.0[name]
            .as_u64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, u16> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u16>> {
        self.0[name]
            .as_u64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, u8> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u8>> {
        self.0[name]
            .as_u64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, i64> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i64>> {
        Ok(self.0[name].as_i64())
    }
}

impl<'a> AsOption<'a, i32> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i32>> {
        self.0[name]
            .as_i64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, i16> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i16>> {
        self.0[name]
            .as_i64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, i8> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i8>> {
        self.0[name]
            .as_i64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, &'a str> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<&'a str>> {
        Ok(self.0[name].as_str())
    }
}

impl<'a> AsOption<'a, String> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<String>> {
        Ok(self.0[name].as_str().map(String::from))
    }
}

impl<'a> AsOption<'a, &'a [Value]> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<&'a [Value]>> {
        Ok(self.0[name].as_array().map(|v| &v[..]))
    }
}

impl<'a> AsOption<'a, VersionRange> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<VersionRange>> {
        self.0[name]
            .as_str()
            .map_or(Ok(None), |s| VersionRange::from_str(s).map(Some))
    }
}

#[cfg(test)]
mod tests;
