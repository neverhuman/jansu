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

//! The `null://` storage [`Engine`]: a minimal in-memory placeholder backend.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use jansu_sans_io::create_topics_request::CreatableTopic;
use url::Url;

use crate::{GroupDetail, Version};

mod groups;
mod offsets;
mod storage_impl;
mod topics;
mod txn;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Group {
    detail: GroupDetail,
    version: Option<Version>,
}

#[derive(Clone, Debug)]
pub struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,

    topics: Arc<Mutex<Vec<CreatableTopic>>>,
    groups: Arc<Mutex<BTreeMap<String, Group>>>,
}

impl Engine {
    pub fn new(cluster: String, node: i32, advertised_listener: Url) -> Self {
        Self {
            cluster,
            node,
            advertised_listener,
            topics: Arc::new(Mutex::new(Vec::new())),
            groups: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

const FEATURE: &str = "storage";
const MESSAGE: &str = "storage has not been defined";
