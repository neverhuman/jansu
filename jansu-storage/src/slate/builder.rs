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

//! Builder for the SlateDB [`Engine`].

use std::{fmt, sync::Arc};

use jansu_schema::{Registry, lake::House};
use slatedb::Db;
use url::Url;

use super::engine::Engine;

/// Builder for SlateDB Engine
///
/// Provides a fluent API for constructing an Engine with required and optional parameters.
#[derive(Clone, Default)]
pub struct Builder {
    cluster: Option<String>,
    node: Option<i32>,
    advertised_listener: Option<Url>,
    db: Option<Arc<Db>>,
    schemas: Option<Registry>,
    lake: Option<House>,
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("cluster", &self.cluster)
            .field("node", &self.node)
            .field("advertised_listener", &self.advertised_listener)
            .field("db", &self.db.as_ref().map(|_| "Arc<Db>"))
            .field("schemas", &self.schemas.as_ref().map(|_| "Registry"))
            .field("lake", &self.lake.as_ref().map(|_| "House"))
            .finish()
    }
}

impl Builder {
    pub fn cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = Some(cluster.into());
        self
    }

    pub fn node(mut self, node: i32) -> Self {
        self.node = Some(node);
        self
    }

    pub fn advertised_listener(mut self, advertised_listener: Url) -> Self {
        self.advertised_listener = Some(advertised_listener);
        self
    }

    pub fn db(mut self, db: Arc<Db>) -> Self {
        self.db = Some(db);
        self
    }

    pub fn schemas(mut self, schemas: Option<Registry>) -> Self {
        self.schemas = schemas;
        self
    }

    pub fn lake(mut self, lake: Option<House>) -> Self {
        self.lake = lake;
        self
    }

    /// Build the Engine, panicking if required fields are missing
    pub fn build(self) -> Engine {
        Engine {
            cluster: self.cluster.expect("cluster is required"),
            node: self.node.expect("node is required"),
            advertised_listener: self
                .advertised_listener
                .expect("advertised_listener is required"),
            db: self.db.expect("db is required"),
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    /// Try to build the Engine, returning None if required fields are missing
    pub fn try_build(self) -> Option<Engine> {
        Some(Engine {
            cluster: self.cluster?,
            node: self.node?,
            advertised_listener: self.advertised_listener?,
            db: self.db?,
            schemas: self.schemas,
            lake: self.lake,
        })
    }
}
