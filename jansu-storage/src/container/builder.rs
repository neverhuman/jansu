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

//! Fluent setter methods for the [`StorageContainer`] [`Builder`].

use jansu_schema::{Registry, lake::House};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use url::Url;

use super::Builder;

impl<N, C, A, S> Builder<N, C, A, S> {
    pub fn node_id(self, node_id: i32) -> Builder<i32, C, A, S> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, A, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn advertised_listener(self, advertised_listener: impl Into<Url>) -> Builder<N, C, Url, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn storage(self, storage: Url) -> Builder<N, C, A, Url> {
        debug!(%storage);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Registry>) -> Self {
        _ = schema_registry
            .as_ref()
            .inspect(|schema_registry| debug!(?schema_registry));

        Self {
            schema_registry,
            ..self
        }
    }

    pub fn lake_house(self, lake_house: Option<House>) -> Self {
        _ = lake_house
            .as_ref()
            .inspect(|lake_house| debug!(?lake_house));

        Self { lake_house, ..self }
    }

    pub fn cancellation(self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self
        }
    }

    pub fn silent(self, silent: bool) -> Self {
        Self { silent, ..self }
    }
}
