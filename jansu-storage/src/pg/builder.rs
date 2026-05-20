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

//! PostgreSQL storage `Builder` and `Postgres::builder` constructor.

use super::*;

/// PostgreSQL Storage Builder
#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, P> {
    cluster: C,
    node: N,
    advertised_listener: L,
    pool: P,
    schemas: Option<Registry>,
    lake: Option<House>,
}

impl<C, N, L, P> Builder<C, N, L, P> {
    pub fn cluster(self, cluster: impl Into<String>) -> Builder<String, N, L, P> {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn node(self, node: i32) -> Builder<C, i32, L, P> {
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, P> {
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, P> {
        Self { schemas, ..self }
    }

    pub fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }
}

impl Builder<String, i32, Url, Pool> {
    pub fn build(self) -> Postgres {
        Postgres {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }
}

impl<C, N> FromStr for Builder<C, N, Url, Pool>
where
    C: Default,
    N: Default,
{
    type Err = Error;

    fn from_str(config: &str) -> Result<Self, Self::Err> {
        let pg_config = Config::from_str(config).inspect(|pg_config| debug!(?pg_config))?;

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let root_store = {
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_native_certs::load_native_certs().certs {
                roots.add(cert).inspect_err(|err| debug!(?err))?;
            }
            roots
        };

        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map(|config| config.with_root_certificates(root_store))
        .map(|config| config.with_no_client_auth())?;

        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);

        let mgr = Manager::from_config(pg_config, tls, mgr_config);
        let advertised_listener = Url::parse("tcp://127.0.0.1/")?;

        Pool::builder(mgr)
            .max_size(16)
            .build()
            .map(|pool| Self {
                pool,
                advertised_listener,
                node: N::default(),
                cluster: C::default(),
                schemas: None,
                lake: None,
            })
            .map_err(Into::into)
    }
}

impl Postgres {
    pub fn builder(
        connection: &str,
    ) -> Result<Builder<PhantomData<String>, PhantomData<i32>, Url, Pool>> {
        debug!(connection);
        Builder::from_str(connection)
    }
}
