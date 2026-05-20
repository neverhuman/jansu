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

use std::{
    collections::HashMap,
    env::vars,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    Error, Registry, Result,
    lake::{LakeHouse, LakeHouseType},
};
use async_trait::async_trait;
use iceberg::memory::MemoryCatalogBuilder;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
    spec::{Schema, TableMetadataBuilder},
    table::Table,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use jansu_sans_io::{describe_configs_response::DescribeConfigsResult, record::inflated::Batch};
use tracing::debug;
use url::Url;

use super::House;

fn env_mapping(k: &str) -> Option<&str> {
    match k {
        "AWS_ACCESS_KEY_ID" => Some(S3_ACCESS_KEY_ID),
        "AWS_SECRET_ACCESS_KEY" => Some(S3_SECRET_ACCESS_KEY),
        "AWS_DEFAULT_REGION" => Some(S3_REGION),
        "AWS_ENDPOINT" => Some(S3_ENDPOINT),
        _ => None,
    }
}

pub fn env_s3_props() -> impl Iterator<Item = (String, String)> {
    vars().filter_map(|(k, v)| env_mapping(k.as_str()).map(|k| (k.to_owned(), v)))
}

#[derive(Clone, Debug, Default)]
pub struct Builder<C = PhantomData<Url>, L = PhantomData<Url>, R = PhantomData<Registry>> {
    location: L,
    catalog: C,
    schema_registry: R,
    namespace: Option<String>,
    warehouse: Option<String>,
}

impl<C, L, R> Builder<C, L, R> {
    pub fn location(self, location: Url) -> Builder<C, Url, R> {
        Builder {
            location,
            catalog: self.catalog,
            schema_registry: self.schema_registry,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn catalog(self, catalog: Url) -> Builder<Url, L, R> {
        Builder {
            location: self.location,
            catalog,
            schema_registry: self.schema_registry,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn schema_registry(self, schema_registry: Registry) -> Builder<C, L, Registry> {
        Builder {
            catalog: self.catalog,
            location: self.location,
            schema_registry,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn namespace(self, namespace: Option<String>) -> Self {
        Self { namespace, ..self }
    }

    pub fn warehouse(self, warehouse: Option<String>) -> Self {
        Self { warehouse, ..self }
    }
}

impl Builder<Url, Url, Registry> {
    pub async fn build(self) -> Result<House> {
        Iceberg::new(self).await.map(House::Iceberg)
    }
}

#[derive(Clone, Debug)]
pub struct Iceberg {
    catalog: Arc<dyn Catalog>,
    namespace: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    schema_registry: Registry,
}

impl Iceberg {
    async fn new(value: Builder<Url, Url, Registry>) -> Result<Self> {
        let catalog = iceberg_catalog(&value.catalog, value.warehouse.clone()).await?;
        Ok(Self {
            catalog,
            namespace: value.namespace.unwrap_or(String::from("jansu")),
            tables: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: value.schema_registry,
        })
    }
}

async fn iceberg_catalog(catalog: &Url, warehouse: Option<String>) -> Result<Arc<dyn Catalog>> {
    debug!(%catalog, ?warehouse);

    match (catalog.scheme(), catalog.path()) {
        ("http" | "https", "/") | ("http" | "https", _) => {
            let uri = if catalog.path() == "/" {
                format!(
                    "{}://{}:{}",
                    catalog.scheme(),
                    catalog.host_str().unwrap_or("localhost"),
                    catalog.port().unwrap_or(80)
                )
            } else {
                catalog.to_string()
            };

            let mut props: HashMap<String, String> = env_s3_props().collect();
            _ = props.insert(REST_CATALOG_PROP_URI.to_string(), uri);
            if let Some(wh) = warehouse {
                _ = props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), wh);
            }

            let catalog = RestCatalogBuilder::default()
                .load("rest", props)
                .await
                .map_err(|e| Error::Iceberg(Box::new(e)))?;

            Ok(Arc::new(catalog) as Arc<dyn Catalog>)
        }

        ("memory", _) => {
            let catalog = MemoryCatalogBuilder::default()
                .load("memory", HashMap::new())
                .await
                .map_err(|e| Error::Iceberg(Box::new(e)))?;
            Ok(Arc::new(catalog) as Arc<dyn Catalog>)
        }

        (_otherwise, _) => Err(Error::UnsupportedIcebergCatalogUrl(catalog.to_owned())),
    }
}

impl Iceberg {
    async fn create_namespace(&self) -> Result<NamespaceIdent> {
        let namespace_ident = NamespaceIdent::new(self.namespace.clone());
        debug!(%namespace_ident);

        if !self
            .catalog
            .namespace_exists(&namespace_ident)
            .await
            .inspect(|namespace| debug!(?namespace))
            .inspect_err(|err| debug!(?err))?
        {
            _ = self
                .catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .inspect(|namespace| debug!(?namespace))
                .inspect_err(|err| debug!(?err))?;
        }

        Ok(namespace_ident)
    }

    async fn load_or_create_table(&self, name: &str, schema: Schema) -> Result<Table> {
        if let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? {
            return Ok(table);
        }

        let namespace_ident = self.create_namespace().await?;
        let table_ident = TableIdent::new(namespace_ident.clone(), name.into());

        let table = if self.catalog.table_exists(&table_ident).await? {
            let table = self
                .catalog
                .load_table(&table_ident)
                .await
                .inspect_err(|err| debug!(?err))?;

            if table.metadata().current_schema().as_ref() != &schema {
                debug!(current = ?table.metadata(), ?schema);

                _ = TableMetadataBuilder::new_from_metadata(
                    table.metadata().to_owned(),
                    table
                        .metadata_location()
                        .map(|location| location.to_owned()),
                )
                .add_schema(schema.clone())?
                .set_current_schema(-1)?
                .build()
                .inspect(|update| {
                    debug!(?update.metadata);
                    debug!(?update.changes);
                    debug!(?update.expired_metadata_logs);
                })?;
            }

            table
        } else {
            self.catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(name.into())
                        .schema(schema.clone())
                        .build(),
                )
                .await
                .inspect(|table| debug!(?table))
                .inspect_err(|err| debug!(?err))?
        };

        _ = self
            .tables
            .lock()
            .map(|mut guard| guard.insert(name.to_owned(), table.clone()))?;

        Ok(table)
    }
}

#[async_trait]
impl LakeHouse for Iceberg {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        inflated: &Batch,
        config: DescribeConfigsResult,
    ) -> Result<()> {
        let _ = config;
        self.store_batch(topic, partition, offset, inflated).await
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Iceberg)
    }
}

mod store;

#[cfg(test)]
mod tests;
