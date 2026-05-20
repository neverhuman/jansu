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
    marker::PhantomData,
    num::NonZeroU32,
    sync::{Arc, Mutex},
};

use crate::{AsArrow as _, Error, Registry, Result, lake::LakeHouseType};
use async_trait::async_trait;
use deltalake::{DeltaTable, aws};
use governor::{
    DefaultDirectRateLimiter, Quota, RateLimiter, clock::QuantaInstant, middleware::NoOpMiddleware,
};
use jansu_sans_io::{describe_configs_response::DescribeConfigsResult, record::inflated::Batch};
use tracing::{debug, instrument};
use url::Url;

use super::{House, LakeHouse};

#[derive(Clone, Debug, Default)]
pub struct Builder<L = PhantomData<Url>, R = PhantomData<Registry>> {
    location: L,
    schema_registry: R,
    database: Option<String>,
    records_per_second: Option<u32>,
}

impl<L, R> Builder<L, R> {
    pub fn location(self, location: Url) -> Builder<Url, R> {
        Builder {
            location,
            schema_registry: self.schema_registry,
            database: self.database,
            records_per_second: self.records_per_second,
        }
    }

    pub fn schema_registry(self, schema_registry: Registry) -> Builder<L, Registry> {
        Builder {
            location: self.location,
            schema_registry,
            database: self.database,
            records_per_second: self.records_per_second,
        }
    }

    pub fn database(self, database: Option<String>) -> Self {
        Self { database, ..self }
    }

    pub fn records_per_second(self, records_per_second: Option<u32>) -> Self {
        Self {
            records_per_second,
            ..self
        }
    }
}

impl Builder<Url, Registry> {
    pub fn build(self) -> Result<House> {
        Delta::try_from(self).map(House::Delta)
    }
}

#[derive(Clone, Debug)]
pub struct Delta {
    location: Url,
    schema_registry: Registry,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    database: String,
    rate_limiter: Option<Arc<DefaultDirectRateLimiter<NoOpMiddleware<QuantaInstant>>>>,
}

#[derive(Clone, Debug)]
struct Table {
    config: Config,
    delta_table: DeltaTable,
}

mod config;
mod maintain;
mod metrics;
mod write;

use config::Config;

#[async_trait]
impl LakeHouse for Delta {
    #[instrument(skip(self, inflated, config), ret)]
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        inflated: &Batch,
        config: DescribeConfigsResult,
    ) -> Result<()> {
        let config = Config::from(config);
        debug!(?config);

        let record_batch = self
            .schema_registry
            .as_arrow(topic, partition, inflated, LakeHouseType::Delta)
            .await?;

        let record_batch = if config.is_normalized()? {
            record_batch.normalize(config.normalize_separator(), None)?
        } else {
            record_batch
        };

        debug!(%topic, partition, offset, rows = record_batch.num_rows(), columns = record_batch.num_columns(), ?config);

        let table =
            if let Some(table) = self.tables.lock().map(|guard| guard.get(topic).cloned())? {
                table.delta_table
            } else {
                self.create_initialized_table(topic, record_batch.schema().as_ref(), config.clone())
                    .await?
            };

        let table = self
            .migrate_schema(table, record_batch.schema().as_ref())
            .await?;

        if config.generated_fields().is_empty() {
            _ = self.write(topic, table, record_batch).await?;
        } else {
            _ = self
                .write_with_datafusion(topic, [record_batch].into_iter(), &config)
                .await
                .inspect(|delta_table| debug!(?delta_table))
                .inspect_err(|err| debug!(?err))?;
        }

        Ok(())
    }

    #[instrument(skip(self), ret)]
    async fn maintain(&self) -> Result<()> {
        debug!(?self);

        let names = self
            .tables
            .lock()
            .map(|guard| guard.keys().map(|name| name.to_owned()).collect::<Vec<_>>())
            .inspect(|names| debug!(?names))
            .inspect_err(|err| debug!(?err))?;

        for name in names {
            debug!(name);

            self.compact(&name).await?;
            self.z_order(&name).await?;
        }

        Ok(())
    }

    #[instrument(skip(self), ret)]
    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Delta)
    }
}

impl TryFrom<Builder<Url, Registry>> for Delta {
    type Error = Error;

    fn try_from(value: Builder<Url, Registry>) -> Result<Self, Self::Error> {
        aws::register_handlers(None);

        Ok(Self {
            location: value.location,
            schema_registry: value.schema_registry,
            database: match value.database {
                Some(database) => database,
                None => String::from("jansu"),
            },
            tables: Arc::new(Mutex::new(HashMap::new())),
            rate_limiter: value
                .records_per_second
                .and_then(NonZeroU32::new)
                .map(Quota::per_second)
                .map(RateLimiter::direct)
                .map(Arc::new)
                .inspect(|rate_limiter| debug!(?rate_limiter)),
        })
    }
}

#[cfg(test)]
mod tests;
