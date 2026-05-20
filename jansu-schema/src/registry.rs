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

//! Schema registry

use std::{
    collections::BTreeMap,
    env::{self},
    path::PathBuf,
    result,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use arrow::record_batch::RecordBatch;

use object_store::{
    DynObjectStore, ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory,
};

use jansu_sans_io::record::inflated::Batch;
use tracing::{debug, instrument};
use url::Url;

use crate::{AsKafkaRecord, Error, Generator, Result, Validator, avro, json, proto};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use crate::{AsArrow, lake};

// Schema
//
// This is wrapper enumeration of the supported schema types
#[derive(Clone, Debug)]
pub enum Schema {
    Avro(Box<avro::Schema>),
    Json(Arc<json::Schema>),
    Proto(Box<proto::Schema>),
}

#[derive(Clone, Debug)]
struct CachedSchema {
    loaded_at: SystemTime,
    schema: Schema,
}

impl CachedSchema {
    fn new(schema: Schema) -> Self {
        Self {
            schema,
            loaded_at: SystemTime::now(),
        }
    }
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &serde_json::Value) -> Result<jansu_sans_io::record::Builder> {
        debug!(?value);

        match self {
            Self::Avro(schema) => schema.as_kafka_record(value),
            Self::Json(schema) => schema.as_kafka_record(value),
            Self::Proto(schema) => schema.as_kafka_record(value),
        }
    }
}

impl Validator for Schema {
    #[instrument(skip(self, batch), ret)]
    fn validate(&self, batch: &Batch) -> Result<()> {
        match self {
            Self::Avro(schema) => schema.validate(batch),
            Self::Json(schema) => schema.validate(batch),
            Self::Proto(schema) => schema.validate(batch),
        }
    }
}

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
impl AsArrow for Schema {
    #[instrument(skip(self, batch), ret)]
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: lake::LakeHouseType,
    ) -> Result<RecordBatch> {
        match self {
            Self::Avro(schema) => schema.as_arrow(topic, partition, batch, lake_type).await,
            Self::Json(schema) => schema.as_arrow(topic, partition, batch, lake_type).await,
            Self::Proto(schema) => schema.as_arrow(topic, partition, batch, lake_type).await,
        }
    }
}

impl crate::AsJsonValue for Schema {
    #[instrument(skip(self, batch), ret)]
    fn as_json_value(&self, batch: &Batch) -> Result<serde_json::Value> {
        debug!(?batch);

        match self {
            Self::Avro(schema) => schema.as_json_value(batch),
            Self::Json(schema) => schema.as_json_value(batch),
            Self::Proto(schema) => schema.as_json_value(batch),
        }
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<jansu_sans_io::record::Builder> {
        match self {
            Schema::Avro(schema) => schema.generate(),
            Schema::Json(schema) => schema.generate(),
            Schema::Proto(schema) => schema.generate(),
        }
    }
}

type SchemaCache = Arc<Mutex<BTreeMap<String, CachedSchema>>>;

// Schema Registry
#[derive(Clone, Debug)]
pub struct Registry {
    object_store: Arc<DynObjectStore>,
    schemas: SchemaCache,
    cache_expiry_after: Option<Duration>,
}

impl FromStr for Registry {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        Url::parse(s)
            .map_err(Into::into)
            .and_then(|location| Builder::try_from(&location))
            .map(Into::into)
    }
}

// Schema Registry builder
#[derive(Clone, Debug)]
pub struct Builder {
    object_store: Arc<DynObjectStore>,
    cache_expiry_after: Option<Duration>,
}

impl TryFrom<&Url> for Builder {
    type Error = Error;

    fn try_from(storage: &Url) -> Result<Self, Self::Error> {
        debug!(%storage);

        match storage.scheme() {
            "s3" => {
                let bucket_name = storage.host_str().unwrap_or("schema");

                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .build()
                    .map_err(Into::into)
                    .map(Self::new)
            }

            "file" => {
                let path = if storage.path().starts_with('/') {
                    PathBuf::from(storage.path())
                } else {
                    let mut path =
                        env::current_dir().inspect(|current_dir| debug!(?current_dir))?;

                    if let Some(domain) = storage.domain() {
                        path.push(domain);
                    }

                    path.push(storage.path());
                    path
                };

                debug!(?path);

                LocalFileSystem::new_with_prefix(path)
                    .map_err(Into::into)
                    .map(Self::new)
            }

            "memory" => Ok(Self::new(InMemory::new())),

            _unsupported => Err(Error::UnsupportedSchemaRegistryUrl(storage.to_owned())),
        }
    }
}

impl From<Builder> for Registry {
    fn from(builder: Builder) -> Self {
        Self {
            object_store: builder.object_store,
            schemas: Arc::new(Mutex::new(BTreeMap::new())),
            cache_expiry_after: builder.cache_expiry_after,
        }
    }
}

impl Builder {
    pub fn new(object_store: impl ObjectStore) -> Self {
        Self {
            object_store: Arc::new(object_store),
            cache_expiry_after: None,
        }
    }

    pub fn with_cache_expiry_after(self, cache_expiry_after: Option<Duration>) -> Self {
        Self {
            cache_expiry_after,
            ..self
        }
    }

    pub fn build(self) -> Registry {
        Registry::from(self)
    }
}

mod lookup;
