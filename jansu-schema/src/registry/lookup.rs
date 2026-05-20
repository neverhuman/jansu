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

//! Schema registry lookup and validation

use std::{
    sync::{Arc, LazyLock},
    time::SystemTime,
};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use arrow::record_batch::RecordBatch;

use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};

use jansu_sans_io::record::inflated::Batch;
use tracing::{debug, instrument};
use url::Url;

use crate::{METER, Result, Validator, avro, json, proto};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use crate::{AsArrow, Error, lake};

use super::{Builder, CachedSchema, Registry, Schema};

static VALIDATION_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("registry_validation_duration")
        .with_unit("ms")
        .with_description("The registry validation request latencies in milliseconds")
        .build()
});

static VALIDATION_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("registry_validation_error")
        .with_description("The registry validation error count")
        .build()
});

impl Registry {
    pub fn new(object_store: impl ObjectStore) -> Self {
        Builder::new(object_store).build()
    }

    pub fn builder(object_store: impl ObjectStore) -> Builder {
        Builder::new(object_store)
    }

    pub fn builder_try_from_url(url: &Url) -> Result<Builder> {
        Builder::try_from(url)
    }

    #[instrument(skip(self))]
    pub async fn schema(&self, topic: &str) -> Result<Option<Schema>> {
        let proto = Path::from(format!("{topic}.proto"));
        let json = Path::from(format!("{topic}.json"));
        let avro = Path::from(format!("{topic}.avsc"));

        if let Some(cached) = self.schemas.lock().map(|guard| guard.get(topic).cloned())? {
            if self.cache_expiry_after.is_some_and(|cache_expiry_after| {
                match SystemTime::now().duration_since(cached.loaded_at) {
                    Ok(elapsed) => elapsed > cache_expiry_after,
                    Err(_) => false,
                }
            }) {
                return Ok(Some(cached.schema));
            } else {
                debug!(cache_expiry = topic);
            }
        }

        if let Ok(get_result) = self
            .object_store
            .get(&proto)
            .await
            .inspect(|get_result| debug!(?get_result))
            .inspect_err(|err| debug!(?err))
        {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(proto::Schema::try_from)
                .map(Box::new)
                .map(Schema::Proto)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.insert(topic.to_owned(), CachedSchema::new(schema.clone()))
                        })
                        .and(Ok(Some(schema)))
                })
        } else if let Ok(get_result) = self.object_store.get(&json).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(json::Schema::try_from)
                .map(Arc::new)
                .map(Schema::Json)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.insert(topic.to_owned(), CachedSchema::new(schema.clone()))
                        })
                        .and(Ok(Some(schema)))
                })
        } else if let Ok(get_result) = self.object_store.get(&avro).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(avro::Schema::try_from)
                .map(Box::new)
                .map(Schema::Avro)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.insert(topic.to_owned(), CachedSchema::new(schema.clone()))
                        })
                        .and(Ok(Some(schema)))
                })
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self, batch))]
    pub async fn validate(&self, topic: &str, batch: &Batch) -> Result<()> {
        let validation_start = SystemTime::now();

        let Some(schema) = self.schema(topic).await? else {
            debug!(no_schema_for_topic = %topic);
            return Ok(());
        };

        schema
            .validate(batch)
            .inspect(|_| {
                VALIDATION_DURATION.record(
                    validation_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[KeyValue::new("topic", topic.to_owned())],
                )
            })
            .inspect_err(|err| {
                VALIDATION_ERROR.add(
                    1,
                    &[
                        KeyValue::new("topic", topic.to_owned()),
                        KeyValue::new("reason", err.to_string()),
                    ],
                )
            })
    }
}

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
impl AsArrow for Registry {
    #[instrument(skip(self, batch), ret)]
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: lake::LakeHouseType,
    ) -> Result<RecordBatch> {
        let start = SystemTime::now();

        let schema = self
            .schema(topic)
            .await
            .and_then(|schema| schema.ok_or(Error::TopicWithoutSchema(topic.to_owned())))?;

        schema
            .as_arrow(topic, partition, batch, lake_type)
            .await
            .inspect(|_| {
                lake::AS_ARROW_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[KeyValue::new("topic", topic.to_owned())],
                )
            })
            .inspect_err(|err| debug!(?err))
    }
}
