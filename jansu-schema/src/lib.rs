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

//! Schema
//!
//! Schema includes the following:
//! - Validation of Kafka messages with an AVRO, JSON or Protobuf schema

use std::sync::LazyLock;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use arrow::record_batch::RecordBatch;

use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_semantic_conventions::SCHEMA_URL;

use jansu_sans_io::record::inflated::Batch;
use serde_json::Value;

pub mod avro;
mod error;
pub mod json;
pub mod lake;
pub mod proto;
mod registry;

#[cfg(feature = "delta")]
pub(crate) mod sql;

#[cfg(test)]
mod tests;

pub use error::{Error, Result};
pub use registry::{Builder, Registry, Schema};

pub(crate) const ARROW_LIST_FIELD_NAME: &str = "element";

/// Validate a Batch with a Schema
pub trait Validator {
    fn validate(&self, batch: &Batch) -> Result<()>;
}

/// Represent a Batch in the Arrow columnar data format
#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
trait AsArrow {
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: lake::LakeHouseType,
    ) -> Result<RecordBatch>;
}

/// Convert a JSON message into a Kafka record
pub trait AsKafkaRecord {
    fn as_kafka_record(&self, value: &Value) -> Result<jansu_sans_io::record::Builder>;
}

/// Convert a Batch into a JSON value
pub trait AsJsonValue {
    fn as_json_value(&self, batch: &Batch) -> Result<Value>;
}

/// Generate a record
pub trait Generator {
    fn generate(&self) -> Result<jansu_sans_io::record::Builder>;
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});
