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

//! Schema registry tests

use crate::{Error, Registry, Result};
use bytes::Bytes;
use jansu_sans_io::record::Record;
use jansu_sans_io::{ErrorCode, record::inflated::Batch};
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
use serde_json::json;
use std::num::TryFromIntError;
use std::{fs::File, sync::Arc, thread};
use tracing::{debug, subscriber::DefaultGuard};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::ParseError;
use url::Url;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use arrow::{datatypes::DataType, error::ArrowError};

#[cfg(feature = "iceberg")]
use iceberg::spec::DataFileBuilderError;

#[cfg(any(feature = "iceberg", feature = "delta"))]
use datafusion::error::DataFusionError;

#[cfg(feature = "delta")]
use deltalake::DeltaTableError;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use parquet::errors::ParquetError;

fn init_tracing() -> Result<DefaultGuard> {
    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

const DEF_PROTO: &[u8] = br#"
  syntax = 'proto3';

  message Key {
    int32 id = 1;
  }

  message Value {
    string name = 1;
    string email = 2;
  }
"#;

const PQR_AVRO: &[u8] = br#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "long", "default": 43}
        ]
    }
"#;

async fn populate() -> Result<Registry> {
    let _guard = init_tracing()?;

    let object_store = InMemory::new();

    let location = Path::from("abc.json");
    let payload = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number",
                "multipleOf": 10
            }
        }
    }))
    .map(Bytes::from)
    .map(PutPayload::from)?;

    _ = object_store.put(&location, payload).await?;

    let location = Path::from("def.proto");
    let payload = PutPayload::from(Bytes::from_static(DEF_PROTO));
    _ = object_store.put(&location, payload).await?;

    let location = Path::from("pqr.avsc");
    let payload = PutPayload::from(Bytes::from_static(PQR_AVRO));
    _ = object_store.put(&location, payload).await?;

    Ok(Registry::new(object_store))
}

#[tokio::test]
async fn abc_valid() -> Result<()> {
    let _guard = init_tracing()?;

    let registry = populate().await?;

    let key = Bytes::from_static(b"5450");

    let batch = Batch::builder()
        .record(Record::builder().key(key.clone().into()))
        .build()?;

    registry.validate("abc", &batch).await?;

    Ok(())
}

#[tokio::test]
async fn abc_invalid() -> Result<()> {
    let _guard = init_tracing()?;
    let registry = populate().await?;

    let key = Bytes::from_static(b"545");

    let batch = Batch::builder()
        .record(Record::builder().key(key.clone().into()))
        .build()?;

    assert!(matches!(
        registry
            .validate("abc", &batch)
            .await
            .inspect_err(|err| debug!(?err)),
        Err(Error::Api(ErrorCode::InvalidRecord))
    ));

    Ok(())
}

#[tokio::test]
async fn pqr_valid() -> Result<()> {
    let _guard = init_tracing()?;
    let registry = populate().await?;

    let key = Bytes::from_static(b"5450");

    let batch = Batch::builder()
        .record(Record::builder().key(key.clone().into()))
        .build()?;

    registry.validate("pqr", &batch).await?;

    Ok(())
}

#[test]
fn error_size_of() -> Result<()> {
    let _guard = init_tracing()?;

    debug!(error = size_of::<Error>());
    debug!(anyhow = size_of::<anyhow::Error>());
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    debug!(arrow = size_of::<ArrowError>());
    debug!(avro_to_json = size_of::<apache_avro::types::Value>());
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    debug!(data_file_builder = size_of::<DataFileBuilderError>());
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    debug!(data_fusion = size_of::<Box<DataFusionError>>());
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    debug!(delta_table = size_of::<Box<DeltaTableError>>());
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    debug!(iceberg = size_of::<Box<::iceberg::Error>>());
    debug!(sans_io = size_of::<jansu_sans_io::Error>());
    debug!(object_store = size_of::<object_store::Error>());

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    debug!(parquet = size_of::<ParquetError>());

    debug!(parse_filter = size_of::<ParseError>());
    debug!(protobuf_json_mapping = size_of::<protobuf_json_mapping::ParseError>());
    debug!(protobuf_json_mapping_print = size_of::<protobuf_json_mapping::PrintError>());
    debug!(protobuf = size_of::<protobuf::Error>());
    debug!(serde_json = size_of::<serde_json::Error>());
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    debug!(sql_parser = size_of::<datafusion::logical_expr::sqlparser::parser::ParserError>());
    debug!(try_from_int = size_of::<TryFromIntError>());
    debug!(url = size_of::<Url>());
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    debug!(unsupported_schema_runtime_value = size_of::<(DataType, serde_json::Value)>());
    debug!(uuid = size_of::<uuid::Error>());
    Ok(())
}
