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

//! JSON schema Arrow conversion tests

use super::*;

#[cfg(feature = "iceberg")]
use arrow::util::pretty::pretty_format_batches;

#[cfg(feature = "iceberg")]
use datafusion::prelude::*;

#[cfg(feature = "iceberg")]
use iceberg::{
    io::FileIOBuilder,
    spec::{
        DataFile, DataFileFormat::Parquet, Schema as IcebergSchema, SchemaRef as IcebergSchemaRef,
    },
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, LocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};

#[cfg(feature = "iceberg")]
use parquet::file::properties::WriterProperties;

use crate::Error;
use bytes::Bytes;
use jansu_sans_io::record::Record;
use serde_json::json;
use std::{fs::File, sync::Arc, thread};
use tracing::error;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

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

#[cfg(feature = "iceberg")]
async fn iceberg_write(record_batch: RecordBatch) -> Result<Vec<DataFile>> {
    let iceberg_schema = IcebergSchema::try_from(record_batch.schema().as_ref())
        .map(IcebergSchemaRef::new)
        .inspect(|schema| debug!(?schema))
        .inspect_err(|err| debug!(?err))?;

    let memory = FileIOBuilder::new("memory").build()?;

    #[derive(Clone)]
    struct JsonArrowLocation;

    impl LocationGenerator for JsonArrowLocation {
        fn generate_location(
            &self,
            _partition_key: Option<&iceberg::spec::PartitionKey>,
            file_name: &str,
        ) -> String {
            format!("json-arrow/{file_name}")
        }
    }

    let parquet_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema);

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        memory,
        JsonArrowLocation,
        DefaultFileNameGenerator::new("json".into(), None, Parquet),
    );

    let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
        .build(None)
        .await
        .inspect_err(|err| error!(?err))?;

    data_file_writer
        .write(record_batch)
        .await
        .inspect_err(|err| debug!(?err))?;

    data_file_writer
        .close()
        .await
        .inspect(|data_files| debug!(?data_files))
        .inspect_err(|err| debug!(?err))
        .map_err(Into::into)
}

mod basic;
mod nested;
mod primitive;
