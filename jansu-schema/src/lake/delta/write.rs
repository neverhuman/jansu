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

//! Delta Lake table creation and write operations

use std::{
    num::NonZeroU32,
    time::{Duration, SystemTime},
};

use arrow::{array::RecordBatch, datatypes::Schema as ArrowSchema};
use datafusion::prelude::SessionContext;
use deltalake::{
    DeltaTable, DeltaTableBuilder,
    kernel::{StructField, engine::arrow_conversion::TryFromArrow},
    operations::create::CreateBuilder,
    operations::write::WriteBuilder,
    protocol::SaveMode,
    writer::{DeltaWriter, RecordBatchWriter},
};
use governor::Jitter;
use opentelemetry::KeyValue;
use parquet::file::properties::WriterProperties;
use tracing::{debug, warn};
use url::Url;

use crate::{Error, Result};

use super::Delta;
use super::Table;
use super::config::Config;
use super::metrics::{
    FLUSH_AND_COMMIT_DURATION, RATE_LIMIT_DURATION, RECORD_BATCH_ROWS, WRITE_DURATION,
    WRITE_WITH_DATAFUSION_DURATION,
};

impl Delta {
    pub(super) fn table_uri(&self, name: &str) -> String {
        format!("{}/{}.{name}", self.location, self.database)
    }

    pub(super) async fn create_initialized_table(
        &self,
        name: &str,
        schema: &ArrowSchema,
        config: Config,
    ) -> Result<DeltaTable> {
        debug!(?name, ?schema);

        let columns = schema
            .fields()
            .iter()
            .inspect(|field| debug!(?field))
            .map(|field| StructField::try_from_arrow(field.as_ref()).map_err(Into::into))
            .inspect(|struct_field| debug!(?struct_field))
            .collect::<Result<Vec<_>>>()
            .inspect(|columns| debug!(?columns))
            .inspect_err(|err| debug!(?err))?;

        // Validate partition columns exist in schema before creating table
        let generated_columns = config.generated()?;
        let all_column_names: std::collections::HashSet<_> = columns
            .iter()
            .chain(generated_columns.iter())
            .map(|c| c.name.as_str())
            .collect();

        for partition_col in config.partition() {
            if !all_column_names.contains(partition_col.as_str()) {
                return Err(Error::DeltaTable(Box::new(
                    deltalake::errors::DeltaTableError::Generic(format!(
                        "Partition column {} not found in schema",
                        partition_col
                    )),
                )));
            }
        }

        let table_url = Url::parse(&self.table_uri(name))?;

        let table = match CreateBuilder::new()
            .with_location(table_url.to_string())
            .with_save_mode(SaveMode::Ignore)
            .with_columns(columns.into_iter().chain(generated_columns))
            .with_partition_columns(config.partition())
            .await
            .inspect(|table| debug!(?table))
            .inspect_err(|err| debug!(?err))
        {
            Err(deltalake::DeltaTableError::VersionAlreadyExists(_)) => {
                if let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? {
                    return Ok(table.delta_table);
                }

                let mut table = DeltaTableBuilder::from_url(table_url)?.build()?;
                table
                    .load()
                    .await
                    .inspect(|table| debug!(?table))
                    .inspect_err(|err| debug!(?err))
                    .and(Ok(table))
            }

            otherwise => otherwise,
        }?;

        self.tables.lock().map(|mut guard| {
            _ = guard
                .entry(name.to_owned())
                .and_modify(|existing| {
                    if table.version() > existing.delta_table.version() {
                        existing.delta_table = table.to_owned()
                    } else {
                        warn!(
                            name,
                            existing = existing.delta_table.version(),
                            current = table.version()
                        );
                    }
                })
                .or_insert(Table {
                    delta_table: table.clone(),
                    config,
                });
        })?;

        Ok(table)
    }

    pub(super) async fn write_with_datafusion(
        &self,
        name: &str,
        batches: impl Iterator<Item = RecordBatch>,
        config: &Config,
    ) -> Result<()> {
        // Transform dot notation struct access to bracket notation.
        // e.g., "meta.timestamp" -> "meta['timestamp']"
        fn transform_struct_access(expr: &str) -> String {
            use regex::Regex;
            // Match patterns like "word.word" but not inside strings
            let re = Regex::new(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
            re.replace_all(expr, |caps: &regex::Captures<'_>| {
                format!("{}['{}']", &caps[1], &caps[2])
            })
            .to_string()
        }

        let start = SystemTime::now();

        let table_url = Url::parse(&self.table_uri(name))?;
        let mut table = DeltaTableBuilder::from_url(table_url)?.build()?;
        table.load().await.inspect_err(|err| debug!(?err))?;

        // Compute generated columns using DataFusion
        let generated_exprs = config.generated_expressions();
        let batches_with_generated: Vec<RecordBatch> = if generated_exprs.is_empty() {
            batches.collect()
        } else {
            let ctx = SessionContext::new();
            let mut result_batches = Vec::new();

            for batch in batches {
                // Register the batch as a table
                _ = ctx.register_batch("t", batch.clone())?;

                let df = ctx.table("t").await?;
                let projection = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| format!("\"{}\"", field.name()))
                    .chain(generated_exprs.iter().map(|(col_name, expr)| {
                        let transformed_expr = transform_struct_access(expr);
                        format!("{} AS \"{}\"", transformed_expr, col_name)
                    }))
                    .collect::<Vec<_>>();
                let projection = projection.iter().map(String::as_str).collect::<Vec<_>>();

                let computed_batches = df.select_exprs(&projection)?.collect().await?;
                result_batches.extend(computed_batches);

                // Deregister the table for the next iteration
                _ = ctx.deregister_table("t")?;
            }

            result_batches
        };

        // Write using WriteBuilder
        let snapshot = table.snapshot().ok().map(|s| s.snapshot().clone());
        let table = WriteBuilder::new(table.log_store(), snapshot)
            .with_input_batches(batches_with_generated)
            .await
            .inspect_err(|err| debug!(?err))
            .inspect(|table| {
                WRITE_WITH_DATAFUSION_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[KeyValue::new("table_uri", table.table_url().to_string())],
                )
            })?;

        self.tables.lock().map(|mut guard| {
            _ = guard.entry(name.to_string()).and_modify(|existing| {
                if table.version() > existing.delta_table.version() {
                    existing.delta_table = table.to_owned();
                } else {
                    warn!(
                        name,
                        existing = existing.delta_table.version(),
                        current = table.version()
                    );
                }
            });
        })?;

        Ok(())
    }

    pub(super) async fn rate_limit(&self, table_uri: String, n_ready: NonZeroU32) -> Result<()> {
        if let Some(ref rate_limiter) = self.rate_limiter {
            let start = SystemTime::now();

            let attributes = [KeyValue::new("table_uri", table_uri)];

            rate_limiter
                .until_n_ready_with_jitter(n_ready, Jitter::up_to(Duration::from_millis(50)))
                .await
                .inspect(|_| {
                    RATE_LIMIT_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &attributes,
                    )
                })
                .map_err(Into::into)
        } else {
            Ok(())
        }
    }

    pub(super) async fn write(
        &self,
        name: &str,
        mut table: DeltaTable,
        batch: RecordBatch,
    ) -> Result<()> {
        let properties = [KeyValue::new("table_uri", table.table_url().to_string())];

        if let Some(num_rows) = NonZeroU32::new(batch.num_rows() as u32) {
            self.rate_limit(table.table_url().to_string(), num_rows)
                .await?;
        }

        let num_rows = batch.num_rows() as u64;

        let mut writer = RecordBatchWriter::for_table(&table)
            .map(|batch_writer| batch_writer.with_writer_properties(WriterProperties::default()))
            .inspect_err(|err| debug!(?err))?;

        {
            let start = SystemTime::now();

            writer
                .write(batch)
                .await
                .inspect_err(|err| debug!(?err))
                .inspect(|_| {
                    RECORD_BATCH_ROWS.add(num_rows, &properties);

                    WRITE_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &properties,
                    )
                })?
        }

        {
            let start = SystemTime::now();

            _ = writer
                .flush_and_commit(&mut table)
                .await
                .inspect_err(|err| debug!(?err))
                .inspect(|_| {
                    FLUSH_AND_COMMIT_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &[KeyValue::new("table", table.table_url().to_string())],
                    )
                })?;
        }

        self.tables.lock().map(|mut guard| {
            _ = guard.entry(name.to_string()).and_modify(|existing| {
                if table.version() > existing.delta_table.version() {
                    existing.delta_table = table;
                } else {
                    warn!(
                        name,
                        existing = existing.delta_table.version(),
                        current = table.version()
                    );
                }
            });
        })?;

        Ok(())
    }
}
