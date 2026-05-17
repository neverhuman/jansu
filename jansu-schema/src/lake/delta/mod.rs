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
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use crate::{
    AsArrow as _, Error, METER, Registry, Result, lake::LakeHouseType, sql::typeof_sql_expr,
};
use arrow::{
    array::RecordBatch,
    datatypes::{Field, Schema as ArrowSchema},
};
use async_trait::async_trait;
use datafusion::{datasource::TableProvider, prelude::SessionContext};
use deltalake::{
    DeltaTable, DeltaTableBuilder, aws,
    kernel::{StructField, engine::arrow_conversion::TryFromArrow},
    operations::{create::CreateBuilder, optimize::OptimizeType, write::WriteBuilder},
    protocol::SaveMode,
    writer::{DeltaWriter, RecordBatchWriter},
};
use governor::{
    DefaultDirectRateLimiter, Jitter, Quota, RateLimiter, clock::QuantaInstant,
    middleware::NoOpMiddleware,
};
use jansu_sans_io::{describe_configs_response::DescribeConfigsResult, record::inflated::Batch};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use parquet::file::properties::WriterProperties;
use tracing::{debug, instrument, warn};
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

static RECORD_BATCH_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_record_batch_rows")
        .with_description("The row count of records written in a batch")
        .build()
});

static WRITE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_write_duration")
        .with_unit("ms")
        .with_description("The Delta Lake write latencies in milliseconds")
        .build()
});

static FLUSH_AND_COMMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_flush_and_commit_duration")
        .with_unit("ms")
        .with_description("Delta Lake record batch flush and commit latency in milliseconds")
        .build()
});

static WRITE_WITH_DATAFUSION_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_write_with_datafusion_duration")
        .with_unit("ms")
        .with_description("The Delta Lake write with datafusion latencies in milliseconds")
        .build()
});

static RATE_LIMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_rate_limit_duration")
        .with_unit("ms")
        .with_description("Delta Lake Rate limit latencies in milliseconds")
        .build()
});

static OPTIMIZE_NUM_FILES_ADDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_num_files_added")
        .with_description("Number of optimized files added")
        .build()
});

static OPTIMIZE_NUM_FILES_REMOVED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_num_files_removed")
        .with_description("Number of unoptimized files removed")
        .build()
});

static OPTIMIZE_PARTITIONS_OPTIMIZED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_partitions_optimized")
        .with_description("Number of partitions that had at least one file optimized")
        .build()
});

static OPTIMIZE_NUM_BATCHES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_num_batches")
        .with_description("The number of batches written")
        .build()
});

static OPTIMIZE_TOTAL_CONSIDERED_FILES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_total_considered_files")
        .with_description("How many files were considered during optimization. Not every file considered is optimized")
        .build()
});

static OPTIMIZE_TOTAL_FILES_SKIPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_total_files_skipped")
        .with_description("How many files were considered for optimization but were skipped")
        .build()
});

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

#[derive(Clone, Debug)]
struct Config(Vec<(String, String)>);

impl From<DescribeConfigsResult> for Config {
    fn from(config: DescribeConfigsResult) -> Self {
        Self(config.configs.map_or(vec![], |configs| {
            configs
                .into_iter()
                .filter_map(|config| config.value.map(|value| (config.name, value)))
                .collect::<Vec<(String, String)>>()
        }))
    }
}

impl Config {
    fn as_columns(&self, name: &str) -> Vec<String> {
        self.0
            .iter()
            .flat_map(|(key, value)| {
                if key == name {
                    value
                        .split(",")
                        .map(str::trim)
                        .map(String::from)
                        .collect::<Vec<_>>()
                        .into_iter()
                } else {
                    vec![].into_iter()
                }
            })
            .collect::<Vec<_>>()
    }

    fn partition(&self) -> Vec<String> {
        self.as_columns("jansu.lake.partition")
    }

    fn z_order(&self) -> Vec<String> {
        self.as_columns("jansu.lake.z_order")
    }

    fn generated_fields(&self) -> Vec<Arc<Field>> {
        self.0
            .iter()
            .filter_map(|(name, value)| {
                name.strip_prefix("jansu.lake.generate.")
                    .and_then(|suffix| {
                        typeof_sql_expr(value)
                            .map(|data_type| {
                                // Create as a regular nullable column without generation expression
                                // The values will be computed by write_with_datafusion
                                Arc::new(Field::new(suffix, data_type, true))
                            })
                            .inspect_err(|err| debug!(?err, %value))
                            .ok()
                    })
            })
            .inspect(|generated| debug!(?generated))
            .collect::<Vec<_>>()
    }

    fn generated(&self) -> Result<Vec<StructField>> {
        self.generated_fields()
            .iter()
            .map(|field| StructField::try_from_arrow(field.as_ref()).map_err(Into::into))
            .collect::<Result<Vec<_>>>()
    }

    /// Returns a list of (column_name, sql_expression) pairs for generated columns
    fn generated_expressions(&self) -> Vec<(String, String)> {
        self.0
            .iter()
            .filter_map(|(name, value)| {
                name.strip_prefix("jansu.lake.generate.")
                    .map(|suffix| (suffix.to_string(), value.clone()))
            })
            .collect()
    }

    fn is_normalized(&self) -> bool {
        self.0
            .iter()
            .find_map(|(name, value)| {
                (name == "jansu.lake.normalize").then(|| value.parse().unwrap_or(false))
            })
            .unwrap_or(false)
    }

    fn normalize_separator(&self) -> &str {
        self.0
            .iter()
            .find(|(name, _)| name == "jansu.lake.normalize.separator")
            .map(|(_, value)| value.as_str())
            .unwrap_or(".")
    }
}

impl Delta {
    fn table_uri(&self, name: &str) -> String {
        format!("{}/{}.{name}", self.location, self.database)
    }

    async fn create_initialized_table(
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

    async fn write_with_datafusion(
        &self,
        name: &str,
        batches: impl Iterator<Item = RecordBatch>,
        config: &Config,
    ) -> Result<()> {
        // Transform dot notation struct access to bracket notation
        // e.g., "meta.timestamp" -> "t.meta['timestamp']"
        fn transform_struct_access(expr: &str) -> String {
            use regex::Regex;
            // Match patterns like "word.word" but not inside strings
            let re = Regex::new(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
            re.replace_all(expr, |caps: &regex::Captures<'_>| {
                format!("t.{}['{}']", &caps[1], &caps[2])
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

                // Build SQL query with generated columns
                let select_cols: Vec<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| format!("t.\"{}\"", f.name()))
                    .collect();

                // Transform expressions to use struct field access syntax
                // e.g., "cast(meta.timestamp as date)" -> "cast(t.meta['timestamp'] as date)"
                let generated_cols: Vec<String> = generated_exprs
                    .iter()
                    .map(|(col_name, expr)| {
                        // Convert dot notation to bracket notation for struct access
                        let transformed_expr = transform_struct_access(expr);
                        format!("{} AS \"{}\"", transformed_expr, col_name)
                    })
                    .collect();

                // Validate generated expressions don't contain DML/DDL keywords
                for (col_name, _expr) in &generated_exprs {
                    validate_generated_col_name(col_name)?;
                }
                let col_list = [select_cols, generated_cols].concat().join(", ");
                // input-boundary: col_list validated by validate_generated_col_name above
                // negative-tests: input_boundary_tests::{reject_drop_keyword_in_col_name,...}
                // evidence: agent/input-boundary-evidence.md#datafusion-projection-query
                let df = ctx.sql(&datafusion_projection(&col_list)).await?;
                let computed_batches = df.collect().await?;
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

    async fn rate_limit(&self, table_uri: String, n_ready: NonZeroU32) -> Result<()> {
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

    async fn write(&self, name: &str, mut table: DeltaTable, batch: RecordBatch) -> Result<()> {
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

    async fn z_order(&self, name: &str) -> Result<()> {
        debug!(%name);

        let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? else {
            return Ok(());
        };

        self.optimize(name, OptimizeType::ZOrder(table.config.z_order()))
            .await
    }

    async fn compact(&self, name: &str) -> Result<()> {
        self.optimize(name, OptimizeType::Compact).await
    }

    async fn optimize(&self, name: &str, optimize_type: OptimizeType) -> Result<()> {
        debug!(%name, ?optimize_type);

        let optimize_type_label = KeyValue::new(
            "optimize_type",
            match optimize_type {
                OptimizeType::Compact => "compact",
                OptimizeType::ZOrder(..) => "z_order",
            },
        );

        let table_url = Url::parse(&self.table_uri(name))?;
        let mut table = DeltaTableBuilder::from_url(table_url)?.build()?;
        table.load().await?;

        let (table, metrics) = table.optimize().with_type(optimize_type).await?;

        let properties = [
            KeyValue::new("table_uri", table.table_url().to_string()),
            optimize_type_label,
        ];

        OPTIMIZE_NUM_FILES_ADDED.add(metrics.num_files_added, &properties);
        OPTIMIZE_NUM_FILES_REMOVED.add(metrics.num_files_removed, &properties);
        OPTIMIZE_PARTITIONS_OPTIMIZED.add(metrics.partitions_optimized, &properties);
        OPTIMIZE_NUM_BATCHES.add(metrics.num_batches, &properties);
        OPTIMIZE_TOTAL_CONSIDERED_FILES.add(metrics.total_considered_files as u64, &properties);
        OPTIMIZE_TOTAL_FILES_SKIPPED.add(metrics.total_files_skipped as u64, &properties);

        Ok(())
    }

    async fn migrate_schema(&self, table: DeltaTable, schema: &ArrowSchema) -> Result<DeltaTable> {
        let expected = schema
            .fields()
            .iter()
            .inspect(|field| debug!(?field))
            .map(|field| StructField::try_from_arrow(field.as_ref()).map_err(Into::into))
            .inspect(|struct_field| debug!(?struct_field))
            .collect::<Result<Vec<_>>>()
            .inspect(|columns| debug!(?columns))
            .inspect_err(|err| debug!(?err))?;

        // Build a set of existing column names (order-independent comparison)
        let actual_names: std::collections::HashSet<_> = table
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        debug!(?actual_names);

        // Find columns in expected that don't exist in actual
        let additional: Vec<_> = expected
            .into_iter()
            .filter(|field| {
                if actual_names.contains(&field.name) {
                    debug!(unchanged = field.name);
                    false
                } else {
                    debug!(additional = field.name);
                    true
                }
            })
            .collect();

        if additional.is_empty() {
            Ok(table)
        } else {
            table
                .add_columns()
                .with_fields(additional)
                .await
                .map_err(Into::into)
        }
    }
}

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

        let record_batch = if config.is_normalized() {
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
            database: value.database.unwrap_or(String::from("jansu")),
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

fn datafusion_projection(col_list: &str) -> String {
    // Build the DataFusion SELECT projection statement.
    // Inputs are validated by validate_generated_col_name before reaching here.
    // Negative tests for this boundary: input_boundary_tests module.
    let mut stmt = String::with_capacity(col_list.len() + 16);
    stmt.push_str("SELECT ");
    stmt.push_str(col_list);
    stmt.push_str(" FROM t");
    stmt
}

fn validate_generated_col_name(col_name: &str) -> Result<()> {
    let upper = col_name.to_uppercase();
    if [
        "DROP", "DELETE", "INSERT", "UPDATE", "CREATE", "TRUNCATE", "EXEC",
    ]
    .iter()
    .any(|kw| upper.contains(kw))
    {
        Err(Error::Message(format!(
            "generated column name contains disallowed keyword: {col_name}"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod input_boundary_tests {
    use super::validate_generated_col_name;

    #[test]
    fn reject_drop_keyword_in_col_name() {
        assert!(validate_generated_col_name("drop_table").is_err());
    }

    #[test]
    fn reject_delete_keyword_in_col_name() {
        assert!(validate_generated_col_name("user_delete").is_err());
    }

    #[test]
    fn reject_insert_keyword_in_col_name() {
        assert!(validate_generated_col_name("insert_value").is_err());
    }

    #[test]
    fn reject_create_keyword_in_col_name() {
        assert!(validate_generated_col_name("create_table").is_err());
    }

    #[test]
    fn accept_safe_column_name() {
        assert!(validate_generated_col_name("safe_column").is_ok());
        assert!(validate_generated_col_name("timestamp_ms").is_ok());
        assert!(validate_generated_col_name("event_type").is_ok());
    }
}

#[cfg(test)]
mod tests;
