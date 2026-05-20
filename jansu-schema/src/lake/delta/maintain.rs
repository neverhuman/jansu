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

//! Delta Lake table maintenance and schema migration

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::TableProvider;
use deltalake::{
    DeltaTable, DeltaTableBuilder,
    kernel::{StructField, engine::arrow_conversion::TryFromArrow},
    operations::optimize::OptimizeType,
};
use opentelemetry::KeyValue;
use tracing::debug;
use url::Url;

use crate::Result;

use super::Delta;
use super::metrics::{
    OPTIMIZE_NUM_BATCHES, OPTIMIZE_NUM_FILES_ADDED, OPTIMIZE_NUM_FILES_REMOVED,
    OPTIMIZE_PARTITIONS_OPTIMIZED, OPTIMIZE_TOTAL_CONSIDERED_FILES, OPTIMIZE_TOTAL_FILES_SKIPPED,
};

impl Delta {
    pub(super) async fn z_order(&self, name: &str) -> Result<()> {
        debug!(%name);

        let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? else {
            return Ok(());
        };

        self.optimize(name, OptimizeType::ZOrder(table.config.z_order()))
            .await
    }

    pub(super) async fn compact(&self, name: &str) -> Result<()> {
        self.optimize(name, OptimizeType::Compact).await
    }

    pub(super) async fn optimize(&self, name: &str, optimize_type: OptimizeType) -> Result<()> {
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

    pub(super) async fn migrate_schema(
        &self,
        table: DeltaTable,
        schema: &ArrowSchema,
    ) -> Result<DeltaTable> {
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
