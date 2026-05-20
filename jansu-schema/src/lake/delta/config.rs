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

//! Delta Lake topic configuration

use std::sync::Arc;

use arrow::datatypes::Field;
use deltalake::kernel::{StructField, engine::arrow_conversion::TryFromArrow};
use jansu_sans_io::describe_configs_response::DescribeConfigsResult;
use tracing::debug;

use crate::{Error, Result, sql::typeof_sql_expr};

#[derive(Clone, Debug)]
pub(super) struct Config(pub(super) Vec<(String, String)>);

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
    pub(super) fn as_columns(&self, name: &str) -> Vec<String> {
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

    pub(super) fn partition(&self) -> Vec<String> {
        self.as_columns("jansu.lake.partition")
    }

    pub(super) fn z_order(&self) -> Vec<String> {
        self.as_columns("jansu.lake.z_order")
    }

    pub(super) fn generated_fields(&self) -> Vec<Arc<Field>> {
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

    pub(super) fn generated(&self) -> Result<Vec<StructField>> {
        self.generated_fields()
            .iter()
            .map(|field| StructField::try_from_arrow(field.as_ref()).map_err(Into::into))
            .collect::<Result<Vec<_>>>()
    }

    /// Returns a list of (column_name, sql_expression) pairs for generated columns
    pub(super) fn generated_expressions(&self) -> Vec<(String, String)> {
        self.0
            .iter()
            .filter_map(|(name, value)| {
                name.strip_prefix("jansu.lake.generate.")
                    .map(|suffix| (suffix.to_string(), value.clone()))
            })
            .collect()
    }

    pub(super) fn is_normalized(&self) -> Result<bool> {
        match self
            .0
            .iter()
            .find_map(|(name, value)| (name == "jansu.lake.normalize").then_some(value))
        {
            Some(value) => value.parse::<bool>().map_err(|_| {
                Error::Message(format!("invalid boolean for jansu.lake.normalize: {value}"))
            }),
            None => Ok(false),
        }
    }

    pub(super) fn normalize_separator(&self) -> &str {
        self.0
            .iter()
            .find(|(name, _)| name == "jansu.lake.normalize.separator")
            .map(|(_, value)| value.as_str())
            .map_or(".", |separator| separator)
    }
}
