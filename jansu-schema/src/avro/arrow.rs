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

use apache_avro::schema::{Schema as AvroSchema, UnionSchema};
use arrow::{
    datatypes::{Fields, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use chrono::{DateTime, Datelike};
use jansu_sans_io::record::inflated::Batch;
use tracing::{debug, instrument};

use crate::{
    AsArrow, Error, Result,
    avro::{Schema, r, schema_write},
    lake::LakeHouseType,
};

use apache_avro::types::Value;

const NULLABLE: bool = true;
const SORTED_MAP_KEYS: bool = false;

pub(super) trait NullableVariant {
    fn nullable_variant(&self) -> Option<&AvroSchema>;
}

impl NullableVariant for UnionSchema {
    fn nullable_variant(&self) -> Option<&AvroSchema> {
        if self.variants().len() == 2
            && self
                .variants()
                .iter()
                .inspect(|variant| debug!(?variant))
                .any(|schema| matches!(schema, AvroSchema::Null))
        {
            self.variants()
                .iter()
                .find(|schema| !matches!(schema, AvroSchema::Null))
                .inspect(|schema| debug!(?schema))
        } else {
            None
        }
    }
}

fn append<'a>(path: &[&'a str], name: &'a str) -> Vec<&'a str> {
    let mut path = Vec::from(path);
    path.push(name);
    path
}

mod builder_kind;
mod list;
mod record;
mod schema;
mod value;

use record::{RecordBuilder, process};

impl AsArrow for Schema {
    #[instrument(skip(self, batch), ret)]
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: LakeHouseType,
    ) -> Result<RecordBatch> {
        debug!(ids = ?self.ids);

        let schema = ArrowSchema::try_from(self)?;
        debug!(?schema);

        let mut record_builder = RecordBuilder::try_from(self)?;

        for record in &batch.records {
            debug!(?record);

            let mut builders = record_builder.0.iter_mut();

            process(self.key.as_ref(), record.key.clone(), &mut builders)?;

            process(self.value.as_ref(), record.value.clone(), &mut builders)?;

            process(
                self.meta.as_ref(),
                self.meta
                    .as_ref()
                    .map(|schema| {
                        schema_write(
                            schema,
                            r(
                                schema,
                                DateTime::from_timestamp_millis(
                                    batch.base_timestamp + record.timestamp_delta,
                                )
                                .map_or(
                                    [
                                        ("partition", Value::Int(partition)),
                                        (
                                            "timestamp",
                                            Value::Long(
                                                (batch.base_timestamp + record.timestamp_delta)
                                                    * 1_000,
                                            ),
                                        ),
                                        ("year", Value::Int(0)),
                                        ("month", Value::Int(0)),
                                        ("day", Value::Int(0)),
                                    ],
                                    |date_time| {
                                        [
                                            ("partition", Value::Int(partition)),
                                            (
                                                "timestamp",
                                                Value::Long(
                                                    (batch.base_timestamp + record.timestamp_delta)
                                                        * 1_000,
                                                ),
                                            ),
                                            ("year", Value::Int(date_time.date_naive().year())),
                                            (
                                                "month",
                                                Value::Int(date_time.date_naive().month() as i32),
                                            ),
                                            (
                                                "day",
                                                Value::Int(date_time.date_naive().day() as i32),
                                            ),
                                        ]
                                    },
                                ),
                            )
                            .into(),
                        )
                    })
                    .transpose()?,
                &mut builders,
            )?;
        }

        debug!(
            rows = ?record_builder.0.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
        );

        RecordBatch::try_new(
            schema.into(),
            record_builder
                .0
                .iter_mut()
                .map(|builder| builder.finish())
                .collect(),
        )
        .map_err(Into::into)
    }
}

impl TryFrom<&Schema> for Fields {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        schema
            .complete
            .as_ref()
            .map_or(Ok(vec![]), |complete| {
                complete
                    .fields
                    .iter()
                    .inspect(|field| debug!(?field))
                    .map(|field| {
                        schema
                            .schema_data_type(&[&field.name], &field.schema)
                            .map(|data_type| schema.new_field(&[], &field.name, data_type))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .map(Into::into)
    }
}

impl TryFrom<&Schema> for ArrowSchema {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        Fields::try_from(schema)
            .inspect(|fields| debug!(?fields))
            .map(ArrowSchema::new)
    }
}

#[cfg(test)]
mod tests;
