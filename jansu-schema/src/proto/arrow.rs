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

use std::collections::BTreeMap;

use crate::{
    AsArrow, Result,
    lake::LakeHouseType,
    proto::{MessageKind, Schema},
};

use arrow::{
    array::ArrayBuilder,
    datatypes::{Fields, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use chrono::{DateTime, Datelike};

use jansu_sans_io::record::inflated::Batch;
use serde_json::json;
use tracing::{debug, instrument};

const GOOGLE_PROTOBUF_TIMESTAMP: &str = "google.protobuf.Timestamp";

const KEY: &str = "Key";
const META: &str = "Meta";
const VALUE: &str = "Value";

const NULLABLE: bool = true;
const SORTED_MAP_KEYS: bool = false;

fn append<'a>(path: &[&'a str], name: &'a str) -> Vec<&'a str> {
    let mut path = Vec::from(path);
    path.push(name);
    path
}

#[derive(Default)]
struct RecordBuilder {
    meta: Option<Box<dyn ArrayBuilder>>,
    key: Option<Box<dyn ArrayBuilder>>,
    value: Option<Box<dyn ArrayBuilder>>,
}

impl RecordBuilder {
    fn new(ids: &BTreeMap<String, i32>, schema: &Schema) -> RecordBuilder {
        Self {
            meta: schema.message_by_package_relative_name_array_builder(ids, MessageKind::Meta),
            key: schema.message_by_package_relative_name_array_builder(ids, MessageKind::Key),
            value: schema.message_by_package_relative_name_array_builder(ids, MessageKind::Value),
        }
    }
}

mod builders;
mod decode;
mod decode_value;
mod field_ids;
mod fields;

use decode_value::process_message_descriptor;
use field_ids::field_ids;

fn fields(ids: &BTreeMap<String, i32>, schema: &Schema) -> Fields {
    let mut fields = vec![];

    if let Some(field) = schema.field(ids, MessageKind::Meta) {
        fields.push(field);
    }

    if let Some(field) = schema.field(ids, MessageKind::Key) {
        fields.push(field);
    }

    if let Some(field) = schema.field(ids, MessageKind::Value) {
        fields.push(field);
    }

    fields.into()
}

fn arrow_schema(ids: &BTreeMap<String, i32>, schema: &Schema) -> ArrowSchema {
    ArrowSchema::new(fields(ids, schema))
}

impl AsArrow for Schema {
    #[instrument(skip(self, batch), ret)]
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: LakeHouseType,
    ) -> Result<RecordBatch> {
        let ids = if lake_type.is_iceberg() {
            field_ids(&self.file_descriptors)
        } else {
            BTreeMap::new()
        };

        let schema = arrow_schema(&ids, self);
        let mut record_builder = RecordBuilder::new(&ids, self);

        for record in batch.records.iter() {
            debug!(?record);

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Meta),
                self.encode_from_value(
                    MessageKind::Meta,
                    &DateTime::from_timestamp_millis(batch.base_timestamp + record.timestamp_delta)
                        .map_or(json!({"partition": partition}), |date_time| {
                            json!({
                            "partition": partition,
                            "timestamp": date_time.to_rfc3339(),
                            "year": date_time.date_naive().year(),
                            "month": date_time.date_naive().month(),
                            "day": date_time.date_naive().day()})
                        }),
                )
                .map(Some)?,
                &mut record_builder.meta.iter_mut(),
            )
            .inspect_err(|err| debug!(?err))?;

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Key),
                record.key(),
                &mut record_builder.key.iter_mut(),
            )
            .inspect_err(|err| debug!(?err))?;

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Value),
                record.value(),
                &mut record_builder.value.iter_mut(),
            )
            .inspect_err(|err| debug!(?err))?;
        }

        debug!(
            meta_rows = ?record_builder.meta.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            key_rows = ?record_builder.key.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            value_rows = ?record_builder.value.iter().map(|rows| rows.len()).collect::<Vec<_>>()
        );

        let mut columns = vec![];

        if let Some(meta) = record_builder.meta {
            columns.push(meta);
        }

        if let Some(key) = record_builder.key {
            columns.push(key);
        }

        if let Some(value) = record_builder.value {
            columns.push(value);
        }

        debug!(columns = columns.len(), ?schema);

        RecordBatch::try_new(
            schema.into(),
            columns.iter_mut().map(|builder| builder.finish()).collect(),
        )
        .inspect_err(|err| debug!(?err))
        .inspect(|record_batch| debug!(?record_batch))
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests;
