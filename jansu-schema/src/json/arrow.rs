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

use std::sync::Arc;

use crate::{
    AsArrow, Result,
    json::{MessageKind, Schema},
    lake::LakeHouseType,
};

use arrow::{
    array::ArrayBuilder,
    datatypes::{DataType, Fields, Schema as ArrowSchema},
    record_batch::RecordBatch,
};

use chrono::{DateTime, Datelike};

use serde_json::{Value, json};

use jansu_sans_io::record::inflated::Batch;
use tracing::{debug, instrument};

const NULLABLE: bool = true;

struct Record {
    meta: Value,
    key: Option<Value>,
    value: Option<Value>,
}

fn sort_dedup(mut input: Vec<DataType>) -> Vec<DataType> {
    input.sort();
    input.dedup();
    input
}

mod builders;
mod schema;

use builders::{append, append_path};

impl AsArrow for Schema {
    #[instrument(skip(self, batch), ret)]
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        _lake_type: LakeHouseType,
    ) -> Result<RecordBatch> {
        let mut builders = vec![];
        let mut fields = vec![];

        {
            let meta = DateTime::from_timestamp_millis(batch.base_timestamp)
                .as_ref()
                .map(|date_time| {
                    json!({
                    "partition": partition,
                    "timestamp": date_time.to_rfc3339(),
                    "year": date_time.date_naive().year(),
                    "month": date_time.date_naive().month(),
                    "day": date_time.date_naive().day()})
                })
                .unwrap_or(json!({"partition": partition}));

            let data_type = self.common_data_type(&[MessageKind::Meta.as_ref()], &[meta][..])?;

            debug!(?data_type);
            builders.push(self.data_type_builder(&[MessageKind::Meta.as_ref()], &data_type)?);
            fields.push(self.new_field(&[], MessageKind::Meta.as_ref(), data_type))
        }

        if let Some(data_type) = batch
            .records
            .iter()
            .map(|record| {
                record.key.clone().map_or(Ok(None), |encoded| {
                    serde_json::from_slice::<Value>(&encoded[..])
                        .map(Some)
                        .map_err(Into::into)
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(|values| values.into_iter().flatten().collect::<Vec<_>>())
            .and_then(|values| {
                if values.is_empty() {
                    Ok(None)
                } else {
                    self.common_data_type(&[MessageKind::Key.as_ref()], values.as_slice())
                        .map(Some)
                }
            })
            .inspect(|data_type| debug!(?data_type))?
        {
            builders.push(self.data_type_builder(&[MessageKind::Key.as_ref()], &data_type)?);
            fields.push(self.new_field(&[], MessageKind::Key.as_ref(), data_type))
        };

        if let Some(data_type) = batch
            .records
            .iter()
            .map(|record| {
                record.value.clone().map_or(Ok(None), |encoded| {
                    serde_json::from_slice::<Value>(&encoded[..])
                        .map(Some)
                        .map_err(Into::into)
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(|values| values.into_iter().flatten().collect::<Vec<_>>())
            .and_then(|values| {
                if values.is_empty() {
                    Ok(None)
                } else {
                    self.common_data_type(&[MessageKind::Value.as_ref()], values.as_slice())
                        .map(Some)
                }
            })
            .inspect(|data_type| debug!(?data_type))?
        {
            builders.push(self.data_type_builder(&[MessageKind::Value.as_ref()], &data_type)?);
            fields.push(self.new_field(&[], MessageKind::Value.as_ref(), data_type))
        };

        for kv in batch
            .records
            .iter()
            .map(|record| {
                record
                    .key
                    .as_ref()
                    .map(|encoded| serde_json::from_slice::<Value>(&encoded[..]))
                    .transpose()
                    .map_err(Into::into)
                    .and_then(|key| {
                        let meta = DateTime::from_timestamp_millis(
                            batch.base_timestamp + record.timestamp_delta,
                        )
                        .as_ref()
                        .map(|date_time| {
                            json!({
                            "partition": partition,
                            "timestamp": date_time.to_rfc3339(),
                            "year": date_time.date_naive().year(),
                            "month": date_time.date_naive().month(),
                            "day": date_time.date_naive().day()})
                        })
                        .unwrap_or(json!({"partition": partition}));

                        record
                            .value
                            .as_ref()
                            .map(|encoded| serde_json::from_slice::<Value>(&encoded[..]))
                            .transpose()
                            .map_err(Into::into)
                            .map(|value| Record { meta, key, value })
                    })
            })
            .collect::<Result<Vec<_>>>()?
        {
            let mut i = fields.iter().zip(builders.iter_mut());

            let (field, builder) = i.next().unwrap();
            debug!(meta = %kv.meta, ?field);
            append(field, kv.meta, builder)?;

            if let Some(key) = kv.key {
                let (field, builder) = i.next().unwrap();
                debug!(%key, ?field);
                append(field, key, builder)?;
            }

            if let Some(value) = kv.value {
                let (field, builder) = i.next().unwrap();
                debug!(%value, ?field);
                append(field, value, builder)?;
            }
        }

        debug!(len = ?builders.iter().map(|builder|builder.len()).collect::<Vec<_>>());

        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(Fields::from(fields))),
            builders
                .iter_mut()
                .map(|builder| builder.finish())
                .collect(),
        )
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests;
