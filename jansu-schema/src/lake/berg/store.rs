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

//! Iceberg lake house batch store operation

use iceberg::{
    spec::{DataFileFormat, Schema},
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use jansu_sans_io::record::inflated::Batch;
use parquet::file::properties::WriterProperties;
use tracing::{debug, error};
use uuid::Uuid;

use crate::{AsArrow as _, Result, lake::LakeHouseType};

use super::Iceberg;

impl Iceberg {
    pub(super) async fn store_batch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        inflated: &Batch,
    ) -> Result<()> {
        let record_batch = self
            .schema_registry
            .as_arrow(topic, partition, inflated, LakeHouseType::Iceberg)
            .await?;

        debug!(?record_batch);

        debug!(schema = ?record_batch.schema());

        let schema = Schema::try_from(record_batch.schema().as_ref())
            .inspect(|schema| {
                for field in schema.as_struct().fields() {
                    debug!(?field);
                }
            })
            .inspect_err(|err| debug!(?err))?;

        let table = self
            .load_or_create_table(topic, schema.clone())
            .await
            .inspect(|table| {
                for field in table.metadata().current_schema().as_struct().fields() {
                    debug!(?field);
                }
            })
            .inspect_err(|err| debug!(?err))?;

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata().clone())?,
            DefaultFileNameGenerator::new(
                topic.to_owned(),
                Some(format!("{partition:0>10}-{offset:0>20}")),
                DataFileFormat::Parquet,
            ),
        );

        let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(None)
            .await
            .inspect_err(|err| error!(?err))?;

        data_file_writer
            .write(record_batch)
            .await
            .inspect_err(|err| debug!(?err))?;

        let data_files = data_file_writer
            .close()
            .await
            .inspect(|data_files| debug!(?data_files))
            .inspect_err(|err| debug!(?err))?;

        let commit_uuid = Uuid::now_v7();
        debug!(%commit_uuid);

        let tx = Transaction::new(&table);

        let tx = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files(data_files)
            .apply(tx)
            .inspect_err(|err| debug!(?err))?;

        tx.commit(self.catalog.as_ref())
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
            .and(Ok(()))
    }
}
