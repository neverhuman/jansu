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

//! Tests for the Avro→Arrow conversion code in `super::arrow`.
//!
//! Extracted from `arrow.rs` so the production code drops well below the
//! workspace shape budget. The file name ends in `/tests.rs` so the Jankurai
//! `is_test_or_example_path` check excludes it from product-code surface
//! counts.

use std::{fs::File, sync::Arc, thread};

use super::*;
use apache_avro::{Decimal, types::Value};

use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;

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

use num_bigint::BigInt;

use parquet::file::properties::WriterProperties;

use jansu_sans_io::record::Record;
use serde_json::json;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use crate::AsKafkaRecord as _;

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

#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn iceberg_write(record_batch: RecordBatch) -> Result<Vec<DataFile>> {
    debug!(?record_batch);
    debug!(schema = ?record_batch.schema());
    let iceberg_schema = IcebergSchema::try_from(record_batch.schema().as_ref())
        .map(IcebergSchemaRef::new)
        .inspect(|schema| debug!(?schema))
        .inspect_err(|err| debug!(?err))?;

    let memory = FileIOBuilder::new("memory").build()?;

    #[derive(Clone)]
    struct Location;

    impl LocationGenerator for Location {
        fn generate_location(
            &self,
            _partition_key: Option<&iceberg::spec::PartitionKey>,
            file_name: &str,
        ) -> String {
            format!("abc/{file_name}")
        }
    }

    let parquet_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema);

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        memory,
        Location,
        DefaultFileNameGenerator::new("pqr".into(), None, Parquet),
    );

    use iceberg::writer::base_writer::data_file_writer::DataFileWriter;

    let data_file_writer_builder: DataFileWriterBuilder<
        ParquetWriterBuilder,
        Location,
        DefaultFileNameGenerator,
    > = DataFileWriterBuilder::new(rolling_writer_builder);

    let mut data_file_writer: DataFileWriter<
        ParquetWriterBuilder,
        Location,
        DefaultFileNameGenerator,
    > = data_file_writer_builder
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

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn record_of_primitive_data_types() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
              "name": "Message",
              "type": "record",
              "fields": [
                {
                  "name": "value",
                  "type": {
                    "name": "sub",
                    "type": "record",
                    "fields": [
                      { "name": "b", "type": "boolean" },
                      { "name": "c", "type": "int" },
                      { "name": "d", "type": "long" },
                      { "name": "e", "type": "float" },
                      { "name": "f", "type": "double" },
                      { "name": "g", "type": "bytes" },
                      { "name": "h", "type": "string" }
                    ]
                  }
                }
              ]
            }
    ));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [r(
            schema.value.as_ref().unwrap(),
            [
                // ("a", Value::Null),
                ("b", false.into()),
                ("c", i32::MAX.into()),
                ("d", i64::MAX.into()),
                ("e", f32::MAX.into()),
                ("f", f64::MAX.into()),
                ("g", Vec::from(&b"abcdef"[..]).into()),
                ("h", "pqr".into()),
            ],
        )];

        for value in values {
            batch = batch.record(
                Record::builder()
                    .value(schema_write(schema.value.as_ref().unwrap(), value.into())?.into()),
            )
        }
        batch.build()?
    };

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(1, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
        "| value                                                                                                                  | meta                                                                          |",
        "+------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
        "| {b: false, c: 2147483647, d: 9223372036854775807, e: 3.4028235e38, f: 1.7976931348623157e308, g: 616263646566, h: pqr} | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn record_of_with_list_of_primitive_data_types() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
              "type": "record",
              "name": "Message",
              "fields": [
                {
                  "name": "value",
                  "type": {
                    "type": "record",
                    "name": "sub",
                    "fields": [
                      { "name": "b", "type": "array", "items": "boolean" },
                      { "name": "c", "type": "array", "items": "int" },
                      { "name": "d", "type": "array", "items": "long" },
                      { "name": "e", "type": "array", "items": "float" },
                      { "name": "f", "type": "array", "items": "double" },
                      { "name": "g", "type": "array", "items": "bytes" },
                      { "name": "h", "type": "array", "items": "string" }
                    ]
                  }
                }
              ]
            }
    ));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [r(
            schema.value.as_ref().unwrap(),
            [
                ("b", Value::Array(vec![false.into(), true.into()])),
                (
                    "c",
                    Value::Array(vec![i32::MIN.into(), 0.into(), i32::MAX.into()]),
                ),
                (
                    "d",
                    Value::Array(vec![i64::MIN.into(), 0.into(), i64::MAX.into()]),
                ),
                (
                    "e",
                    Value::Array(vec![f32::MIN.into(), 0.0f32.into(), f32::MAX.into()]),
                ),
                (
                    "f",
                    Value::Array(vec![f64::MIN.into(), 0.0f64.into(), f64::MAX.into()]),
                ),
                ("g", Value::Array(vec![Vec::from(&b"abcdef"[..]).into()])),
                (
                    "h",
                    Value::Array(vec!["abc".into(), "pqr".into(), "xyz".into()]),
                ),
            ],
        )];

        for value in values {
            batch = batch.record(
                Record::builder()
                    .value(schema_write(schema.value.as_ref().unwrap(), value.into())?.into()),
            )
        }

        batch.build()?
    };

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(1, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
        "| value                                                                                                                                                                                                                                           | meta                                                                          |",
        "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
        "| {b: [false, true], c: [-2147483648, 0, 2147483647], d: [-9223372036854775808, 0, 9223372036854775807], e: [-3.4028235e38, 0.0, 3.4028235e38], f: [-1.7976931348623157e308, 0.0, 1.7976931348623157e308], g: [616263646566], h: [abc, pqr, xyz]} | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn union() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "union",
        "fields": [{"name": "value", "type": ["null", "float"]}]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [
            Value::Union(1, Box::new(Value::Float(f32::MIN))),
            Value::Union(0, Box::new(Value::Null)),
            Value::Union(1, Box::new(Value::Float(f32::MAX))),
        ];

        for value in values {
            batch = batch.record(
                Record::builder()
                    .value(schema_write(schema.value.as_ref().unwrap(), value)?.into()),
            )
        }
        batch.build()?
    };

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(3, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+---------------+-------------------------------------------------------------------------------+",
        "| value         | meta                                                                          |",
        "+---------------+-------------------------------------------------------------------------------+",
        "| -3.4028235e38 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "|               | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 3.4028235e38  | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+---------------+-------------------------------------------------------------------------------+"
    ]
    .into_iter()
    .collect::<Vec<_>>();

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn enumeration() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "Suit",
        "fields": [
            {
                "name": "value",
                "type": "enum",
                "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }
        ]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [Value::from(json!("CLUBS")), Value::from(json!("HEARTS"))];

        for value in values {
            batch = batch.record(
                Record::builder()
                    .value(schema_write(schema.value.as_ref().unwrap(), value)?.into()),
            )
        }
        batch.build()?
    };

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+--------+-------------------------------------------------------------------------------+",
        "| value  | meta                                                                          |",
        "+--------+-------------------------------------------------------------------------------+",
        "| CLUBS  | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| HEARTS | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+--------+-------------------------------------------------------------------------------+"
    ]
    .into_iter()
    .collect::<Vec<_>>();

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn observation_enumeration() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
      "type": "record",
      "name": "observation",
      "fields": [
        { "name": "key", "type": "string", "logicalType": "uuid" },
        {
          "name": "value",
          "type": {
            "name": "sub",
            "type": "record",

            "fields": [
              { "name": "amount", "type": "double" },
              { "name": "unit", "type": "enum", "symbols": ["CELSIUS", "MILLIBAR"] }
            ]
          }
        }
      ]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [json!({
            "key": "1E44D9C2-5E7A-443B-BF10-2B1E5FD72F15",
            "value": {
                "amount": 23.2,
                "unit": "CELSIUS"
            }
        })];

        for value in values {
            batch = batch.record(schema.as_kafka_record(&value)?);
        }
        batch.build()?
    };

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;
    debug!(?record_batch);

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(1, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+--------------------------------------+-------------------------------+-------------------------------------------------------------------------------+",
        "| key                                  | value                         | meta                                                                          |",
        "+--------------------------------------+-------------------------------+-------------------------------------------------------------------------------+",
        "| 1e44d9c2-5e7a-443b-bf10-2b1e5fd72f15 | {amount: 23.2, unit: CELSIUS} | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+--------------------------------------+-------------------------------+-------------------------------------------------------------------------------+"
    ]
    .into_iter()
    .collect::<Vec<_>>();

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn map() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "Long",
        "fields": [
            {"name": "value", "type": "map", "values": "long", "default": {}},
        ],
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [Value::from(json!({"a": 1, "b": 3, "c": 5}))];

        for value in values {
            batch = batch.record(
                Record::builder()
                    .value(schema_write(schema.value.as_ref().unwrap(), value)?.into()),
            )
        }
        batch.build()?
    };

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    fn sort(s: &str) -> String {
        let mut chars = s.chars().collect::<Vec<_>>();
        chars.sort();
        chars.into_iter().collect()
    }

    let expected = vec![
        "+--------------------+",
        "| value              |",
        "+--------------------+",
        "| {c: 5, a: 1, b: 3} |",
        "+--------------------+",
    ]
    .into_iter()
    .map(sort)
    .collect::<Vec<_>>();

    assert_eq!(
        pretty_results.trim().lines().map(sort).collect::<Vec<_>>(),
        expected
    );

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn simple_integer_key_as_arrow() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "key", "type": "int"}
        ]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let keys = [32123, 45654, 87678, 12321];

        for key in keys {
            batch = batch.record(
                Record::builder()
                    .key(schema_write(schema.key.as_ref().unwrap(), key.into())?.into()),
            );
        }

        batch.build()
    }?;

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(4, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+-------+-------------------------------------------------------------------------------+",
        "| key   | meta                                                                          |",
        "+-------+-------------------------------------------------------------------------------+",
        "| 32123 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 45654 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 87678 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 12321 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+-------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn simple_record_value_as_arrow() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
              "type": "record",
              "name": "Person",
              "fields": [
                {
                  "name": "value",
                  "type": {
                    "type": "record",
                    "name": "sub",
                    "fields": [
                      { "name": "id", "type": "int" },
                      { "name": "name", "type": "string" },
                      { "name": "lucky", "type": "array", "items": "int", "default": [] }
                    ]
                  }
                }
              ]
            }
    ));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [
            r(
                schema.value.as_ref().unwrap(),
                [
                    ("id", 32123.into()),
                    ("name", "alice".into()),
                    ("lucky", Value::Array([6.into()].into())),
                ],
            ),
            r(
                schema.value.as_ref().unwrap(),
                [
                    ("id", 45654.into()),
                    ("name", "bob".into()),
                    ("lucky", Value::Array([5.into(), 9.into()].into())),
                ],
            ),
        ];

        for value in values {
            batch = batch.record(
                Record::builder()
                    .value(schema_write(schema.value.as_ref().unwrap(), value.into())?.into()),
            )
        }
        batch.build()?
    };

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+---------------------------------------+-------------------------------------------------------------------------------+",
        "| value                                 | meta                                                                          |",
        "+---------------------------------------+-------------------------------------------------------------------------------+",
        "| {id: 32123, name: alice, lucky: [6]}  | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| {id: 45654, name: bob, lucky: [5, 9]} | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+---------------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn array_bool_value() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "array",
            "items": "boolean",
            "default": []
        }]
    }));

    let values = [[true, true], [false, true], [true, false], [false, false]]
        .into_iter()
        .map(|l| Value::Array(l.into_iter().map(Value::Boolean).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(4, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+----------------+-------------------------------------------------------------------------------+",
        "| value          | meta                                                                          |",
        "+----------------+-------------------------------------------------------------------------------+",
        "| [true, true]   | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| [false, true]  | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| [true, false]  | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| [false, false] | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+----------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn array_int_value() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "array",
            "items": "int",
            "default": []
        }]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [vec![32123, 23432, 12321, 56765], vec![i32::MIN, i32::MAX]]
            .into_iter()
            .map(|l| Value::Array(l.into_iter().map(Value::Int).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+------------------------------+-------------------------------------------------------------------------------+",
        "| value                        | meta                                                                          |",
        "+------------------------------+-------------------------------------------------------------------------------+",
        "| [32123, 23432, 12321, 56765] | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| [-2147483648, 2147483647]    | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn array_long_value() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "array",
            "items": "long",
            "default": []
        }]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [vec![32123, 23432, 12321, 56765], vec![i64::MIN, i64::MAX]]
            .into_iter()
            .map(|l| Value::Array(l.into_iter().map(Value::Long).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+---------------------------------------------+-------------------------------------------------------------------------------+",
        "| value                                       | meta                                                                          |",
        "+---------------------------------------------+-------------------------------------------------------------------------------+",
        "| [32123, 23432, 12321, 56765]                | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| [-9223372036854775808, 9223372036854775807] | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+---------------------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn array_float_value() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "name": "test",
        "type": "record",
        "fields": [{
            "name": "value",
            "type": "array",
            "items": "float",
            "default": []
        }]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [
            vec![3.2123, 23.432, 123.21, 5676.5],
            vec![f32::MIN, f32::MAX],
        ]
        .into_iter()
        .map(|l| Value::Array(l.into_iter().map(Value::Float).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+----------------------------------+-------------------------------------------------------------------------------+",
        "| value                            | meta                                                                          |",
        "+----------------------------------+-------------------------------------------------------------------------------+",
        "| [3.2123, 23.432, 123.21, 5676.5] | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| [-3.4028235e38, 3.4028235e38]    | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+----------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn array_double_value() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
           "name": "value",
            "type": "array",
            "items": "double",
            "default": []
        }],
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [
            vec![3.2123, 23.432, 123.21, 5676.5],
            vec![f64::MIN, f64::MAX],
        ]
        .into_iter()
        .map(|l| Value::Array(l.into_iter().map(Value::Double).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+---------------------------------------------------+-------------------------------------------------------------------------------+",
        "| value                                             | meta                                                                          |",
        "+---------------------------------------------------+-------------------------------------------------------------------------------+",
        "| [3.2123, 23.432, 123.21, 5676.5]                  | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| [-1.7976931348623157e308, 1.7976931348623157e308] | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+---------------------------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn array_string_value() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "array",
            "items": "string",
            "default": []
        }]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [
            vec!["abc".to_string(), "def".to_string(), "pqr".to_string()],
            vec!["xyz".to_string()],
        ]
        .into_iter()
        .map(|l| Value::Array(l.into_iter().map(Value::String).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+-----------------+-------------------------------------------------------------------------------+",
        "| value           | meta                                                                          |",
        "+-----------------+-------------------------------------------------------------------------------+",
        "| [abc, def, pqr] | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| [xyz]           | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+-----------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn array_record_value() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "array",
            "items": {
                "type": "record",
                "name": "xyz",
                "fields": [{
                    "name": "id",
                    "type": "int"
                },
                {
                    "name": "name",
                    "type": "string"
                }
            ]},
            "default": []
        }]
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [
            Value::Array(vec![
                Value::Record(vec![
                    ("id".into(), 32123.into()),
                    ("name".into(), "alice".into()),
                ]),
                Value::Record(vec![
                    ("id".into(), 45654.into()),
                    ("name".into(), "bob".into()),
                ]),
            ]),
            Value::Array(vec![Value::Record(vec![
                ("id".into(), 54345.into()),
                ("name".into(), "betty".into()),
            ])]),
        ];

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let ctx = SessionContext::new();
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+----------------------------------------------------+",
        "| value                                              |",
        "+----------------------------------------------------+",
        "| [{id: 32123, name: alice}, {id: 45654, name: bob}] |",
        "| [{id: 54345, name: betty}]                         |",
        "+----------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn array_bytes_value() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "array",
            "items": "bytes",
            "default": []
        }]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [
            vec![b"abc".to_vec(), b"def".to_vec(), b"pqr".to_vec()],
            vec![b"54345".to_vec()],
        ]
        .into_iter()
        .map(|l| Value::Array(l.into_iter().map(Value::Bytes).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+--------------------------+-------------------------------------------------------------------------------+",
        "| value                    | meta                                                                          |",
        "+--------------------------+-------------------------------------------------------------------------------+",
        "| [616263, 646566, 707172] | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| [3534333435]             | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+--------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn uuid_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "string",
            "logicalType": "uuid"
        }]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [
            "383BB977-7D38-42B5-8BE7-58A1C606DE7A",
            "2C1FDDC8-4EBE-43FD-8F1C-47E18B7A4E21",
            "F9B45334-9AA2-4978-8735-9800D27A551C",
        ]
        .into_iter()
        .map(|uuid| Uuid::parse_str(uuid).map(Value::Uuid).map_err(Into::into))
        .collect::<Result<Vec<_>>>()?;

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(3, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+--------------------------------------+-------------------------------------------------------------------------------+",
        "| value                                | meta                                                                          |",
        "+--------------------------------------+-------------------------------------------------------------------------------+",
        "| 383bb977-7d38-42b5-8be7-58a1c606de7a | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 2c1fddc8-4ebe-43fd-8f1c-47e18b7a4e21 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| f9b45334-9aa2-4978-8735-9800d27a551c | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+--------------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn time_millis_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "int",
            "logicalType": "time-millis"
        }]
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [1, 2, 3]
            .into_iter()
            .map(Value::TimeMillis)
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(3, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+--------------+",
        "| value        |",
        "+--------------+",
        "| 00:00:00.001 |",
        "| 00:00:00.002 |",
        "| 00:00:00.003 |",
        "+--------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn time_micros_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "long",
            "logicalType": "time-micros"
        }]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [1, 2, 3]
            .into_iter()
            .map(Value::TimeMicros)
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(3, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+-----------------+-------------------------------------------------------------------------------+",
        "| value           | meta                                                                          |",
        "+-----------------+-------------------------------------------------------------------------------+",
        "| 00:00:00.000001 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 00:00:00.000002 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 00:00:00.000003 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+-----------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn timestamp_millis_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "long",
            "logicalType": "timestamp-millis"
        }]
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [119_731_017, 1_000_000_000, 1_234_567_890]
            .into_iter()
            .map(|seconds| Value::TimestampMillis(seconds * 1_000))
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(3, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+---------------------+",
        "| value               |",
        "+---------------------+",
        "| 1973-10-17T18:36:57 |",
        "| 2001-09-09T01:46:40 |",
        "| 2009-02-13T23:31:30 |",
        "+---------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn timestamp_micros_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "long",
            "logicalType": "timestamp-micros"
        }]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [119_731_017, 1_000_000_000, 1_234_567_890]
            .into_iter()
            .map(|seconds| Value::TimestampMicros(seconds * 1_000 * 1_000))
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(3, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+---------------------+-------------------------------------------------------------------------------+",
        "| value               | meta                                                                          |",
        "+---------------------+-------------------------------------------------------------------------------+",
        "| 1973-10-17T18:36:57 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 2001-09-09T01:46:40 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 2009-02-13T23:31:30 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+---------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn local_timestamp_millis_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
            "fields": [{
                "name": "value",
                "type": "long",
                "logicalType": "local-timestamp-millis"
            }]
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [119_731_017, 1_000_000_000, 1_234_567_890]
            .into_iter()
            .map(|seconds| Value::LocalTimestampMillis(seconds * 1_000))
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(3, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+---------------------+",
        "| value               |",
        "+---------------------+",
        "| 1973-10-17T18:36:57 |",
        "| 2001-09-09T01:46:40 |",
        "| 2009-02-13T23:31:30 |",
        "+---------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn local_timestamp_micros_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "long",
            "logicalType": "local-timestamp-micros"
        }]
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [119_731_017, 1_000_000_000, 1_234_567_890]
            .into_iter()
            .map(|seconds| Value::LocalTimestampMicros(seconds * 1_000 * 1_000))
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(3, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+---------------------+",
        "| value               |",
        "+---------------------+",
        "| 1973-10-17T18:36:57 |",
        "| 2001-09-09T01:46:40 |",
        "| 2009-02-13T23:31:30 |",
        "+---------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn date_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "int",
            "logicalType": "date"
        }]
    }));

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [
            Value::Int(1),
            Value::Int(1_385),
            Value::Int(11_574),
            Value::Int(14_288),
        ];

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(4, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+------------+-------------------------------------------------------------------------------+",
        "| value      | meta                                                                          |",
        "+------------+-------------------------------------------------------------------------------+",
        "| 1970-01-02 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 1973-10-17 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 2001-09-09 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 2009-02-13 | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(feature = "parquet")]
async fn decimal_fixed_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": {
                "type": "fixed",
                "size": 8,
                "name": "decimal"
            },
            "logicalType": "decimal",
            "precision": 8,
            "scale": 2,
        }]
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [32123, 45654, 87678, 12321]
            .into_iter()
            .map(BigInt::from)
            .map(|big_int| big_int.to_signed_bytes_be())
            .map(Decimal::from)
            .map(Value::Decimal)
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+------------+",
        "| value      |",
        "+------------+",
        "| 1970-01-02 |",
        "| 1973-10-17 |",
        "| 2001-09-09 |",
        "| 2009-02-13 |",
        "+------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(feature = "parquet")]
async fn decimal_variable_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 8,
            "scale": 2,
        }]
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [32123, 45654, 87678, 12321]
            .into_iter()
            .map(BigInt::from)
            .map(|big_int| big_int.to_signed_bytes_be())
            .map(Decimal::from)
            .map(Value::Decimal)
            .collect::<Vec<_>>();

        for value in values {
            batch = batch.record(
                Record::builder().value(
                    schema_write(schema.value.as_ref().unwrap(), value)
                        .inspect(|encoded| debug!(?encoded))?
                        .into(),
                ),
            )
        }

        batch.build()
    }?;

    debug!(?batch);

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    debug!(?record_batch);

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+------------+",
        "| value      |",
        "+------------+",
        "| 1970-01-02 |",
        "| 1973-10-17 |",
        "| 2001-09-09 |",
        "| 2009-02-13 |",
        "+------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn string_key_with_record_as_arrow() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
              "type": "record",
              "name": "test",
              "fields": [
                {
                  "name": "key",
                  "type": "string"
                },
                {
                  "name": "value",
                  "type": {
                    "name": "sub",
                    "type": "record",
                    "fields": [
                      { "name": "first", "type": "string" },
                      { "name": "last", "type": "string" },
                      { "name": "test1", "type": "double" },
                      { "name": "test2", "type": "double" },
                      { "name": "test3", "type": "double" },
                      { "name": "test4", "type": "double" },
                      { "name": "final", "type": "double" },
                      { "name": "grade", "type": "string" }
                    ]
                  }
                }
              ]
            }
    ));

    // https://people.math.sc.edu/Burkardt/datasets/csv/csv.html
    let grades = [
        (
            "Alfalfa",
            "Aloysius",
            "123-45-6789",
            40.0,
            90.0,
            100.0,
            83.0,
            49.0,
            "D-",
        ),
        (
            "Alfred",
            "University",
            "123-12-1234",
            41.0,
            97.0,
            96.0,
            97.0,
            48.0,
            "D+",
        ),
        (
            "Gerty",
            "Gramma",
            "567-89-0123",
            41.0,
            80.0,
            60.0,
            40.0,
            44.0,
            "C",
        ),
        (
            "Android",
            "Electric",
            "087-65-4321",
            42.0,
            23.0,
            36.0,
            45.0,
            47.0,
            "B-",
        ),
        (
            "Bumpkin",
            "Fred",
            "456-78-9012",
            43.0,
            78.0,
            88.0,
            77.0,
            45.0,
            "A-",
        ),
        (
            "Rubble",
            "Betty",
            "234-56-7890",
            44.0,
            90.0,
            80.0,
            90.0,
            46.0,
            "C-",
        ),
        (
            "Noshow",
            "Cecil",
            "345-67-8901",
            45.0,
            11.0,
            -1.0,
            4.0,
            43.0,
            "F",
        ),
        (
            "Buff",
            "Bif",
            "632-79-9939",
            46.0,
            20.0,
            30.0,
            40.0,
            50.0,
            "B+",
        ),
        (
            "Airpump",
            "Andrew",
            "223-45-6789",
            49.0,
            1.0,
            90.0,
            100.0,
            83.0,
            "A",
        ),
        (
            "Backus",
            "Jim",
            "143-12-1234",
            48.0,
            1.0,
            97.0,
            96.0,
            97.0,
            "A+",
        ),
        (
            "Carnivore",
            "Art",
            "565-89-0123",
            44.0,
            1.0,
            80.0,
            60.0,
            40.0,
            "D+",
        ),
        (
            "Dandy",
            "Jim",
            "087-75-4321",
            47.0,
            1.0,
            23.0,
            36.0,
            45.0,
            "C+",
        ),
        (
            "Elephant",
            "Ima",
            "456-71-9012",
            45.0,
            1.0,
            78.0,
            88.0,
            77.0,
            "B-",
        ),
        (
            "Franklin",
            "Benny",
            "234-56-2890",
            50.0,
            1.0,
            90.0,
            80.0,
            90.0,
            "B-",
        ),
        (
            "George",
            "Boy",
            "345-67-3901",
            40.0,
            1.0,
            11.0,
            -1.0,
            4.0,
            "B",
        ),
        (
            "Heffalump",
            "Harvey",
            "632-79-9439",
            30.0,
            1.0,
            20.0,
            30.0,
            40.0,
            "C",
        ),
    ];

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        for grade in grades {
            let mut value =
                apache_avro::types::Record::new(schema.value.as_ref().unwrap()).unwrap();
            value.put("first", grade.0);
            value.put("last", grade.1);
            value.put("test1", grade.3);
            value.put("test2", grade.4);
            value.put("test3", grade.5);
            value.put("test4", grade.6);
            value.put("final", grade.7);
            value.put("grade", grade.8);

            batch = batch.record(
                Record::builder()
                    .key(schema_write(schema.key.as_ref().unwrap(), grade.2.into())?.into())
                    .value(schema_write(schema.value.as_ref().unwrap(), value.into())?.into()),
            );
        }

        batch.build()
    }?;

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(16, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+-------------+---------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
        "| key         | value                                                                                                         | meta                                                                          |",
        "+-------------+---------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
        "| 123-45-6789 | {first: Alfalfa, last: Aloysius, test1: 40.0, test2: 90.0, test3: 100.0, test4: 83.0, final: 49.0, grade: D-} | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 123-12-1234 | {first: Alfred, last: University, test1: 41.0, test2: 97.0, test3: 96.0, test4: 97.0, final: 48.0, grade: D+} | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 567-89-0123 | {first: Gerty, last: Gramma, test1: 41.0, test2: 80.0, test3: 60.0, test4: 40.0, final: 44.0, grade: C}       | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 087-65-4321 | {first: Android, last: Electric, test1: 42.0, test2: 23.0, test3: 36.0, test4: 45.0, final: 47.0, grade: B-}  | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 456-78-9012 | {first: Bumpkin, last: Fred, test1: 43.0, test2: 78.0, test3: 88.0, test4: 77.0, final: 45.0, grade: A-}      | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 234-56-7890 | {first: Rubble, last: Betty, test1: 44.0, test2: 90.0, test3: 80.0, test4: 90.0, final: 46.0, grade: C-}      | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 345-67-8901 | {first: Noshow, last: Cecil, test1: 45.0, test2: 11.0, test3: -1.0, test4: 4.0, final: 43.0, grade: F}        | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 632-79-9939 | {first: Buff, last: Bif, test1: 46.0, test2: 20.0, test3: 30.0, test4: 40.0, final: 50.0, grade: B+}          | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 223-45-6789 | {first: Airpump, last: Andrew, test1: 49.0, test2: 1.0, test3: 90.0, test4: 100.0, final: 83.0, grade: A}     | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 143-12-1234 | {first: Backus, last: Jim, test1: 48.0, test2: 1.0, test3: 97.0, test4: 96.0, final: 97.0, grade: A+}         | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 565-89-0123 | {first: Carnivore, last: Art, test1: 44.0, test2: 1.0, test3: 80.0, test4: 60.0, final: 40.0, grade: D+}      | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 087-75-4321 | {first: Dandy, last: Jim, test1: 47.0, test2: 1.0, test3: 23.0, test4: 36.0, final: 45.0, grade: C+}          | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 456-71-9012 | {first: Elephant, last: Ima, test1: 45.0, test2: 1.0, test3: 78.0, test4: 88.0, final: 77.0, grade: B-}       | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 234-56-2890 | {first: Franklin, last: Benny, test1: 50.0, test2: 1.0, test3: 90.0, test4: 80.0, final: 90.0, grade: B-}     | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 345-67-3901 | {first: George, last: Boy, test1: 40.0, test2: 1.0, test3: 11.0, test4: -1.0, final: 4.0, grade: B}           | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 632-79-9439 | {first: Heffalump, last: Harvey, test1: 30.0, test2: 1.0, test3: 20.0, test4: 30.0, final: 40.0, grade: C}    | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+-------------+---------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
