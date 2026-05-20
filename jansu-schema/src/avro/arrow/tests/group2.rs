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

//! AVRO Arrow conversion tests (group2)

use super::*;

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
