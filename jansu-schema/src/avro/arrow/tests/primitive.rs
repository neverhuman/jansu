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

use super::*;

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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
