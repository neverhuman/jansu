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

//! AVRO Arrow conversion tests (group1)

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
