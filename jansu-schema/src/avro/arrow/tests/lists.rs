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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
