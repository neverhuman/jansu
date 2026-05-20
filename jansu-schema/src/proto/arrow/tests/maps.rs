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

//! Protobuf Arrow map and repeated tests

use super::*;

#[ignore]
#[tokio::test]
#[cfg(feature = "parquet")]
async fn simple_map() -> Result<()> {
    let _guard = init_tracing()?;

    let proto = Bytes::from_static(
        br#"
        syntax = 'proto3';

        message Value {
            map<string, int32> kv = 1;
        }
        "#,
    );

    let schema = Schema::try_from(proto)?;

    let value = schema.encode_from_value(
        MessageKind::Value,
        &json!({
            "kv": {"a": 31234, "b": 56765, "c": 12321}
        }),
    )?;

    let batch = Batch::builder()
        .record(Record::builder().value(value.into()))
        .build()?;

    let topic = "snippets";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from snippets").await?;
    let results = df.collect().await?;

    let pretty = pretty_format_batches(&results)?.to_string();
    debug!(pretty);

    let kv = pretty.trim().lines().collect::<Vec<_>>()[3];
    debug!(kv);

    assert!(
        kv == "| {a: 31234, b: 56765, c: 12321} |"
            || kv == "| {a: 31234, c: 12321, b: 56765} |"
            || kv == "| {b: 56765, c: 12321, a: 31234} |"
            || kv == "| {b: 56765, a: 31234, c: 12321} |"
            || kv == "| {c: 12321, a: 31234, b: 56765} |"
            || kv == "| {c: 12321, b: 56765, a: 31234} |"
    );

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(feature = "parquet")]
async fn map_other_type() -> Result<()> {
    let _guard = init_tracing()?;

    let proto = Bytes::from_static(
        br#"
        syntax = 'proto3';

        message Project {
            string name = 1;
            float complete = 2;
        }

        message Value {
            map<string, Project> kv = 1;
        }
        "#,
    );

    let schema = Schema::try_from(proto)?;

    let value = schema.encode_from_value(
        MessageKind::Value,
        &json!({
            "kv": {"a": {"name": "xyz", "complete": 0.99}}
        }),
    )?;

    let batch = Batch::builder()
        .record(Record::builder().value(value.into()))
        .base_timestamp(119_731_017_000)
        .build()?;

    let topic = "snippets";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from snippets").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+----------------------------------+",
        "| kv                               |",
        "+----------------------------------+",
        "| {a: {name: xyz, complete: 0.99}} |",
        "+----------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "iceberg")]
async fn value_message_ref() -> Result<()> {
    let _guard = init_tracing()?;

    let proto = Bytes::from_static(
        br#"
            syntax = 'proto3';

            message Project {
                string name = 1;
                float complete = 2;
            }

            message Value {
                Project project = 1;
                string title = 2;
            }
            "#,
    );

    let schema = Schema::try_from(proto)?;

    let value = schema.encode_from_value(
        MessageKind::Value,
        &json!({
            "project": {"name": "xyz", "complete": 0.99},
            "title": "abc",
        }),
    )?;

    let batch = Batch::builder()
        .base_timestamp(119_731_017_000)
        .record(Record::builder().value(value.into()))
        .build()?;

    let topic = "snippets";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Iceberg)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(1, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from snippets").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+--------------------------------------------------------------------------------+----------------------------------------------------+",
        "| meta                                                                           | value                                              |",
        "+--------------------------------------------------------------------------------+----------------------------------------------------+",
        "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {project: {name: xyz, complete: 0.99}, title: abc} |",
        "+--------------------------------------------------------------------------------+----------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "iceberg")]
async fn simple_repeated() -> Result<()> {
    let _guard = init_tracing()?;

    let proto = Bytes::from_static(
        br#"
        syntax = 'proto3';

        message Value {
          string url = 1;
          string title = 2;
          repeated string snippets = 3;
        }
        "#,
    );

    let schema = Schema::try_from(proto)?;

    let value = schema.encode_from_value(
        MessageKind::Value,
        &json!({
            "url": "https://example.com/a", "title": "a", "snippets": ["p", "q", "r"]
        }),
    )?;

    let batch = Batch::builder()
        .base_timestamp(119_731_017_000)
        .record(Record::builder().value(value.into()))
        .build()?;

    let topic = "snippets";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Iceberg)
        .await?;
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(1, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from snippets").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+--------------------------------------------------------------------------------+-------------------------------------------------------------+",
        "| meta                                                                           | value                                                       |",
        "+--------------------------------------------------------------------------------+-------------------------------------------------------------+",
        "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {url: https://example.com/a, title: a, snippets: [p, q, r]} |",
        "+--------------------------------------------------------------------------------+-------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "iceberg")]
async fn repeated() -> Result<()> {
    let _guard = init_tracing()?;

    let proto = Bytes::from_static(
        br#"
            syntax = 'proto3';

            message Value {
              repeated Result results = 1;
            }

            message Result {
              string url = 1;
              string title = 2;
              repeated string snippets = 3;
            }
            "#,
    );

    let schema = Schema::try_from(proto)?;

    let value = schema.encode_from_value(
        MessageKind::Value,
        &json!({
            "results": [{"url": "https://example.com/abc", "title": "a", "snippets": ["p", "q", "r"]},
                        {"url": "https://example.com/def", "title": "b", "snippets": ["x", "y", "z"]}]
        }),
    )?;

    let batch = Batch::builder()
        .record(Record::builder().value(value.into()))
        .base_timestamp(119_731_017_000)
        .build()?;

    let topic = "snippets";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Iceberg)
        .await?;
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(1, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from snippets").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+--------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+",
        "| meta                                                                           | value                                                                                                                                     |",
        "+--------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+",
        "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {results: [{url: https://example.com/abc, title: a, snippets: [p, q, r]}, {url: https://example.com/def, title: b, snippets: [x, y, z]}]} |",
        "+--------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
