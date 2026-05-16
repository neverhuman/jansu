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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
    let df = ctx.table(topic).await?;
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
