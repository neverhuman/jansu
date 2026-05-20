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

//! JSON Arrow key/value and grade tests

use super::*;

#[tokio::test]
#[cfg(feature = "iceberg")]
async fn key_and_value_as_arrow() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let schema = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number"
            },
            "value": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                    },
                    "email": {
                        "type": "string",
                        "format": "email"
                    }
                }
            }
        }
    }))
    .map_err(Into::into)
    .map(Bytes::from)
    .and_then(Schema::try_from)?;

    let kv = [
        (
            json!(12321),
            json!({"name": "alice", "email": "alice@example.com"}),
        ),
        (
            json!(32123),
            json!({"name": "bob", "email": "bob@example.com"}),
        ),
    ];

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        for (ref key, ref value) in kv {
            batch = batch.record(
                Record::builder()
                    .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                    .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
            );
        }

        batch.build()?
    };

    let record_batch = schema
        .as_arrow(topic, 0, &batch, LakeHouseType::Parquet)
        .await?;
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from def").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+-------------------------------------------------------------------------------------+-------+-----------------------------------------+",
        "| meta                                                                                | key   | value                                   |",
        "+-------------------------------------------------------------------------------------+-------+-----------------------------------------+",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 | {email: alice@example.com, name: alice} |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 32123 | {email: bob@example.com, name: bob}     |",
        "+-------------------------------------------------------------------------------------+-------+-----------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "iceberg")]
async fn grade() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../etc/schema/grade.json"
    ))))?;

    let kv = if let Value::Array(values) = serde_json::from_slice::<Value>(include_bytes!(
        concat!(env!("CARGO_MANIFEST_DIR"), "/../etc/data/grades.json")
    ))? {
        values
            .into_iter()
            .map(|value| {
                (
                    value.get("key").cloned().unwrap(),
                    value.get("value").cloned().unwrap(),
                )
            })
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        for (ref key, ref value) in kv {
            debug!(?key, ?value);

            batch = batch.record(
                Record::builder()
                    .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                    .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
            );
        }

        batch.build()?
    };

    let record_batch = schema
        .as_arrow(topic, 0, &batch, LakeHouseType::Parquet)
        .await?;
    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(16, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from def").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+-------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
        "| meta                                                                                | key         | value                                                                                                         |",
        "+-------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 123-45-6789 | {final: 49.0, first: Aloysius, grade: D-, last: Alfalfa, test1: 40.0, test2: 90.0, test3: 100.0, test4: 83.0} |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 123-12-1234 | {final: 48.0, first: University, grade: D+, last: Alfred, test1: 41.0, test2: 97.0, test3: 96.0, test4: 97.0} |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 567-89-0123 | {final: 44.0, first: Gramma, grade: C, last: Gerty, test1: 41.0, test2: 80.0, test3: 60.0, test4: 40.0}       |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 087-65-4321 | {final: 47.0, first: Electric, grade: B-, last: Android, test1: 42.0, test2: 23.0, test3: 36.0, test4: 45.0}  |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 456-78-9012 | {final: 45.0, first: Fred, grade: A-, last: Bumpkin, test1: 43.0, test2: 78.0, test3: 88.0, test4: 77.0}      |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 234-56-7890 | {final: 46.0, first: Betty, grade: C-, last: Rubble, test1: 44.0, test2: 90.0, test3: 80.0, test4: 90.0}      |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 345-67-8901 | {final: 43.0, first: Cecil, grade: F, last: Noshow, test1: 45.0, test2: 11.0, test3: -1.0, test4: 4.0}        |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 632-79-9939 | {final: 50.0, first: Bif, grade: B+, last: Buff, test1: 46.0, test2: 20.0, test3: 30.0, test4: 40.0}          |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 223-45-6789 | {final: 83.0, first: Andrew, grade: A, last: Airpump, test1: 49.0, test2: 1.0, test3: 90.0, test4: 100.0}     |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 143-12-1234 | {final: 97.0, first: Jim, grade: A+, last: Backus, test1: 48.0, test2: 1.0, test3: 97.0, test4: 96.0}         |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 565-89-0123 | {final: 40.0, first: Art, grade: D+, last: Carnivore, test1: 44.0, test2: 1.0, test3: 80.0, test4: 60.0}      |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 087-75-4321 | {final: 45.0, first: Jim, grade: C+, last: Dandy, test1: 47.0, test2: 1.0, test3: 23.0, test4: 36.0}          |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 456-71-9012 | {final: 77.0, first: Ima, grade: B-, last: Elephant, test1: 45.0, test2: 1.0, test3: 78.0, test4: 88.0}       |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 234-56-2890 | {final: 90.0, first: Benny, grade: B-, last: Franklin, test1: 50.0, test2: 1.0, test3: 90.0, test4: 80.0}     |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 345-67-3901 | {final: 4.0, first: Boy, grade: B, last: George, test1: 40.0, test2: 1.0, test3: 11.0, test4: -1.0}           |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 632-79-9439 | {final: 40.0, first: Harvey, grade: C, last: Heffalump, test1: 30.0, test2: 1.0, test3: 20.0, test4: 30.0}    |",
        "+-------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
