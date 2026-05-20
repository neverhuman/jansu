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

//! Protobuf Arrow conversion tests (continued)

use super::*;

#[tokio::test]
#[cfg(feature = "iceberg")]
async fn key_and_value_as_arrow() -> Result<()> {
    let _guard = init_tracing()?;

    let proto = Bytes::from_static(
        br#"
        syntax = 'proto3';

        message Key {
            int32 id = 1;
        }

        message Value {
            string name = 1;
            string email = 2;
        }
        "#,
    );

    let kv = [
        (
            json!({"id": 12321}),
            json!({
                "name": "alice",
                "email": "alice@example.com"
            }),
        ),
        (
            json!({"id": 32123}),
            json!({
                "name": "bob",
                "email": "bob@example.com"
            }),
        ),
    ];

    let schema = Schema::try_from(proto)?;

    let batch = {
        let mut batch = Batch::builder().base_timestamp(119_731_017_000);

        for (delta, (key, value)) in kv.iter().enumerate() {
            batch = batch.record(
                Record::builder()
                    .key(schema.encode_from_value(MessageKind::Key, key)?.into())
                    .value(schema.encode_from_value(MessageKind::Value, value)?.into())
                    .timestamp_delta(delta as i64)
                    .offset_delta(delta as i32),
            );
        }

        batch.build()?
    };

    let topic = "abc";
    let partition = 0;

    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Iceberg)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(2, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from abc").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+------------------------------------------------------------------------------------+-------------+-----------------------------------------+",
        "| meta                                                                               | key         | value                                   |",
        "+------------------------------------------------------------------------------------+-------------+-----------------------------------------+",
        "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17}     | {id: 12321} | {name: alice, email: alice@example.com} |",
        "| {partition: 0, timestamp: 1973-10-17T18:36:57.001, year: 1973, month: 10, day: 17} | {id: 32123} | {name: bob, email: bob@example.com}     |",
        "+------------------------------------------------------------------------------------+-------------+-----------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "iceberg")]
async fn taxi() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../etc/schema/taxi.proto"
    ))))?;

    let value = schema.encode_from_value(
        MessageKind::Value,
        &json!({
          "vendor_id": 1,
          "trip_id": 1000371,
          "trip_distance": 1.8,
          "fare_amount": 15.32,
          "store_and_fwd": "N"
        }),
    )?;

    let batch = Batch::builder()
        .record(Record::builder().value(value.into()))
        .base_timestamp(119_731_017_000)
        .build()?;

    let topic = "taxi";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Iceberg)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(1, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from taxi").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
        "| meta                                                                           | value                                                                                      |",
        "+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
        "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} |",
        "+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
