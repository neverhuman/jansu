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

//! Delta Lake protobuf schema tests (proto1)

use super::*;
use crate::proto::{MessageKind, Schema};

#[tokio::test]
async fn message_descriptor_singular_to_field() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "abc";

    let proto = Bytes::from_static(
        br#"
        syntax = 'proto3';

        message Key {
            int32 id = 1;
        }

        message Value {
            double a = 1;
            float b = 2;
            int32 c = 3;
            int64 d = 4;
            uint32 e = 5;
            uint64 f = 6;
            sint32 g = 7;
            sint64 h = 8;
            fixed32 i = 9;
            fixed64 j = 10;
            sfixed32 k = 11;
            sfixed64 l = 12;
            bool m = 13;
            string n = 14;
        }
        "#,
    );

    let object_store = InMemory::new();

    let location = Path::from(format!("{topic}.proto"));
    _ = object_store
        .put(&location, PutPayload::from(proto.clone()))
        .await?;
    let schema_registry = Registry::new(object_store);

    let kv = [(
        json!({"id": 32123}),
        json!({"a": 567.65,
                "b": 45.654,
                "c": -6,
                "d": -66,
                "e": 23432,
                "f": 34543,
                "g": 45654,
                "h": 67876,
                "i": 78987,
                "j": 89098,
                "k": 90109,
                "l": 12321,
                "m": true,
                "n": "Hello World!"}),
    )];

    let partition = 32123;

    let schema = Schema::try_from(proto)?;

    let record_batch = {
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

        batch.build()
    }?;

    schema_registry.validate(topic, &record_batch).await?;

    let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let lake_house = Url::parse(location.as_ref())
        .map_err(Into::into)
        .and_then(|location| {
            Builder::<PhantomData<Url>, PhantomData<Registry>>::default()
                .location(location)
                .database(Some(database.into()))
                .schema_registry(schema_registry)
                .build()
        })?;

    let config = DescribeConfigsResult::default()
        .error_code(ErrorCode::None.into())
        .error_message(None)
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic.into())
        .configs(Some(vec![
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.generate.date"))
                .value(Some(String::from("cast(meta.timestamp as date)")))
                .read_only(true)
                .is_default(None)
                .config_source(None)
                .is_sensitive(false)
                .synonyms(None)
                .config_type(None)
                .documentation(None),
        ]));

    let offset = 543212345;

    lake_house
        .store(topic, partition, offset, &record_batch, config)
        .await
        .inspect(|result| debug!(?result))
        .inspect_err(|err| debug!(?err))?;

    let table = {
        let mut table =
            DeltaTableBuilder::from_url(Url::parse(&format!("{location}/{database}.{topic}"))?)?
                .build()?;
        table.load().await?;
        table
    };

    let ctx = SessionContext::new();

    _ = ctx.register_table("t", Arc::new(table))?;

    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+------------+",
        "| meta                                                                               | key         | value                                                                                                                                           | date       |",
        "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+------------+",
        "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {id: 32123} | {a: 567.65, b: 45.654, c: -6, d: -66, e: 23432, f: 34543, g: 45654, h: 67876, i: 78987, j: 89098, k: 90109, l: 12321, m: true, n: Hello World!} | 1973-10-17 |",
        "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
async fn taxi_plain() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "taxi";

    let schema = Schema::try_from(repo_asset_bytes("etc/schema/taxi.proto")?)?;

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

    let partition = 32123;

    let record_batch = Batch::builder()
        .record(Record::builder().value(value.into()))
        .base_timestamp(119_731_017_000)
        .build()?;

    let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let schema_registry = repo_schema_registry()?;

    schema_registry.validate(topic, &record_batch).await?;

    let lake_house = Url::parse(location.as_ref())
        .map_err(Into::into)
        .and_then(|location| {
            Builder::<PhantomData<Url>, PhantomData<Registry>>::default()
                .location(location)
                .database(Some(database.into()))
                .schema_registry(schema_registry)
                .build()
        })?;

    let config = DescribeConfigsResult::default()
        .error_code(ErrorCode::None.into())
        .error_message(None)
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic.into())
        .configs(Some(vec![]));

    let offset = 543212345;

    lake_house
        .store(topic, partition, offset, &record_batch, config)
        .await
        .inspect(|result| debug!(?result))
        .inspect_err(|err| debug!(?err))?;

    let table = {
        let mut table =
            DeltaTableBuilder::from_url(Url::parse(&format!("{location}/{database}.{topic}"))?)?
                .build()?;
        table.load().await?;
        table
    };

    let ctx = SessionContext::new();

    _ = ctx.register_table("t", Arc::new(table))?;

    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
        "| meta                                                                               | value                                                                                      |",
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
        "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} |",
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
