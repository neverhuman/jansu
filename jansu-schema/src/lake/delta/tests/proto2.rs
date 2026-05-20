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

//! Delta Lake protobuf schema tests (proto2)

use super::*;
use crate::proto::{MessageKind, Schema};

#[tokio::test]
async fn taxi_normalized() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "taxi";

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
        .configs(Some(vec![
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.normalize"))
                .value(Some(String::from("true")))
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
        "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
        "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.vendor_id | value.trip_id | value.trip_distance | value.fare_amount | value.store_and_fwd |",
        "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
        "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | 1               | 1000371       | 1.8                 | 15.32             | 0                   |",
        "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
async fn taxi_normalized_with_separator() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "taxi";

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
        .configs(Some(vec![
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.normalize"))
                .value(Some(String::from("true")))
                .read_only(true)
                .is_default(None)
                .config_source(None)
                .is_sensitive(false)
                .synonyms(None)
                .config_type(None)
                .documentation(None),
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.normalize.separator"))
                .value(Some(String::from("_")))
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
        "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
        "| meta_partition | meta_timestamp      | meta_year | meta_month | meta_day | value_vendor_id | value_trip_id | value_trip_distance | value_fare_amount | value_store_and_fwd |",
        "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
        "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | 1               | 1000371       | 1.8                 | 15.32             | 0                   |",
        "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
