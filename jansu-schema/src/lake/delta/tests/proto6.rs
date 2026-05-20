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

//! Delta Lake protobuf schema tests (proto6)

use super::*;
use crate::{Generator, proto::Schema};

#[tokio::test]
async fn customer_schema_migration() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "t";
    let partition = 32123;

    let (schema_registry, record_batch_001) = {
        let proto = Bytes::from_static(include_bytes!("../../../../tests/migrate-001.proto"));
        let schema = Schema::try_from(proto.clone())?;

        let object_store = InMemory::new();

        let location = Path::from(format!("{topic}.proto"));
        _ = object_store.put(&location, PutPayload::from(proto)).await?;

        (
            Registry::new(object_store),
            Batch::builder()
                .record(schema.generate()?)
                .base_timestamp(119_731_017_000)
                .build()?,
        )
    };

    schema_registry.validate(topic, &record_batch_001).await?;

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
                .name(String::from("jansu.lake.normalize"))
                .value(Some(String::from("true")))
                .read_only(true),
        ]));

    let offset = 543212345;

    lake_house
        .store(topic, partition, offset, &record_batch_001, config.clone())
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
        "+----------------+---------------------+-----------+------------+----------+---------------------+",
        "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.email_address |",
        "+----------------+---------------------+-----------+------------+----------+---------------------+",
        "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               |",
        "+----------------+---------------------+-----------+------------+----------+---------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    let (schema_registry, record_batch_002) = {
        let proto = Bytes::from_static(include_bytes!("../../../../tests/migrate-002.proto"));
        let schema = Schema::try_from(proto.clone())?;

        let object_store = InMemory::new();

        let location = Path::from(format!("{topic}.proto"));
        _ = object_store.put(&location, PutPayload::from(proto)).await?;

        (
            Registry::new(object_store),
            Batch::builder()
                .record(schema.generate()?)
                .base_timestamp(119_731_017_000)
                .build()?,
        )
    };

    schema_registry.validate(topic, &record_batch_002).await?;

    let lake_house = Url::parse(location.as_ref())
        .map_err(Into::into)
        .and_then(|location| {
            Builder::<PhantomData<Url>, PhantomData<Registry>>::default()
                .location(location)
                .database(Some(database.into()))
                .schema_registry(schema_registry)
                .build()
        })?;

    let offset = 654323456;

    lake_house
        .store(topic, partition, offset, &record_batch_002, config.clone())
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
        "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+",
        "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.email_address | value.full_name |",
        "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+",
        "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               | ipsum           |",
        "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               |                 |",
        "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    let (schema_registry, record_batch_003) = {
        let proto = Bytes::from_static(include_bytes!("../../../../tests/migrate-003.proto"));
        let schema = Schema::try_from(proto.clone())?;

        let object_store = InMemory::new();

        let location = Path::from(format!("{topic}.proto"));
        _ = object_store.put(&location, PutPayload::from(proto)).await?;

        (
            Registry::new(object_store),
            Batch::builder()
                .record(schema.generate()?)
                .base_timestamp(119_731_017_000)
                .build()?,
        )
    };

    schema_registry.validate(topic, &record_batch_003).await?;

    let lake_house = Url::parse(location.as_ref())
        .map_err(Into::into)
        .and_then(|location| {
            Builder::<PhantomData<Url>, PhantomData<Registry>>::default()
                .location(location)
                .database(Some(database.into()))
                .schema_registry(schema_registry)
                .build()
        })?;

    let offset = 765434567;

    lake_house
        .store(topic, partition, offset, &record_batch_003, config)
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
        "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+----------------------------+------------------------+-----------------+----------------------+-------------------------+--------------------+",
        "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.email_address | value.full_name | value.home.building_number | value.home.street_name | value.home.city | value.home.post_code | value.home.country_name | value.industry     |",
        "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+----------------------------+------------------------+-----------------+----------------------+-------------------------+--------------------+",
        "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               | ipsum           | dolor                      | sit                    | amet            | consectetur          | adipiscing              | [elit, elit, elit] |",
        "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               | ipsum           |                            |                        |                 |                      |                         |                    |",
        "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               |                 |                            |                        |                 |                      |                         |                    |",
        "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+----------------------------+------------------------+-----------------+----------------------+-------------------------+--------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
