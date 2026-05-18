use super::*;
use crate::{
    Generator,
    proto::{MessageKind, Schema},
};

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
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

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../../../etc/schema/taxi.proto"
    )))?;

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let schema_registry = Registry::from_str("file://../../../etc/schema")?;

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

#[tokio::test]
async fn taxi_normalized() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "taxi";

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../../../etc/schema/taxi.proto"
    )))?;

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let schema_registry = Registry::from_str("file://../../../etc/schema")?;

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

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../../../etc/schema/taxi.proto"
    )))?;

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let schema_registry = Registry::from_str("file://../../../etc/schema")?;

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

#[tokio::test]
async fn taxi_normalized_partition_on_value_dot_vendor_id() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "taxi";

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../../../etc/schema/taxi.proto"
    )))?;

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let schema_registry = Registry::from_str("file://../../../etc/schema")?;

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
                .name(String::from("jansu.lake.partition"))
                .value(Some(String::from("value.vendor_id")))
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
        "+----------------+---------------------+-----------+------------+----------+---------------+---------------------+-------------------+---------------------+-----------------+",
        "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.trip_id | value.trip_distance | value.fare_amount | value.store_and_fwd | value.vendor_id |",
        "+----------------+---------------------+-----------+------------+----------+---------------+---------------------+-------------------+---------------------+-----------------+",
        "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | 1000371       | 1.8                 | 15.32             | 0                   | 1               |",
        "+----------------+---------------------+-----------+------------+----------+---------------+---------------------+-------------------+---------------------+-----------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
async fn taxi_date_generated_field() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "taxi";

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../../../etc/schema/taxi.proto"
    )))?;

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let schema_registry = Registry::from_str("file://../../../etc/schema")?;

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
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
        "| meta                                                                               | value                                                                                      | date       |",
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
        "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} | 1973-10-17 |",
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
async fn taxi_partition_on_date_generated_field() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "taxi";

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../../../etc/schema/taxi.proto"
    )))?;

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let schema_registry = Registry::from_str("file://../../../etc/schema")?;

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
                .name(String::from("jansu.lake.generate.date"))
                .value(Some(String::from("cast(meta.timestamp as date)")))
                .read_only(true)
                .is_default(None)
                .config_source(None)
                .is_sensitive(false)
                .synonyms(None)
                .config_type(None)
                .documentation(None),
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.partition"))
                .value(Some(String::from("date")))
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
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
        "| meta                                                                               | value                                                                                      | date       |",
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
        "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} | 1973-10-17 |",
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
async fn taxi_partition_on_value_vendor_id_is_an_error() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "taxi";

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../../../etc/schema/taxi.proto"
    )))?;

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let schema_registry = Registry::from_str("file://../../../etc/schema")?;

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
                .name(String::from("jansu.lake.partition"))
                .value(Some(String::from("value.vendor_id")))
                .read_only(true)
                .is_default(None)
                .config_source(None)
                .is_sensitive(false)
                .synonyms(None)
                .config_type(None)
                .documentation(None),
        ]));

    let offset = 543212345;

    let not_found_in_schema = "Partition column value.vendor_id not found in schema";

    assert!(matches!(
        lake_house
            .store(topic, partition, offset, &record_batch, config)
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err)),
        Err(Error::DeltaTable(ref boxed)) if matches!(&**boxed, deltalake::errors::DeltaTableError::Generic(error) if error == not_found_in_schema)
    ));

    Ok(())
}

#[tokio::test]
async fn taxi_partition_on_vendor_id_generated_field() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "taxi";

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../../../etc/schema/taxi.proto"
    )))?;

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

    let schema_registry = Registry::from_str("file://../../../etc/schema")?;

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
        .error_message(None)
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic.into())
        .configs(Some(vec![
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.generate.year"))
                .value(Some(String::from("cast(meta.year as integer)")))
                .read_only(true)
                .is_default(None)
                .config_source(None)
                .is_sensitive(false)
                .synonyms(None)
                .config_type(None)
                .documentation(None),
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.generate.month"))
                .value(Some(String::from("cast(meta.month as integer)")))
                .read_only(true)
                .is_default(None)
                .config_source(None)
                .is_sensitive(false)
                .synonyms(None)
                .config_type(None)
                .documentation(None),
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.generate.day"))
                .value(Some(String::from("cast(meta.day as integer)")))
                .read_only(true)
                .is_default(None)
                .config_source(None)
                .is_sensitive(false)
                .synonyms(None)
                .config_type(None)
                .documentation(None),
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.generate.vendor_id"))
                .value(Some(String::from("cast(value.vendor_id as integer)")))
                .read_only(true)
                .is_default(None)
                .config_source(None)
                .is_sensitive(false)
                .synonyms(None)
                .config_type(None)
                .documentation(None),
            DescribeConfigsResourceResult::default()
                .name(String::from("jansu.lake.partition"))
                .value(Some(String::from("year,month,day,vendor_id")))
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
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------+-------+-----+-----------+",
        "| meta                                                                               | value                                                                                      | year | month | day | vendor_id |",
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------+-------+-----+-----------+",
        "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} | 1973 | 10    | 17  | 1         |",
        "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------+-------+-----+-----------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[tokio::test]
async fn repeated_string() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "t";

    let proto = Bytes::from_static(include_bytes!("../../../../tests/repeated-string.proto"));
    let object_store = InMemory::new();

    let location = Path::from(format!("{topic}.proto"));
    _ = object_store
        .put(&location, PutPayload::from(proto.clone()))
        .await?;
    let schema_registry = Registry::new(object_store);

    let schema = Schema::try_from(proto)?;

    let value = schema.encode_from_value(
        MessageKind::Value,
        &json!({
          "id": 12321,
          "industry": ["abc", "def", "pqr"],
        }),
    )?;

    let partition = 32123;

    let record_batch = Batch::builder()
        .record(Record::builder().value(value.into()))
        .base_timestamp(119_731_017_000)
        .build()?;

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "pqr";

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
        "+------------------------------------------------------------------------------------+----------------------------------------+",
        "| meta                                                                               | value                                  |",
        "+------------------------------------------------------------------------------------+----------------------------------------+",
        "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {id: 12321, industry: [abc, def, pqr]} |",
        "+------------------------------------------------------------------------------------+----------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

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

    let temp_dir = tempdir().inspect(|scratch| debug!(?scratch))?;
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
