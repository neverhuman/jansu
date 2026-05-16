use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    use ::arrow::util::pretty::pretty_format_batches;

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    use datafusion::prelude::*;

    #[cfg(feature = "iceberg")]
    use iceberg::{
        io::FileIOBuilder,
        spec::{
            DataFile, DataFileFormat::Parquet, Schema as IcebergSchema,
            SchemaRef as IcebergSchemaRef,
        },
        writer::{
            IcebergWriter, IcebergWriterBuilder,
            base_writer::data_file_writer::DataFileWriterBuilder,
            file_writer::{
                ParquetWriterBuilder,
                location_generator::{DefaultFileNameGenerator, LocationGenerator},
                rolling_writer::RollingFileWriterBuilder,
            },
        },
    };

    #[cfg(feature = "iceberg")]
    use parquet::file::properties::WriterProperties;

    use jansu_sans_io::record::Record;
    use serde_json::json;
    use std::{fs::File, sync::Arc, thread};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::{AsJsonValue as _, Generator as _};

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    #[cfg(feature = "iceberg")]
    async fn enumeration() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "t";

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Key {
                int32 id = 1;
            }

            enum Corpus {
              CORPUS_UNSPECIFIED = 0;
              CORPUS_UNIVERSAL = 1;
              CORPUS_WEB = 2;
              CORPUS_IMAGES = 3;
              CORPUS_LOCAL = 4;
              CORPUS_NEWS = 5;
              CORPUS_PRODUCTS = 6;
              CORPUS_VIDEO = 7;
            }

            message Value {
              string query = 1;
              int32 page_number = 2;
              int32 results_per_page = 3;
              Corpus corpus = 4;
            }
            "#,
        );

        let kv = [
            (
                &json!({"id": 32123}),
                &json!({"query": "abc/def", "pageNumber": 6, "resultsPerPage": 13, "corpus": "CORPUS_WEB"}),
            ),
            (
                &json!({"id": 45654}),
                &json!({"query": "pqr/stu", "pageNumber": 42, "resultsPerPage": 5, "corpus": "CORPUS_PRODUCTS"}),
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

        let record_batch = schema
            .as_arrow(topic, 0, &batch, LakeHouseType::Iceberg)
            .await?;

        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(2, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch(topic, record_batch.clone())?;
        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------+",
            "| meta                                                                               | key         | value                                                             |",
            "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17}     | {id: 32123} | {query: abc/def, page_number: 6, results_per_page: 13, corpus: 2} |",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57.001, year: 1973, month: 10, day: 17} | {id: 45654} | {query: pqr/stu, page_number: 42, results_per_page: 5, corpus: 6} |",
            "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        assert_eq!(
            json!([{"key": {"id": 32123},
                    "value": {
                        "query": "abc/def",
                        "pageNumber": 6,
                        "resultsPerPage": 13,
                        "corpus": "CORPUS_WEB"}},
                    {"key": {"id": 45654},
                     "value": {
                         "query": "pqr/stu",
                         "pageNumber": 42,
                         "resultsPerPage": 5,
                         "corpus": "CORPUS_PRODUCTS"}}]),
            schema.as_json_value(&batch)?
        );

        Ok(())
    }

    #[cfg(feature = "iceberg")]
    async fn iceberg_write(record_batch: RecordBatch) -> Result<Vec<DataFile>> {
        let iceberg_schema = IcebergSchema::try_from(record_batch.schema().as_ref())
            .map(IcebergSchemaRef::new)
            .inspect(|schema| debug!(?schema))
            .inspect_err(|err| debug!(?err))?;

        let memory = FileIOBuilder::new("memory").build()?;

        #[derive(Clone)]
        struct Location;

        impl LocationGenerator for Location {
            fn generate_location(
                &self,
                _partition_key: Option<&iceberg::spec::PartitionKey>,
                file_name: &str,
            ) -> String {
                format!("abc/{file_name}")
            }
        }

        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema);

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            memory,
            Location,
            DefaultFileNameGenerator::new("pqr".into(), None, Parquet),
        );

        let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(None)
            .await
            .inspect_err(|err| error!(?err))?;

        data_file_writer
            .write(record_batch)
            .await
            .inspect_err(|err| debug!(?err))?;

        data_file_writer
            .close()
            .await
            .inspect(|data_files| debug!(?data_files))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    #[tokio::test]
    #[cfg(feature = "iceberg")]
    async fn message_descriptor_singular_to_field() -> Result<()> {
        let _guard = init_tracing()?;

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
                bytes o = 15;
            }
            "#,
        );

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
                    "n": "Hello World!",
                    "o": "YWJjMTIzIT8kKiYoKSctPUB+"}),
        )];

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

        let topic = "ty";
        let partition = 0;

        let record_batch = schema
            .as_arrow(topic, partition, &batch, LakeHouseType::Iceberg)
            .await?;

        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(1, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                           | key         | value                                                                                                                                                                                    |",
            "+--------------------------------------------------------------------------------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {id: 32123} | {a: 567.65, b: 45.654, c: -6, d: -66, e: 23432, f: 34543, g: 45654, h: 67876, i: 78987, j: 89098, k: 90109, l: 12321, m: true, n: Hello World!, o: 616263313233213f242a262829272d3d407e} |",
            "+--------------------------------------------------------------------------------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

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
        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
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

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../../../../../jansu/etc/schema/taxi.proto"
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
        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
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
        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
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
        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
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
        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
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
        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
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
        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
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

    #[tokio::test]
    #[cfg(feature = "parquet")]
    async fn customer_001() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../../../tests/customer-001.proto"
        )))?;

        let topic = "t";
        let partition = 0;
        let ctx = SessionContext::new();

        let batch = schema.generate().and_then(|record| {
            Batch::builder()
                .record(record)
                .base_timestamp(119_731_017_000)
                .build()
                .map_err(Into::into)
        })?;

        let record_batch = schema
            .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
            .await?;
        _ = ctx.register_batch(topic, record_batch)?;

        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                           | value                                                                                                                                                    |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {email_address: lorem, full_name: ipsum, home: {building_number: dolor, street_name: sit, city: amet, post_code: consectetur, country_name: adipiscing}} |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "parquet")]
    async fn customer_002() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../../../tests/customer-002.proto"
        )))?;

        let topic = "t";
        let partition = 0;
        let ctx = SessionContext::new();

        let batch = schema.generate().and_then(|record| {
            Batch::builder()
                .record(record)
                .base_timestamp(119_731_017_000)
                .build()
                .map_err(Into::into)
        })?;

        let record_batch = schema
            .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
            .await?;
        _ = ctx.register_batch(topic, record_batch)?;

        let df = ctx.sql(&datafusion_table_query(topic)?).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                           | value                                                                                                                                                                                              |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {user_id: 0, email_address: lorem, full_name: ipsum, home: {building_number: dolor, street_name: sit, city: amet, post_code: consectetur, country_name: adipiscing}, industry: [elit, elit, elit]} |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }
}
