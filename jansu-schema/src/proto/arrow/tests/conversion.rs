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

//! Protobuf Arrow conversion tests

use super::*;

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
    let df = ctx.sql("select * from t").await?;
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
    let df = ctx.sql("select * from ty").await?;
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
