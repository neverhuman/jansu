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

//! JSON Arrow nested array and struct tests

use super::*;

#[tokio::test]
#[cfg(feature = "iceberg")]
async fn primitive_key_and_array_value_as_arrow() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let schema = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number"
            },
            "value": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            }
        }
    }))
    .map_err(Into::into)
    .map(Bytes::from)
    .and_then(Schema::try_from)?;

    let kv = [
        (json!(12321), json!(["a", "b", "c"])),
        (json!(32123), json!(["p", "q", "r"])),
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
        "+-------------------------------------------------------------------------------------+-------+-----------+",
        "| meta                                                                                | key   | value     |",
        "+-------------------------------------------------------------------------------------+-------+-----------+",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 | [a, b, c] |",
        "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 32123 | [p, q, r] |",
        "+-------------------------------------------------------------------------------------+-------+-----------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(feature = "iceberg")]
async fn primitive_key_and_array_object_value_as_arrow() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let schema = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number"
            },
            "value": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "quantity": {
                            "type": "integer",
                        },
                        "location": {
                            "type": "string",
                        }
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
            json!([{"quantity": 6, "location": "abc"}, {"quantity": 11, "location": "pqr"}]),
        ),
        (
            json!(32123),
            json!([{"quantity": 3, "location": "abc"},
                   {"quantity": 33, "location": "def"},
                   {"quantity": 21, "location": "xyz"}]),
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
    assert_eq!(3, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from def").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+-------+----------------------------------------------------------------------------------------------+",
        "| key   | value                                                                                        |",
        "+-------+----------------------------------------------------------------------------------------------+",
        "| 12321 | [{location: abc, quantity: 6}, {location: pqr, quantity: 11}]                                |",
        "| 32123 | [{location: abc, quantity: 3}, {location: def, quantity: 33}, {location: xyz, quantity: 21}] |",
        "+-------+----------------------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(feature = "iceberg")]
async fn primitive_key_and_struct_with_array_field_value_as_arrow() -> Result<()> {
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
                    "zone": {
                        "type": "number",
                    },
                    "locations": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
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
            json!({"zone": 6, "locations": ["abc", "def"]}),
        ),
        (json!(32123), json!({"zone": 11, "locations": ["pqr"]})),
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
        "+-------+----------------------------------+",
        "| key   | value                            |",
        "+-------+----------------------------------+",
        "| 12321 | {locations: [abc, def], zone: 6} |",
        "| 32123 | {locations: [pqr], zone: 11}     |",
        "+-------+----------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
