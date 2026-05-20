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

//! AVRO Arrow conversion tests (group7)

use super::*;

#[ignore]
#[tokio::test]
#[cfg(feature = "parquet")]
async fn decimal_fixed_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": {
                "type": "fixed",
                "size": 8,
                "name": "decimal"
            },
            "logicalType": "decimal",
            "precision": 8,
            "scale": 2,
        }]
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [32123, 45654, 87678, 12321]
            .into_iter()
            .map(BigInt::from)
            .map(|big_int| big_int.to_signed_bytes_be())
            .map(Decimal::from)
            .map(Value::Decimal)
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

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+------------+",
        "| value      |",
        "+------------+",
        "| 1970-01-02 |",
        "| 1973-10-17 |",
        "| 2001-09-09 |",
        "| 2009-02-13 |",
        "+------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

#[ignore]
#[tokio::test]
#[cfg(feature = "parquet")]
async fn decimal_variable_logical_type() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "value",
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 8,
            "scale": 2,
        }]
    }));

    let batch = {
        let mut batch = Batch::builder();

        let values = [32123, 45654, 87678, 12321]
            .into_iter()
            .map(BigInt::from)
            .map(|big_int| big_int.to_signed_bytes_be())
            .map(Decimal::from)
            .map(Value::Decimal)
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

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+------------+",
        "| value      |",
        "+------------+",
        "| 1970-01-02 |",
        "| 1973-10-17 |",
        "| 2001-09-09 |",
        "| 2009-02-13 |",
        "+------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
