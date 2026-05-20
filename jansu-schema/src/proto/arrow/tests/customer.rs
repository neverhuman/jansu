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

//! Protobuf Arrow customer schema tests

use super::*;

#[tokio::test]
#[cfg(feature = "parquet")]
async fn customer_001() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../../tests/customer-001.proto"
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

    let df = ctx.sql("select * from t").await?;
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
        "../../../../tests/customer-002.proto"
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

    let df = ctx.sql("select * from t").await?;
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
