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

//! Delta Lake AVRO schema tests

use super::*;
use crate::avro::{Schema, r, schema_write};

#[tokio::test]
async fn record_of_primitive_data_types() -> Result<()> {
    let _guard = init_tracing()?;

    let definition = json!({
                  "type": "record",
                  "name": "Message",
                  "fields": [
                    {
                      "name": "value",
                      "type": {
                        "name": "sub",
                        "type": "record",
                        "fields": [
                          { "name": "b", "type": "boolean" },
                          { "name": "c", "type": "int" },
                          { "name": "d", "type": "long" },
                          { "name": "e", "type": "float" },
                          { "name": "f", "type": "double" },
                          { "name": "h", "type": "string" }
                        ]
                      }
                    }
                  ]
                }
    );

    let topic = "abc";
    let partition = 32123;

    let (schema_registry, record_batch) = {
        let schema = Schema::from(definition.clone());
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        let values = [r(
            schema.value.as_ref().unwrap(),
            [
                ("b", false.into()),
                ("c", i32::MAX.into()),
                ("d", i64::MAX.into()),
                ("e", f32::MAX.into()),
                ("f", f64::MAX.into()),
                ("h", "pqr".into()),
            ],
        )];

        for value in values {
            batch = batch.record(
                Record::builder()
                    .value(schema_write(schema.value.as_ref().unwrap(), value.into())?.into()),
            )
        }

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&definition)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;
        }

        let registry = Registry::new(object_store);

        (registry, batch.build()?)
    };

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
        "+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+------------+",
        "| value                                                                                                 | meta                                                                              | date       |",
        "+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+------------+",
        "| {b: false, c: 2147483647, d: 9223372036854775807, e: 3.4028235e38, f: 1.7976931348623157e308, h: pqr} | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} | 2009-02-13 |",
        "+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
