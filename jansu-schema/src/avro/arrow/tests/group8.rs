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

//! AVRO Arrow conversion tests (group8)

use super::*;

#[tokio::test]
#[cfg(all(feature = "parquet", feature = "iceberg"))]
async fn string_key_with_record_as_arrow() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
              "type": "record",
              "name": "test",
              "fields": [
                {
                  "name": "key",
                  "type": "string"
                },
                {
                  "name": "value",
                  "type": {
                    "name": "sub",
                    "type": "record",
                    "fields": [
                      { "name": "first", "type": "string" },
                      { "name": "last", "type": "string" },
                      { "name": "test1", "type": "double" },
                      { "name": "test2", "type": "double" },
                      { "name": "test3", "type": "double" },
                      { "name": "test4", "type": "double" },
                      { "name": "final", "type": "double" },
                      { "name": "grade", "type": "string" }
                    ]
                  }
                }
              ]
            }
    ));

    // https://people.math.sc.edu/Burkardt/datasets/csv/csv.html
    let grades = [
        (
            "Alfalfa",
            "Aloysius",
            "123-45-6789",
            40.0,
            90.0,
            100.0,
            83.0,
            49.0,
            "D-",
        ),
        (
            "Alfred",
            "University",
            "123-12-1234",
            41.0,
            97.0,
            96.0,
            97.0,
            48.0,
            "D+",
        ),
        (
            "Gerty",
            "Gramma",
            "567-89-0123",
            41.0,
            80.0,
            60.0,
            40.0,
            44.0,
            "C",
        ),
        (
            "Android",
            "Electric",
            "087-65-4321",
            42.0,
            23.0,
            36.0,
            45.0,
            47.0,
            "B-",
        ),
        (
            "Bumpkin",
            "Fred",
            "456-78-9012",
            43.0,
            78.0,
            88.0,
            77.0,
            45.0,
            "A-",
        ),
        (
            "Rubble",
            "Betty",
            "234-56-7890",
            44.0,
            90.0,
            80.0,
            90.0,
            46.0,
            "C-",
        ),
        (
            "Noshow",
            "Cecil",
            "345-67-8901",
            45.0,
            11.0,
            -1.0,
            4.0,
            43.0,
            "F",
        ),
        (
            "Buff",
            "Bif",
            "632-79-9939",
            46.0,
            20.0,
            30.0,
            40.0,
            50.0,
            "B+",
        ),
        (
            "Airpump",
            "Andrew",
            "223-45-6789",
            49.0,
            1.0,
            90.0,
            100.0,
            83.0,
            "A",
        ),
        (
            "Backus",
            "Jim",
            "143-12-1234",
            48.0,
            1.0,
            97.0,
            96.0,
            97.0,
            "A+",
        ),
        (
            "Carnivore",
            "Art",
            "565-89-0123",
            44.0,
            1.0,
            80.0,
            60.0,
            40.0,
            "D+",
        ),
        (
            "Dandy",
            "Jim",
            "087-75-4321",
            47.0,
            1.0,
            23.0,
            36.0,
            45.0,
            "C+",
        ),
        (
            "Elephant",
            "Ima",
            "456-71-9012",
            45.0,
            1.0,
            78.0,
            88.0,
            77.0,
            "B-",
        ),
        (
            "Franklin",
            "Benny",
            "234-56-2890",
            50.0,
            1.0,
            90.0,
            80.0,
            90.0,
            "B-",
        ),
        (
            "George",
            "Boy",
            "345-67-3901",
            40.0,
            1.0,
            11.0,
            -1.0,
            4.0,
            "B",
        ),
        (
            "Heffalump",
            "Harvey",
            "632-79-9439",
            30.0,
            1.0,
            20.0,
            30.0,
            40.0,
            "C",
        ),
    ];

    let batch = {
        let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

        for grade in grades {
            let mut value =
                apache_avro::types::Record::new(schema.value.as_ref().unwrap()).unwrap();
            value.put("first", grade.0);
            value.put("last", grade.1);
            value.put("test1", grade.3);
            value.put("test2", grade.4);
            value.put("test3", grade.5);
            value.put("test4", grade.6);
            value.put("final", grade.7);
            value.put("grade", grade.8);

            batch = batch.record(
                Record::builder()
                    .key(schema_write(schema.key.as_ref().unwrap(), grade.2.into())?.into())
                    .value(schema_write(schema.value.as_ref().unwrap(), value.into())?.into()),
            );
        }

        batch.build()
    }?;

    let topic = "t";
    let partition = 0;
    let record_batch = schema
        .as_arrow(topic, partition, &batch, LakeHouseType::Parquet)
        .await?;

    let data_files = iceberg_write(record_batch.clone()).await?;
    assert_eq!(1, data_files.len());
    assert_eq!(16, data_files[0].record_count());

    let ctx = SessionContext::new();

    _ = ctx.register_batch(topic, record_batch)?;
    let df = ctx.sql("select * from t").await?;
    let results = df.collect().await?;

    let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

    let expected = vec![
        "+-------------+---------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
        "| key         | value                                                                                                         | meta                                                                          |",
        "+-------------+---------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
        "| 123-45-6789 | {first: Alfalfa, last: Aloysius, test1: 40.0, test2: 90.0, test3: 100.0, test4: 83.0, final: 49.0, grade: D-} | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 123-12-1234 | {first: Alfred, last: University, test1: 41.0, test2: 97.0, test3: 96.0, test4: 97.0, final: 48.0, grade: D+} | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 567-89-0123 | {first: Gerty, last: Gramma, test1: 41.0, test2: 80.0, test3: 60.0, test4: 40.0, final: 44.0, grade: C}       | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 087-65-4321 | {first: Android, last: Electric, test1: 42.0, test2: 23.0, test3: 36.0, test4: 45.0, final: 47.0, grade: B-}  | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 456-78-9012 | {first: Bumpkin, last: Fred, test1: 43.0, test2: 78.0, test3: 88.0, test4: 77.0, final: 45.0, grade: A-}      | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 234-56-7890 | {first: Rubble, last: Betty, test1: 44.0, test2: 90.0, test3: 80.0, test4: 90.0, final: 46.0, grade: C-}      | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 345-67-8901 | {first: Noshow, last: Cecil, test1: 45.0, test2: 11.0, test3: -1.0, test4: 4.0, final: 43.0, grade: F}        | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 632-79-9939 | {first: Buff, last: Bif, test1: 46.0, test2: 20.0, test3: 30.0, test4: 40.0, final: 50.0, grade: B+}          | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 223-45-6789 | {first: Airpump, last: Andrew, test1: 49.0, test2: 1.0, test3: 90.0, test4: 100.0, final: 83.0, grade: A}     | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 143-12-1234 | {first: Backus, last: Jim, test1: 48.0, test2: 1.0, test3: 97.0, test4: 96.0, final: 97.0, grade: A+}         | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 565-89-0123 | {first: Carnivore, last: Art, test1: 44.0, test2: 1.0, test3: 80.0, test4: 60.0, final: 40.0, grade: D+}      | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 087-75-4321 | {first: Dandy, last: Jim, test1: 47.0, test2: 1.0, test3: 23.0, test4: 36.0, final: 45.0, grade: C+}          | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 456-71-9012 | {first: Elephant, last: Ima, test1: 45.0, test2: 1.0, test3: 78.0, test4: 88.0, final: 77.0, grade: B-}       | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 234-56-2890 | {first: Franklin, last: Benny, test1: 50.0, test2: 1.0, test3: 90.0, test4: 80.0, final: 90.0, grade: B-}     | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 345-67-3901 | {first: George, last: Boy, test1: 40.0, test2: 1.0, test3: 11.0, test4: -1.0, final: 4.0, grade: B}           | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "| 632-79-9439 | {first: Heffalump, last: Harvey, test1: 30.0, test2: 1.0, test3: 20.0, test4: 30.0, final: 40.0, grade: C}    | {partition: 0, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
        "+-------------+---------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}
