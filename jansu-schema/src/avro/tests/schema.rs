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

//! AVRO schema validation tests

use super::*;

#[test]
fn key() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{"name": "key", "type": "int"}]
    }));

    let input = {
        let mut writer = apache_avro::Writer::new(schema.key.as_ref().unwrap(), vec![]);

        writer
            .append(Value::Int(32123))
            .and(writer.into_inner())
            .map(Bytes::from)?
    };

    let batch = Batch::builder()
        .record(Record::builder().key(input.clone().into()))
        .build()?;

    schema.validate(&batch)
}

#[test]
fn invalid_key() -> Result<()> {
    let _guard = init_tracing()?;

    let input = {
        let schema = Schema::from(json!({
            "type": "record",
            "name": "test",
            "fields": [{"name": "key", "type": "long"}]
        }));

        let mut writer = apache_avro::Writer::new(schema.key.as_ref().unwrap(), vec![]);
        writer
            .append(Value::Long(32123))
            .and(writer.into_inner())
            .map(Bytes::from)?
    };

    let batch = Batch::builder()
        .record(Record::builder().key(input.clone().into()))
        .build()?;

    let s = Schema::from(json!({
        "type": "record",
        "name": "test",
        "fields": [{
            "name": "key",
            "type": "string"
        }]
    }));

    assert!(matches!(
        s.validate(&batch),
        Err(Error::Api(ErrorCode::InvalidRecord))
    ));
    Ok(())
}

#[test]
fn simple_schema() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = json!({
        "type": "record",
        "name": "Message",
        "fields": [{"name": "title", "type": "string"}, {"name": "message", "type": "string"}]
    });

    let schema = AvroSchema::parse(&schema)?;

    let mut record = apache_avro::types::Record::new(&schema).unwrap();
    record.put("title", "Lorem ipsum dolor sit amet");
    record.put("message", "consectetur adipiscing elit");

    let mut writer = apache_avro::Writer::new(&schema, vec![]);
    assert!(writer.append(record)? > 0);

    let input = writer.into_inner()?;
    let reader = Reader::with_schema(&schema, &input[..])?;

    let v = reader.into_iter().next().unwrap()?;

    assert_eq!(
        Value::Record(vec![
            (
                "title".into(),
                Value::String("Lorem ipsum dolor sit amet".into()),
            ),
            (
                "message".into(),
                Value::String("consectetur adipiscing elit".into()),
            ),
        ]),
        v
    );

    Ok(())
}
