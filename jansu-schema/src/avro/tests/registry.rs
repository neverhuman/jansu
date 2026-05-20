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

//! AVRO registry roundtrip tests

use super::*;

#[tokio::test]
async fn key_only_invalid_record() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let schema = json!({
        "type": "record",
        "name": "Test",
        "fields": [{
            "name": "key",
            "type": "int"
        },
        {
            "name": "value",
            "type": {
                "type": "record",
                "name": "person",
                "fields": [{
                    "name": "name",
                    "type": "string"
                },
                {
                    "name": "email",
                    "type": "string"
                }]
            }
    }]});

    let object_store = InMemory::new();
    {
        let location = Path::from(format!("{topic}.avsc"));
        _ = object_store
            .put(
                &location,
                serde_json::to_vec(&schema)
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
    }

    let registry = Registry::new(object_store);

    let key = AvroSchema::parse(&json!({
        "type": "int"
    }))
    .and_then(|schema| {
        let mut writer = apache_avro::Writer::new(&schema, vec![]);
        writer
            .append(Value::Int(32123))
            .and(writer.into_inner())
            .map(Bytes::from)
    })?;

    let batch = Batch::builder()
        .record(Record::builder().key(key.into()))
        .build()?;

    assert!(matches!(
        registry.validate(topic, &batch).await,
        Err(Error::Api(ErrorCode::InvalidRecord))
    ));

    Ok(())
}

#[tokio::test]
async fn key_and_value() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let schema = json!({
        "type": "record",
        "name": "Test",
        "fields": [
            {"name": "key", "type": "int"},
            {"name": "value", "type": {
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"}]}}]});

    let object_store = InMemory::new();
    {
        let location = Path::from(format!("{topic}/.avsc"));
        _ = object_store
            .put(
                &location,
                serde_json::to_vec(&schema)
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
    }

    let registry = Registry::new(object_store);

    let key = AvroSchema::parse(&json!({
        "type": "int"
    }))
    .and_then(|schema| {
        let mut writer = apache_avro::Writer::new(&schema, vec![]);
        writer
            .append(Value::Int(32123))
            .and(writer.into_inner())
            .map(Bytes::from)
    })?;

    let value = AvroSchema::parse(&json!({
        "type": "record",
        "name": "Message",
        "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
    }))
    .and_then(|schema| {
        let mut writer = apache_avro::Writer::new(&schema, vec![]);
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("name", "alice");
        record.put("email", "alice@example.com");

        writer
            .append(record)
            .and(writer.into_inner())
            .map(Bytes::from)
    })?;

    let batch = Batch::builder()
        .record(Record::builder().key(key.into()).value(value.into()))
        .build()?;

    registry.validate(topic, &batch).await
}

#[tokio::test]
async fn value_only_invalid_record() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let schema = json!({
        "type": "record",
        "name": "Test",
        "fields": [
            {"name": "key", "type": "int"},
            {"name": "value", "type": {
                "name": "value",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"}]}}]});

    let object_store = InMemory::new();
    {
        let location = Path::from(format!("{topic}.avsc"));
        _ = object_store
            .put(
                &location,
                serde_json::to_vec(&schema)
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
    }

    let registry = Registry::new(object_store);

    let value = AvroSchema::parse(&json!({
        "type": "record",
        "name": "Message",
        "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
    }))
    .and_then(|schema| {
        let mut writer = apache_avro::Writer::new(&schema, vec![]);
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("name", "alice");
        record.put("email", "alice@example.com");

        writer
            .append(record)
            .and(writer.into_inner())
            .map(Bytes::from)
    })?;

    let batch = Batch::builder()
        .record(Record::builder().value(value.into()))
        .build()?;

    assert!(matches!(
        registry.validate(topic, &batch).await,
        Err(Error::Api(ErrorCode::InvalidRecord))
    ));

    Ok(())
}
