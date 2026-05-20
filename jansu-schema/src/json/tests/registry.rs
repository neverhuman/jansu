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

//! JSON schema registry roundtrip tests

use super::*;

#[tokio::test]
async fn key_only_invalid_record() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let payload = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number"
            },
            "value": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                    },
                    "email": {
                        "type": "string",
                        "format": "email"
                    }
                }
            }
        }
    }))
    .map(Bytes::from)
    .map(PutPayload::from)?;

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.json"));
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let key = serde_json::to_vec(&json!(12320)).map(Bytes::from)?;

    let batch = Batch::builder()
        .base_timestamp(1_234_567_890 * 1_000)
        .record(Record::builder().key(key.clone().into()))
        .build()?;

    assert!(matches!(
        registry.validate(topic, &batch).await,
        Err(Error::Api(ErrorCode::InvalidRecord))
    ));

    Ok(())
}

#[tokio::test]
async fn value_only_invalid_record() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let payload = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number"
            },
            "value": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                    },
                    "email": {
                        "type": "string",
                        "format": "email"
                    }
                }
            }
        }
    }))
    .map(Bytes::from)
    .map(PutPayload::from)?;

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.json"));
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let value = serde_json::to_vec(&json!({
        "name": "alice",
        "email": "alice@example.com"}))
    .map(Bytes::from)?;

    let batch = Batch::builder()
        .base_timestamp(1_234_567_890 * 1_000)
        .record(Record::builder().value(value.clone().into()))
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

    let payload = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number"
            },
            "value": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                    },
                    "email": {
                        "type": "string",
                        "format": "email"
                    }
                }
            }
        }
    }))
    .map(Bytes::from)
    .map(PutPayload::from)?;

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.json"));
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let key = serde_json::to_vec(&json!(12320)).map(Bytes::from)?;

    let value = serde_json::to_vec(&json!({
            "name": "alice",
            "email": "alice@example.com"}))
    .map(Bytes::from)?;

    let batch = Batch::builder()
        .base_timestamp(1_234_567_890 * 1_000)
        .record(
            Record::builder()
                .key(key.clone().into())
                .value(value.clone().into()),
        )
        .build()?;

    registry.validate(topic, &batch).await
}

#[tokio::test]
async fn no_schema() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let registry = Registry::new(InMemory::new());

    let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
    let value = Bytes::from_static(b"Consectetur adipiscing elit");

    let batch = Batch::builder()
        .base_timestamp(1_234_567_890 * 1_000)
        .record(
            Record::builder()
                .key(key.clone().into())
                .value(value.clone().into()),
        )
        .build()?;

    registry.validate(topic, &batch).await
}

#[tokio::test]
async fn empty_schema() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let payload = serde_json::to_vec(&json!({}))
        .map(Bytes::from)
        .map(PutPayload::from)?;

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.json"));
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
    let value = Bytes::from_static(b"Consectetur adipiscing elit");

    let batch = Batch::builder()
        .base_timestamp(1_234_567_890 * 1_000)
        .record(
            Record::builder()
                .key(key.clone().into())
                .value(value.clone().into()),
        )
        .build()?;

    registry.validate(topic, &batch).await
}
