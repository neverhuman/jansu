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

//! Protobuf registry roundtrip tests

use super::*;

#[tokio::test]
async fn key_only_invalid_record() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

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

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.proto"));
    let payload = PutPayload::from(proto.clone());
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let key = Schema::try_from(proto.clone())
        .and_then(|schema| schema.encode_from_value(MessageKind::Key, &json!({"id": 12321})))?;

    let batch = Batch::builder()
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

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.proto"));
    let payload = PutPayload::from(proto.clone());
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let value = Schema::try_from(proto).and_then(|schema| {
        schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "name": "alice",
                "email": "alice@example.com"
            }),
        )
    })?;

    let batch = Batch::builder()
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

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.proto"));
    let payload = PutPayload::from(proto.clone());
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let schema = Schema::try_from(proto.clone())?;

    let key = schema.encode_from_value(MessageKind::Key, &json!({"id": 12321}))?;
    let value = schema.encode_from_value(
        MessageKind::Value,
        &json!({
            "name": "alice",
            "email": "alice@example.com"
        }),
    )?;

    let batch = Batch::builder()
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

    let proto = Bytes::from_static(br#"syntax = 'proto3';"#);

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.proto"));
    let payload = PutPayload::from(proto.clone());
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
    let value = Bytes::from_static(b"Consectetur adipiscing elit");

    let batch = Batch::builder()
        .record(
            Record::builder()
                .key(key.clone().into())
                .value(value.clone().into()),
        )
        .build()?;

    registry.validate(topic, &batch).await
}
