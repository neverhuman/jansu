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

//! Tests for the JSON schema validator

use super::*;
use crate::Registry;

use bytes::Bytes;
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

use jansu_sans_io::{ErrorCode, record::Record};
use serde_json::json;
use std::{fs::File, sync::Arc, thread};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

fn init_tracing() -> Result<DefaultGuard> {
    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

#[test]
fn assign_field_id() {
    let schema = json!({
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
    });

    let ids = field_ids(&schema);

    assert!(ids.contains_key("key"));
    assert!(ids.contains_key("value"));
    assert!(ids.contains_key("value.name"));
    assert!(ids.contains_key("value.email"));
}

#[test]
fn assign_field_id_with_array() {
    let schema = json!({
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
    });

    let ids = field_ids(&schema);

    assert!(ids.contains_key("key"));
    assert!(ids.contains_key("value"));
    assert!(ids.contains_key("value.element"));
}

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

#[tokio::test]
async fn key_schema_only() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let payload = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number"
            },
        }
    }))
    .map(Bytes::from)
    .map(PutPayload::from)?;

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.json"));
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let key = serde_json::to_vec(&json!(12320)).map(Bytes::from)?;

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
async fn bad_key() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let payload = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number"
            },
        }
    }))
    .map(Bytes::from)
    .map(PutPayload::from)?;

    let object_store = InMemory::new();
    let location = Path::from(format!("{topic}.json"));
    _ = object_store.put(&location, payload).await?;

    let registry = Registry::new(object_store);

    let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

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
async fn value_schema_only() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let payload = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
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

    let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

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
async fn bad_value() -> Result<()> {
    let _guard = init_tracing()?;

    let topic = "def";

    let payload = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
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

    let value = Bytes::from_static(b"Consectetur adipiscing elit");

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

#[test]
fn integer_type_can_be_float_dot_zero() -> Result<()> {
    let schema = json!({"type": "integer"});
    let validator = jsonschema::validator_for(&schema)?;

    assert!(validator.is_valid(&json!(42)));
    assert!(validator.is_valid(&json!(-1)));
    assert!(validator.is_valid(&json!(1.0)));

    Ok(())
}

#[test]
fn array_with_items_type_basic_output() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "value": {
                "type": "array",
                "items": {
                    "type": "number"
                }
            }
        }
    }))
    .map_err(Into::into)
    .map(Bytes::from)
    .and_then(Schema::try_from)?;

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([1, 2, 3, 4, 5]))
            .flag()
            .valid
    );

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([-1, 2.3, 3, 4.0, 5]))
            .flag()
            .valid
    );

    assert!(
        !schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([3, "different", { "types": "of values" }]))
            .flag()
            .valid
    );

    assert!(
        !schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!({"Not": "an array"}))
            .flag()
            .valid,
    );

    Ok(())
}

#[test]
fn array_basic_output() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "value": {
                "type": "array",
            }
        }
    }))
    .map_err(Into::into)
    .map(Bytes::from)
    .and_then(Schema::try_from)?;

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([1, 2, 3, 4, 5]))
            .flag()
            .valid
    );

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([3, "different", { "types": "of values" }]))
            .flag()
            .valid
    );

    assert!(
        !schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!({"Not": "an array"}))
            .flag()
            .valid,
    );

    Ok(())
}

#[test]
fn schema_basic_output() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = serde_json::to_vec(&json!({
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
    .map_err(Into::into)
    .map(Bytes::from)
    .and_then(Schema::try_from)?;

    tracing::debug!(?schema);

    assert!(
        schema
            .key
            .as_ref()
            .unwrap()
            .evaluate(&json!(12321))
            .flag()
            .valid
    );

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!({"name": "alice", "email": "alice@example.com"}))
            .flag()
            .valid
    );

    Ok(())
}
