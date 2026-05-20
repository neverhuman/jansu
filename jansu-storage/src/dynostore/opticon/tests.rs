use object_store::{PutPayload, memory::InMemory};
use serde::{Deserialize, Serialize};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

use crate::Error;

use super::*;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct X(i32);

fn init_tracing() -> Result<DefaultGuard> {
    use std::{fs::File, sync::Arc, thread};

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(EnvFilter::from_default_env().add_directive(
                format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
            ))
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

#[tokio::test]
async fn with_does_not_exist() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let object_store = InMemory::new();

    let o = OptiCon::path(path.clone());

    assert_eq!(1, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

    assert!(matches!(
        object_store.get(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    assert_eq!(1, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

    assert!(matches!(
        object_store.get(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    Ok(())
}

#[tokio::test]
async fn with_mut_does_not_exist() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let object_store = InMemory::new();

    let o = OptiCon::path(path.clone());

    let expected = 1;
    assert_eq!(
        expected,
        o.with_mut(&object_store, |x: &mut X| {
            x.0 += 1;
            Ok(x.0)
        })
        .await?
    );

    let get_result = object_store.get(&path).await?;
    let encoded = get_result.bytes().await?;
    let data = serde_json::from_slice::<X>(&encoded)?;
    assert_eq!(expected, data.0);

    let expected = 2;
    assert_eq!(
        expected,
        o.with_mut(&object_store, |x: &mut X| {
            x.0 += 1;
            Ok(x.0)
        })
        .await?
    );

    let get_result = object_store.get(&path).await?;
    let encoded = get_result.bytes().await?;
    let data = serde_json::from_slice::<X>(&encoded)?;
    assert_eq!(expected, data.0);

    Ok(())
}

#[tokio::test]
async fn with_did_exist() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let object_store = InMemory::new();

    _ = object_store
        .put(
            &path,
            serde_json::to_vec(&X(6))
                .map(Bytes::from)
                .map(PutPayload::from)?,
        )
        .await?;

    let o = OptiCon::path(path.clone());

    assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

    object_store.delete(&path).await?;

    assert_eq!(1, o.with(&object_store, |x| Ok(x.0 + 1)).await?);

    assert!(matches!(
        object_store.get(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    assert_eq!(1, o.with(&object_store, |x| Ok(x.0 + 1)).await?);

    assert!(matches!(
        object_store.get(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    Ok(())
}

#[tokio::test]
async fn with_mut_did_exist() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let object_store = InMemory::new();

    _ = object_store
        .put(
            &path,
            serde_json::to_vec(&X(6))
                .map(Bytes::from)
                .map(PutPayload::from)?,
        )
        .await?;

    let o = OptiCon::path(path.clone());

    let expected = 7;
    assert_eq!(
        expected,
        o.with_mut(&object_store, |x: &mut X| {
            x.0 += 1;
            Ok(x.0)
        })
        .await?
    );

    let get_result = object_store.get(&path).await?;
    let encoded = get_result.bytes().await?;
    let data = serde_json::from_slice::<X>(&encoded)?;
    assert_eq!(expected, data.0);

    object_store.delete(&path).await?;

    let expected = 1;
    assert_eq!(
        expected,
        o.with_mut(&object_store, |x| {
            x.0 += 1;
            Ok(x.0)
        })
        .await?
    );

    let get_result = object_store.get(&path).await?;
    let encoded = get_result.bytes().await?;
    let data = serde_json::from_slice::<X>(&encoded)?;
    assert_eq!(expected, data.0);

    let expected = 2;
    assert_eq!(
        expected,
        o.with_mut(&object_store, |x| {
            x.0 += 1;
            Ok(x.0)
        })
        .await?
    );

    let get_result = object_store.get(&path).await?;
    let encoded = get_result.bytes().await?;
    let data = serde_json::from_slice::<X>(&encoded)?;
    assert_eq!(expected, data.0);

    Ok(())
}

#[tokio::test]
async fn with_already_exists() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let object_store = InMemory::new();

    _ = object_store
        .put(
            &path,
            serde_json::to_vec(&X(6))
                .map(Bytes::from)
                .map(PutPayload::from)?,
        )
        .await?;

    let o = OptiCon::path(path.clone());

    assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

    let get_result = object_store.get(&path).await?;
    let encoded = get_result.bytes().await?;
    let data = serde_json::from_slice::<X>(&encoded)?;
    assert_eq!(6, data.0);

    assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

    let get_result = object_store.get(&path).await?;
    let encoded = get_result.bytes().await?;
    let data = serde_json::from_slice::<X>(&encoded)?;
    assert_eq!(6, data.0);

    Ok(())
}

#[tokio::test]
async fn with_mut_already_exists() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let object_store = InMemory::new();

    _ = object_store
        .put(
            &path,
            serde_json::to_vec(&X(6))
                .map(Bytes::from)
                .map(PutPayload::from)?,
        )
        .await?;

    let o = OptiCon::path(path.clone());

    assert_eq!(
        42,
        o.with_mut(&object_store, |x: &mut X| {
            x.0 += 1;

            Ok(6 * x.0)
        })
        .await?
    );

    let get_result = object_store.get(&path).await?;
    let encoded = get_result.bytes().await?;
    let data = serde_json::from_slice::<X>(&encoded)?;
    assert_eq!(7, data.0);

    assert_eq!(
        48,
        o.with_mut(&object_store, |x: &mut X| {
            x.0 += 1;

            Ok(6 * x.0)
        })
        .await?
    );

    let get_result = object_store.get(&path).await?;
    let encoded = get_result.bytes().await?;
    let data = serde_json::from_slice::<X>(&encoded)?;
    assert_eq!(8, data.0);

    Ok(())
}
