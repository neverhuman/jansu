use std::{
    fmt::Display,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

use crate::{Error, Result};

use super::*;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct X(i32);

#[derive(Clone, Debug)]
struct Counter<O> {
    put_opts: Arc<Mutex<u64>>,
    get_opts: Arc<Mutex<u64>>,
    object_store: O,
}

#[allow(dead_code)]
impl<O> Counter<O> {
    fn new(object_store: O) -> Self {
        Self {
            put_opts: Default::default(),
            get_opts: Default::default(),
            object_store,
        }
    }

    fn put_opts(&self) -> Result<u64> {
        self.put_opts.lock().map(|guard| *guard).map_err(Into::into)
    }

    fn get_opts(&self) -> Result<u64> {
        self.get_opts.lock().map(|guard| *guard).map_err(Into::into)
    }
}

impl<O> Display for Counter<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Counter").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for Counter<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if let Ok(mut guard) = self.put_opts.lock() {
            *guard += 1;
        }

        self.object_store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.object_store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        if let Ok(mut guard) = self.get_opts.lock() {
            *guard += 1;
        }

        self.object_store.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.object_store.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.object_store.copy_opts(from, to, opts).await
    }
}

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
async fn get() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let object_store = Counter::new(InMemory::new());

    _ = object_store
        .put(
            &path,
            serde_json::to_vec(&X(6))
                .map(Bytes::from)
                .map(PutPayload::from)?,
        )
        .await?;

    let duration = Duration::from_millis(100);

    let cache = Cache::new(object_store, duration);

    assert_eq!(0, cache.inner().get_opts()?);

    let metadata = cache.get(&path).await?;
    assert_eq!(1, cache.inner().get_opts()?);

    let options = GetOptions {
        if_none_match: metadata.meta.e_tag.clone(),
        ..Default::default()
    };

    assert!(matches!(
        cache.get_opts(&path, options).await,
        Err(object_store::Error::NotModified { .. })
    ));
    assert_eq!(1, cache.inner().get_opts()?);

    Ok(())
}

#[tokio::test]
async fn get_evict_get() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let object_store = Counter::new(InMemory::new());

    _ = object_store
        .put(
            &path,
            serde_json::to_vec(&X(6))
                .map(Bytes::from)
                .map(PutPayload::from)?,
        )
        .await?;

    let duration = Duration::from_millis(100);
    let cache = Cache::new(object_store, duration);

    assert_eq!(0, cache.inner().get_opts()?);

    let metadata = cache.get(&path).await?;
    assert_eq!(1, cache.inner().get_opts()?);

    let options = GetOptions {
        if_none_match: metadata.meta.e_tag.clone(),
        ..Default::default()
    };

    assert!(matches!(
        cache.get_opts(&path, options.clone()).await,
        Err(object_store::Error::NotModified { .. })
    ));
    assert_eq!(1, cache.inner().get_opts()?);

    sleep(duration).await;

    assert!(matches!(
        cache.get_opts(&path, options).await,
        Err(object_store::Error::NotModified { .. })
    ));
    assert_eq!(2, cache.inner().get_opts()?);

    Ok(())
}

#[tokio::test]
async fn put_get() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let duration = Duration::from_millis(5_000);
    let cache = Cache::new(Counter::new(InMemory::new()), duration);

    assert_eq!(0, cache.inner().put_opts()?);
    assert_eq!(0, cache.inner().get_opts()?);

    let put_result = cache
        .put(
            &path,
            serde_json::to_vec(&X(6))
                .map(Bytes::from)
                .map(PutPayload::from)?,
        )
        .await?;
    assert_eq!(1, cache.inner().put_opts()?);
    assert_eq!(0, cache.inner().get_opts()?);

    let options = GetOptions {
        if_none_match: put_result.e_tag,
        ..Default::default()
    };

    assert!(matches!(
        cache.get_opts(&path, options.clone()).await,
        Err(object_store::Error::NotModified { .. })
    ));
    assert_eq!(0, cache.inner().get_opts()?);

    assert!(matches!(
        cache.get_opts(&path, options).await,
        Err(object_store::Error::NotModified { .. })
    ));
    assert_eq!(0, cache.inner().get_opts()?);

    Ok(())
}

#[tokio::test]
async fn put_delete_get() -> Result<()> {
    let _guard = init_tracing()?;

    let id = "test";
    let path = Path::from(format!("/abc/{id}.json"));

    let duration = Duration::from_millis(100);
    let cache = Cache::new(Counter::new(InMemory::new()), duration);

    assert_eq!(0, cache.inner().put_opts()?);
    assert_eq!(0, cache.inner().get_opts()?);

    let put_result = cache
        .put(
            &path,
            serde_json::to_vec(&X(6))
                .map(Bytes::from)
                .map(PutPayload::from)?,
        )
        .await?;
    assert_eq!(1, cache.inner().put_opts()?);
    assert_eq!(0, cache.inner().get_opts()?);

    cache.delete(&path).await?;

    let options = GetOptions {
        if_none_match: put_result.e_tag,
        ..Default::default()
    };

    assert!(matches!(
        cache.get_opts(&path, options.clone()).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(1, cache.inner().get_opts()?);

    Ok(())
}
