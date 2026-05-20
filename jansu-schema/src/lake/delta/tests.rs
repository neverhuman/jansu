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

//! Delta Lake integration tests

use arrow::util::pretty::pretty_format_batches;
use bytes::Bytes;
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTableBuilder;
use jansu_sans_io::{
    ConfigResource, ErrorCode,
    describe_configs_response::DescribeConfigsResourceResult,
    record::{Record, inflated::Batch},
};
use object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path};
use serde_json::json;
use std::{fs, fs::File, marker::PhantomData, path::PathBuf, sync::Arc, thread};
use tempfile::tempdir;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;
use url::Url;

use crate::Error;

use super::*;

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

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests run from the crate directory")
        .to_path_buf()
}

fn repo_asset_path(relative: &str) -> PathBuf {
    repo_root().join(relative)
}

fn repo_asset_bytes(relative: &str) -> Result<Bytes> {
    Ok(Bytes::from(fs::read(repo_asset_path(relative))?))
}

fn schema_registry_url() -> Url {
    Url::from_directory_path(repo_asset_path("etc/schema"))
        .expect("repository schema directory should exist")
}

fn repo_schema_registry() -> Result<Registry> {
    Ok(Registry::builder_try_from_url(&schema_registry_url())?.build())
}

#[test]
fn config_is_normalized_rejects_invalid_boolean() {
    let config = Config(vec![(
        String::from("jansu.lake.normalize"),
        String::from("maybe"),
    )]);

    assert!(matches!(
        config.is_normalized(),
        Err(Error::Message(message)) if message.contains("jansu.lake.normalize")
    ));
}

mod avro;
mod json;
mod proto1;
mod proto2;
mod proto3;
mod proto4;
mod proto5;
mod proto6;
mod sql;
