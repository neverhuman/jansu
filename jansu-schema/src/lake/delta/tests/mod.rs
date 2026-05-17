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
use std::{fs::File, marker::PhantomData, str::FromStr as _, sync::Arc, thread};
use tempfile::tempdir;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

use crate::Error;

use super::*;

pub(super) fn init_tracing() -> Result<DefaultGuard> {
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

mod avro;
mod json;
mod proto;
mod sql;
