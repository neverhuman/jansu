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

//! Dynamic Object Storage engine (S3, memory, ...)

use std::{
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use jansu_sans_io::{
    ErrorCode,
    record::deflated,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use jansu_schema::{Registry, lake::House};
use metadata::Cache;
use object_store::{
    Attribute, AttributeValue, Attributes, DynObjectStore, ObjectStore, ObjectStoreExt, PutMode,
    PutOptions, PutPayload, PutResult, UpdateVersion, path::Path,
};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Notify;
use tracing::{debug, error};
use url::Url;

use metron::Metron;
use opticon::OptiCon;

mod dispatch;
mod metadata;
mod metron;
mod model;
mod notify;
mod opticon;
mod storage;

use model::*;

use crate::{Error, Result, TopicId, Topition, TxnOffsetCommitRequest, UpdateError, Version};

const APPLICATION_JSON: &str = "application/json";

#[derive(Clone, Debug)]
pub struct DynoStore {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    schemas: Option<Registry>,
    lake: Option<House>,
    watermarks: Arc<Mutex<BTreeMap<Topition, OptiCon<Watermark>>>>,
    meta: OptiCon<Meta>,

    object_store: Arc<DynObjectStore>,

    /// Per-`Topition` notification handles so `fetch_wait` can block until a
    /// concurrent `produce` lands new records, rather than busy-polling.
    /// Notifiers are created lazily on first observe; cleared once the
    /// engine is dropped along with the rest of the `Arc`-shared state.
    produce_notify: Arc<Mutex<BTreeMap<Topition, Arc<Notify>>>>,
}

fn json_content_type() -> Attributes {
    let mut attributes = Attributes::new();
    _ = attributes.insert(
        Attribute::ContentType,
        AttributeValue::from(APPLICATION_JSON),
    );
    attributes
}

impl DynoStore {
    pub fn new(cluster: &str, node: i32, object_store: impl ObjectStore) -> Self {
        Self {
            cluster: cluster.into(),
            node,
            advertised_listener: Url::parse("tcp://127.0.0.1/").unwrap(),
            schemas: None,

            lake: None,

            watermarks: Arc::new(Mutex::new(BTreeMap::new())),
            meta: OptiCon::<Meta>::new(cluster),
            object_store: Arc::new(Cache::new(
                Metron::new(object_store, cluster),
                Duration::from_millis(5_000),
            )),
            produce_notify: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Self {
        Self {
            advertised_listener,
            ..self
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Self {
        Self { schemas, ..self }
    }

    pub fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }

    async fn topic_metadata(&self, topic: &TopicId) -> Result<Option<TopicMetadata>> {
        debug!(?topic);

        self.meta
            .with(&self.object_store, |meta| match topic {
                TopicId::Name(name) => Ok(meta.topics.get(name).cloned()),
                TopicId::Id(id) => {
                    for (_, metadata) in meta.topics.iter() {
                        if &metadata.id == id {
                            return Ok(Some(metadata.clone()));
                        }
                    }

                    Ok(None)
                }
            })
            .await
    }

    fn encode(&self, deflated: deflated::Batch) -> Result<PutPayload> {
        Ok(PutPayload::from(Bytes::from(deflated)))
    }

    fn decode(&self, encoded: Bytes) -> Result<deflated::Batch> {
        debug!(encoded = ?&encoded[..]);
        deflated::Batch::try_from(encoded)
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    async fn get<V>(&self, location: &Path) -> Result<(V, Version)>
    where
        V: DeserializeOwned,
    {
        let get_result = self.object_store.get(location).await?;
        let meta = get_result.meta.clone();

        let payload = get_result
            .bytes()
            .await
            .map_err(Into::into)
            .and_then(|encoded| serde_json::from_reader(&encoded[..]).map_err(Error::from))?;

        Ok((payload, meta.into()))
    }

    async fn put<V>(
        &self,
        location: &Path,
        value: V,
        attributes: Attributes,
        update_version: Option<UpdateVersion>,
    ) -> Result<PutResult, UpdateError<V>>
    where
        V: PartialEq + Serialize + DeserializeOwned + Debug,
    {
        debug!(%location, ?attributes, ?update_version, ?value);

        let options = PutOptions {
            mode: update_version.map_or(PutMode::Create, PutMode::Update),
            attributes,
            ..Default::default()
        };

        let payload = serde_json::to_vec(&value)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        match self
            .object_store
            .put_opts(location, payload, options)
            .await
            .inspect_err(|error| debug!(%location, ?error))
        {
            Ok(put_result) => Ok(put_result),

            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                let (current, version) = self
                    .get(location)
                    .await
                    .inspect_err(|error| error!(%location, ?error))?;

                debug!(%location, ?value, ?current);

                Err(UpdateError::Outdated {
                    current: Box::new(current),
                    version,
                })
            }

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    fn txn_offset_commit_response_error(
        offsets: &TxnOffsetCommitRequest,
        error_code: ErrorCode,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    partition_responses.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(error_code.into()),
                    );
                }
            }

            responses.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.to_string())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }
}

fn object_store_error_name(error: &object_store::Error) -> &'static str {
    match error {
        object_store::Error::Precondition { .. } => "pre_condition",

        object_store::Error::AlreadyExists { .. } => "already_exists",

        object_store::Error::NotModified { .. } => "not_modified",

        object_store::Error::NotFound { .. } => "not_found",

        otherwise => {
            debug!(?otherwise);
            "otherwise"
        }
    }
}

#[cfg(test)]
mod tests;
