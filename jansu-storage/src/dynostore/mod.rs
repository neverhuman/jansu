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
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    fmt::{Debug, Display},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt,
    stream::{BoxStream, TryStreamExt},
};
use jansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Record, deflated, inflated},
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use jansu_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use metadata::Cache;
use object_store::{
    Attribute, AttributeValue, Attributes, CopyOptions, DynObjectStore, GetOptions, GetResult,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion, path::Path,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use opticon::OptiCon;
use rand::{prelude::*, rng};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::Notify;
use tracing::{debug, error, instrument, warn};
use url::Url;
use uuid::Uuid;

mod batch;
mod delegate_compaction;
mod describe;
mod features;
mod fetch;
mod groups;
mod helpers;
mod metadata;
mod metron;
mod opticon;
mod produce;
mod queries;
mod storage_impl;
mod txn;

use metron::Metron;
pub(crate) use metron::object_store_error_name;

use crate::{
    BrokerRegistrationRequest, DEFAULT_OFFSET_RETENTION, Error, GroupDetail, LeaderEpochRecord,
    ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetFetchRecord, OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, TopicId,
    Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version,
};

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

type Group = String;
type Offset = i64;
type Partition = i32;
type ProducerEpoch = i16;
type ProducerId = i64;
type Sequence = i32;
type Topic = String;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Meta {
    producers: BTreeMap<ProducerId, ProducerDetail>,
    topics: BTreeMap<Topic, TopicMetadata>,
    transactions: BTreeMap<String, Txn>,
    /// Leader epoch history: topition → sorted list of (epoch, start_offset).
    #[serde(default)]
    leader_epoch_history: BTreeMap<String, Vec<(i32, i64)>>,
}

impl OptiCon<Meta> {
    fn new(cluster: &str) -> Self {
        Self::path(format!("clusters/{cluster}/meta.json"))
    }
}

impl Meta {
    fn supports_topic_config(name: &str) -> bool {
        matches!(
            name,
            "cleanup.policy"
                | "compression.type"
                | "delete.retention.ms"
                | "file.delete.delay.ms"
                | "flush.messages"
                | "flush.ms"
                | "index.interval.bytes"
                | "max.compaction.lag.ms"
                | "max.message.bytes"
                | "message.downconversion.enable"
                | "message.timestamp.difference.max.ms"
                | "message.timestamp.type"
                | "min.cleanable.dirty.ratio"
                | "min.compaction.lag.ms"
                | "min.insync.replicas"
                | "retention.bytes"
                | "retention.ms"
                | "segment.bytes"
                | "segment.index.bytes"
                | "segment.jitter.ms"
                | "segment.ms"
                | "unclean.leader.election.enable"
        )
    }

    fn validate_topic_configs(configs: Option<&[CreatableTopicConfig]>) -> Result<()> {
        if let Some(configs) = configs {
            for config in configs {
                if !Self::supports_topic_config(config.name.as_str()) {
                    return Err(Error::Api(ErrorCode::InvalidRequest));
                }
            }
        }

        Ok(())
    }

    fn validate_topic_config_changes(changes: &[AlterableConfig]) -> Result<()> {
        for change in changes {
            if !Self::supports_topic_config(change.name.as_str()) {
                return Err(Error::Api(ErrorCode::InvalidRequest));
            }
        }

        Ok(())
    }

    fn record_leader_epoch_boundary(&mut self, topition: &Topition, epoch: i32, start_offset: i64) {
        let key = format!("{}:{}", topition.topic(), topition.partition());
        let history = self.leader_epoch_history.entry(key).or_default();
        let current_epoch = history.iter().map(|(epoch, _)| *epoch).max();

        if current_epoch.is_none_or(|current_epoch| epoch > current_epoch) {
            history.push((epoch, start_offset));
            history.sort_unstable();
            history.dedup_by_key(|(epoch, _)| *epoch);
        }
    }

    fn produced(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<BTreeMap<Topition, TxnProduceOffset>> {
        let Some(txn) = self.transactions.get(transaction_id) else {
            return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
        };

        if txn.producer != producer_id {
            return Err(Error::Api(ErrorCode::UnknownProducerId));
        }

        let Some(txn_detail) = txn.epochs.get(&producer_epoch) else {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        };

        let mut produced = BTreeMap::new();

        for (topic, partitions) in txn_detail.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, produced.insert(tp, *offset_range));
            }
        }

        Ok(produced)
    }

    fn overlapping_transactions(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<Vec<TxnId>> {
        let candidates = self.produced(transaction_id, producer_id, producer_epoch)?;

        let mut overlapping = Vec::new();

        'candidates: for (candidate_id, txn) in self.transactions.iter() {
            for (epoch, txn_detail) in txn.epochs.iter() {
                if transaction_id == candidate_id
                    && producer_id == txn.producer
                    && producer_epoch == *epoch
                {
                    continue;
                }

                let Some(state) = txn_detail.state else {
                    continue;
                };

                for (topic, partitions) in txn_detail.produces.iter() {
                    for (partition, offset_range) in partitions.iter() {
                        let Some(offset_range) = offset_range else {
                            continue;
                        };

                        let tp = Topition::new(topic.to_owned(), *partition);

                        if let Some(candidate) = candidates.get(&tp)
                            && offset_range.offset_start < candidate.offset_end
                        {
                            overlapping.push(TxnId {
                                transaction: candidate_id.to_owned(),
                                producer_id: txn.producer,
                                producer_epoch: *epoch,
                                state,
                            });

                            continue 'candidates;
                        }
                    }
                }
            }
        }

        Ok(overlapping)
    }

    fn alter_topic(&mut self, topic: &str, changes: &[AlterableConfig]) -> Result<()> {
        if let Some(metadata) = self.topics.get_mut(topic) {
            Self::validate_topic_config_changes(changes)?;

            let mut configuration = metadata
                .topic
                .configs
                .iter()
                .flat_map(|configs| configs.iter())
                .fold(BTreeMap::new(), |mut acc, item| {
                    _ = acc.insert(item.name.clone(), item.value.clone());
                    acc
                });

            for change in changes {
                match OpType::try_from(change.config_operation)? {
                    OpType::Set => {
                        _ = configuration.insert(change.name.clone(), change.value.clone());
                    }
                    OpType::Delete => {
                        _ = configuration.remove(change.name.as_str());
                    }
                    OpType::Append => {
                        if let Some(new_val) = &change.value {
                            let mut list = configuration
                                .get(change.name.as_str())
                                .and_then(|v| v.as_deref())
                                .map(|s| {
                                    s.split(',')
                                        .map(str::trim)
                                        .filter(|s| !s.is_empty())
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or(Vec::new());

                            if !list.contains(&new_val.as_str()) {
                                list.push(new_val.as_str());
                            }

                            _ = configuration.insert(change.name.clone(), Some(list.join(",")));
                        }
                    }
                    OpType::Subtract => {
                        if let Some(del_val) = &change.value {
                            let list = configuration
                                .get(change.name.as_str())
                                .and_then(|v| v.as_deref())
                                .map(|s| {
                                    s.split(',')
                                        .map(str::trim)
                                        .filter(|s| !s.is_empty() && *s != del_val.as_str())
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or(Vec::new());

                            if list.is_empty() {
                                _ = configuration.remove(change.name.as_str());
                            } else {
                                _ = configuration.insert(change.name.clone(), Some(list.join(",")));
                            }
                        }
                    }
                }
            }

            _ = metadata
                .topic
                .configs
                .replace(
                    configuration
                        .into_iter()
                        .fold(Vec::new(), |mut acc, (key, value)| {
                            acc.push(CreatableTopicConfig::default().name(key).value(value));
                            acc
                        }),
                );
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct ProducerDetail {
    sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnId {
    transaction: String,
    producer_id: ProducerId,
    producer_epoch: ProducerEpoch,
    state: TxnState,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Txn {
    producer: ProducerId,
    epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnDetail {
    transaction_timeout_ms: i32,
    started_at: Option<SystemTime>,
    state: Option<TxnState>,
    produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}

impl From<&TxnDetail> for BTreeMap<Topition, Offset> {
    fn from(value: &TxnDetail) -> Self {
        let mut result = BTreeMap::new();

        for (topic, partitions) in value.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, result.insert(tp, offset_range.offset_start));
            }
        }

        result
    }
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
struct TxnProduceOffset {
    offset_start: Offset,
    offset_end: Offset,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(default)]
struct TxnCommitOffset {
    committed_offset: Offset,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
    commit_timestamp: Option<SystemTime>,
    expires_at: Option<SystemTime>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicMetadata {
    id: Uuid,
    topic: CreatableTopic,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Watermark {
    low: Option<i64>,
    high: Option<i64>,
    timestamps: Option<BTreeMap<i64, i64>>,
}

impl OptiCon<Watermark> {
    fn new(cluster: &str, topition: &Topition) -> Self {
        Self::path(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/watermark.json",
            cluster, topition.topic, topition.partition,
        ))
    }
}

fn json_content_type() -> Attributes {
    let mut attributes = Attributes::new();
    _ = attributes.insert(
        Attribute::ContentType,
        AttributeValue::from(APPLICATION_JSON),
    );
    attributes
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn range_check() {
        let map = BTreeMap::from([(3, "a"), (5, "b"), (8, "c")]);

        assert_eq!(Some((&3, &"a")), map.range(2..).next());
        assert_eq!(Some((&5, &"b")), map.range(4..).next());
        assert_eq!(None, map.range(9..).next());
    }

    #[test]
    fn schema_change() -> Result<()> {
        #[derive(
            Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        struct X0 {
            low: Option<i64>,
            high: Option<i64>,
        }

        let low = Some(6);
        let high = Some(66);

        let x0 = X0 { low, high };

        let encoded = serde_json::to_string(&x0)?;

        #[derive(
            Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        struct X1 {
            low: Option<i64>,
            high: Option<i64>,
            timestamps: Option<BTreeMap<i64, i64>>,
        }

        let x1: X1 = serde_json::from_str(&encoded[..])?;

        assert_eq!(low, x1.low);
        assert_eq!(high, x1.high);
        assert!(x1.timestamps.is_none());

        Ok(())
    }
}
