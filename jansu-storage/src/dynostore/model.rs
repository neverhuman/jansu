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

use std::{collections::BTreeMap, time::SystemTime};

use jansu_sans_io::{
    ErrorCode, OpType,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    incremental_alter_configs_request::AlterableConfig,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::OptiCon;
use crate::{AbortedTransactionRange, Error, Result, Topition, TxnState};

pub(super) type Group = String;
pub(super) type Offset = i64;
pub(super) type Partition = i32;
pub(super) type ProducerEpoch = i16;
pub(super) type ProducerId = i64;
pub(super) type Sequence = i32;
pub(super) type Topic = String;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct Meta {
    pub(super) producers: BTreeMap<ProducerId, ProducerDetail>,
    pub(super) topics: BTreeMap<Topic, TopicMetadata>,
    pub(super) transactions: BTreeMap<String, Txn>,
    /// Leader epoch history: topition → sorted list of (epoch, start_offset).
    #[serde(default)]
    pub(super) leader_epoch_history: BTreeMap<String, Vec<(i32, i64)>>,
    /// Aborted transactional ranges: topition → sorted list of aborted ranges.
    #[serde(default)]
    pub(super) aborted_transaction_ranges: BTreeMap<String, Vec<AbortedTransactionRange>>,
}

impl OptiCon<Meta> {
    pub(super) fn new(cluster: &str) -> Self {
        Self::path(format!("clusters/{cluster}/meta.json"))
    }
}

impl Meta {
    pub(super) fn record_leader_epoch_boundary(
        &mut self,
        topition: &Topition,
        epoch: i32,
        start_offset: i64,
    ) {
        let key = format!("{}:{}", topition.topic(), topition.partition());
        let history = self.leader_epoch_history.entry(key).or_default();
        let current_epoch = history.iter().map(|(epoch, _)| *epoch).max();

        if current_epoch.is_none_or(|current_epoch| epoch > current_epoch) {
            history.push((epoch, start_offset));
            history.sort_unstable();
            history.dedup_by_key(|(epoch, _)| *epoch);
        }
    }

    pub(super) fn aborted_transaction_ranges(
        &self,
        topition: &Topition,
    ) -> Vec<AbortedTransactionRange> {
        let key = format!("{}:{}", topition.topic(), topition.partition());
        let mut ranges = match self.aborted_transaction_ranges.get(&key) {
            Some(ranges) => ranges.clone(),
            None => Vec::new(),
        };

        ranges.sort_unstable();
        ranges.dedup();
        ranges
    }

    pub(super) fn produced(
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

    pub(super) fn overlapping_transactions(
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

    pub(super) fn alter_topic(&mut self, topic: &str, changes: &[AlterableConfig]) -> Result<()> {
        if let Some(metadata) = self.topics.get_mut(topic) {
            let mut configuration = match metadata.topic.configs.as_deref() {
                Some(configs) => configs.iter().fold(BTreeMap::new(), |mut acc, item| {
                    _ = acc.insert(item.name.clone(), item.value.clone());
                    acc
                }),
                None => BTreeMap::new(),
            };

            for change in changes {
                match OpType::try_from(change.config_operation)? {
                    OpType::Set => {
                        _ = configuration.insert(change.name.clone(), change.value.clone());
                    }
                    OpType::Delete => {
                        _ = configuration.remove(change.name.as_str());
                    }
                    OpType::Append => {
                        let current = configuration
                            .get(change.name.as_str())
                            .and_then(|value| value.as_deref());
                        let updated = crate::append_config_tokens(current, change.value.as_deref());
                        _ = configuration.insert(change.name.clone(), updated);
                    }
                    OpType::Subtract => {
                        let current = configuration
                            .get(change.name.as_str())
                            .and_then(|value| value.as_deref());
                        if let Some(updated) =
                            crate::subtract_config_tokens(current, change.value.as_deref())
                        {
                            _ = configuration.insert(change.name.clone(), Some(updated));
                        } else {
                            _ = configuration.remove(change.name.as_str());
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
pub(super) struct ProducerDetail {
    pub(super) sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TxnId {
    pub(super) transaction: String,
    pub(super) producer_id: ProducerId,
    pub(super) producer_epoch: ProducerEpoch,
    pub(super) state: TxnState,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct Txn {
    pub(super) producer: ProducerId,
    pub(super) epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TxnDetail {
    pub(super) transaction_timeout_ms: i32,
    pub(super) started_at: Option<SystemTime>,
    pub(super) state: Option<TxnState>,
    pub(super) produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    pub(super) offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
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
pub(super) struct TxnProduceOffset {
    pub(super) offset_start: Offset,
    pub(super) offset_end: Offset,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(default)]
pub(super) struct TxnCommitOffset {
    pub(super) committed_offset: Offset,
    pub(super) leader_epoch: Option<i32>,
    pub(super) metadata: Option<String>,
    pub(super) commit_timestamp: Option<SystemTime>,
    pub(super) expires_at: Option<SystemTime>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TopicMetadata {
    pub(super) id: Uuid,
    pub(super) topic: CreatableTopic,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct Watermark {
    pub(super) low: Option<i64>,
    pub(super) high: Option<i64>,
    pub(super) timestamps: Option<BTreeMap<i64, i64>>,
}

impl OptiCon<Watermark> {
    pub(super) fn new(cluster: &str, topition: &Topition) -> Self {
        Self::path(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/watermark.json",
            cluster, topition.topic, topition.partition,
        ))
    }
}
