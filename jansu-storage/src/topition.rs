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

//! Topic / partition domain types: [`Topition`], [`TopitionOffset`],
//! [`TopicId`] and related conversions.

use std::{fs::DirEntry, path::PathBuf, str::FromStr};

use jansu_sans_io::{
    ErrorCode, NULL_TOPIC_ID,
    delete_topics_request::DeleteTopicState,
    describe_topic_partitions_request::{Cursor, TopicRequest},
    fetch_request::FetchTopic,
    fetch_response::AbortedTransaction,
    metadata_request::MetadataRequestTopic,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Error;

/// Topic Partition (topition)
///
/// A topic partition pair.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Topition {
    pub(crate) topic: String,
    pub(crate) partition: i32,
}

impl Topition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        let topic = topic.into();
        Self { topic, partition }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
}

/// A recorded leader epoch boundary for a topic partition.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct LeaderEpochRecord {
    pub epoch: i32,
    pub start_offset: i64,
}

/// An aborted transactional offset range for a topic partition.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct AbortedTransactionRange {
    pub producer_id: i64,
    pub offset_start: i64,
    pub offset_end: i64,
}

impl From<&AbortedTransactionRange> for AbortedTransaction {
    fn from(value: &AbortedTransactionRange) -> Self {
        AbortedTransaction::default()
            .producer_id(value.producer_id)
            .first_offset(value.offset_start)
    }
}

impl From<Cursor> for Topition {
    fn from(value: Cursor) -> Self {
        Self {
            topic: value.topic_name,
            partition: value.partition_index,
        }
    }
}

impl TryFrom<&DirEntry> for Topition {
    type Error = Error;

    fn try_from(value: &DirEntry) -> Result<Self, Self::Error> {
        Regex::new(r"^(?<topic>.+)-(?<partition>\d{10})$")
            .map_err(Into::into)
            .and_then(|re| {
                value
                    .file_name()
                    .into_string()
                    .map_err(Error::OsString)
                    .and_then(|ref file_name| {
                        re.captures(file_name)
                            .ok_or(Error::Message(format!("no captures for {file_name}")))
                            .and_then(|ref captures| {
                                let topic = captures
                                    .name("topic")
                                    .ok_or(Error::Message(format!("missing topic for {file_name}")))
                                    .map(|s| s.as_str().to_owned())?;

                                let partition = captures
                                    .name("partition")
                                    .ok_or(Error::Message(format!(
                                        "missing partition for: {file_name}"
                                    )))
                                    .map(|s| s.as_str())
                                    .and_then(|s| str::parse(s).map_err(Into::into))?;

                                Ok(Self { topic, partition })
                            })
                    })
            })
    }
}

impl FromStr for Topition {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        i32::from_str(&s[s.len() - 10..])
            .map(|partition| {
                let topic = String::from(&s[..s.len() - 11]);

                Self { topic, partition }
            })
            .map_err(Into::into)
    }
}

impl From<&Topition> for PathBuf {
    fn from(value: &Topition) -> Self {
        let topic = value.topic.as_str();
        let partition = value.partition;
        PathBuf::from(format!("{topic}-{partition:0>10}"))
    }
}

/// Topic Partition Offset
///
/// A topic partition with an offset.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopitionOffset {
    topition: Topition,
    offset: i64,
}

impl TopitionOffset {
    pub fn new(topition: Topition, offset: i64) -> Self {
        Self { topition, offset }
    }

    pub fn topition(&self) -> &Topition {
        &self.topition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl From<&TopitionOffset> for PathBuf {
    fn from(value: &TopitionOffset) -> Self {
        let offset = value.offset;
        PathBuf::from(value.topition()).join(format!("{offset:0>20}"))
    }
}

/// Topic Id
///
/// An enumeration of either the name or UUID of a topic.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TopicId {
    Name(String),
    Id(Uuid),
}

impl FromStr for TopicId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::Name(s.into()))
    }
}

impl From<&str> for TopicId {
    fn from(value: &str) -> Self {
        Self::Name(value.to_owned())
    }
}

impl From<String> for TopicId {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl From<Uuid> for TopicId {
    fn from(value: Uuid) -> Self {
        Self::Id(value)
    }
}

impl From<[u8; 16]> for TopicId {
    fn from(value: [u8; 16]) -> Self {
        Self::Id(Uuid::from_bytes(value))
    }
}

impl From<&TopicId> for [u8; 16] {
    fn from(value: &TopicId) -> Self {
        match value {
            TopicId::Id(id) => id.into_bytes(),
            TopicId::Name(_) => NULL_TOPIC_ID,
        }
    }
}

impl From<&FetchTopic> for TopicId {
    fn from(value: &FetchTopic) -> Self {
        if let Some(ref id) = value.topic_id
            && id != &NULL_TOPIC_ID
        {
            return Self::Id(Uuid::from_bytes(*id));
        }

        if let Some(ref name) = value.topic {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            panic!("neither name nor uuid")
        }
    }
}

impl From<&MetadataRequestTopic> for TopicId {
    fn from(value: &MetadataRequestTopic) -> Self {
        if let Some(ref name) = value.name {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            panic!("neither name nor uuid")
        }
    }
}

impl From<DeleteTopicState> for TopicId {
    fn from(value: DeleteTopicState) -> Self {
        match value {
            DeleteTopicState {
                name: Some(name),
                topic_id,
                ..
            } if topic_id == NULL_TOPIC_ID => name.into(),

            DeleteTopicState { topic_id, .. } => topic_id.into(),
        }
    }
}

impl From<&TopicRequest> for TopicId {
    fn from(value: &TopicRequest) -> Self {
        value.name.to_owned().into()
    }
}

impl From<&Topition> for TopicId {
    fn from(value: &Topition) -> Self {
        value.topic.to_owned().into()
    }
}

/// Topition (topic partition) Detail
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopitionDetail {
    error: ErrorCode,
    topic: TopicId,
    partitions: Option<Vec<PartitionDetail>>,
}

/// Partition Detail
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PartitionDetail {
    error: ErrorCode,
    partition_index: i32,
}

/// Version representing an `e_tag` and `version` used in conditional writes to an object store.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Version {
    pub(crate) e_tag: Option<String>,
    pub(crate) version: Option<String>,
}

impl From<&Uuid> for Version {
    fn from(value: &Uuid) -> Self {
        Self {
            e_tag: Some(value.to_string()),
            version: None,
        }
    }
}
