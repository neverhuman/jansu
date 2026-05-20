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

use std::{
    collections::BTreeMap,
    env,
    fmt::Debug,
    marker::PhantomData,
    ops::Deref,
    result,
    str::FromStr as _,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, LeaderEpochRecord, ListOffsetRequest,
    ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetFetchRecord, OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, TopicId,
    Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version,
    sql::{Cache, default_hash, idempotent_sequence_check, remove_comments},
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDateTime;
use jansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Header, Record, deflated, inflated},
    to_system_time, to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use jansu_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use tracing::{debug, error};
use turso::{
    Connection, Database, Row, Value, params::IntoParams, transaction::Transaction,
    transaction::TransactionBehavior,
};
use url::Url;
use uuid::Uuid;

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

mod builder;
mod engine;
mod groups;
mod offsets;
mod offsets_fetch;
mod offsets_fetch_records;
mod offsets_list;
mod sql;
mod storage;
#[cfg(test)]
mod tests;
mod timestamp;
mod topics;
mod topics_config;
mod topics_describe;
mod topics_metadata;
mod transactions;
mod txn_end_helper;
mod txn_init;
mod txn_ops;
mod txn_produce;

pub(crate) use builder::Builder;
pub(crate) use engine::Engine;
#[cfg(test)]
use sql::fix_parameters;
use timestamp::LiteTimestamp;
