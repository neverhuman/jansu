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
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Debug,
    marker::PhantomData,
    ops::Deref,
    path::PathBuf,
    result,
    str::FromStr,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use crate::{
    BrokerRegistrationRequest, ChannelRequestLayer, DEFAULT_OFFSET_RETENTION, Error, GroupDetail,
    LeaderEpochRecord, ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetFetchRecord, OffsetStage, ProducerIdResponse, RequestChannelService,
    RequestStorageService, Result, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version, bounded_channel,
    proxy::SemaphoreProxy,
    sql::{Cache, default_hash, idempotent_sequence_check, remove_comments},
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDateTime;
use deadpool::managed;
use jansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType, ScramMechanism,
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
use libsql::{
    Connection, Database, Row, Rows, Statement, Transaction, TransactionBehavior, Value,
    ffi::SQLITE_CONSTRAINT_UNIQUE, params::IntoParams,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rama::{Context, Layer as _, Service as _};
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use tokio::{fs::rename, sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, warn};
use url::Url;
use uuid::Uuid;

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sqlite_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
        .build()
});

static CONNECT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sqlite_connect_duration")
        .with_unit("ms")
        .with_description("The connection latencies in milliseconds")
        .build()
});

static PRODUCE_IN_TX_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sqlite_produce_in_tx_duration")
        .with_unit("ms")
        .with_description("The produce in TX latencies in milliseconds")
        .build()
});

static TRANSACTION_WITH_BEHAVIOR_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sqlite_transaction_with_behavior_duration")
        .with_unit("ms")
        .with_description("The transaction with behavior latencies in milliseconds")
        .build()
});

static TRANSACTION_COMMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sqlite_transaction_commit_duration")
        .with_unit("ms")
        .with_description("The transaction commit latencies in milliseconds")
        .build()
});

static ENGINE_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sqlite_engine_request_duration")
        .with_unit("ms")
        .with_description("The engine latencies in milliseconds")
        .build()
});

static DELEGATE_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sqlite_delegate_request_duration")
        .with_boundaries(
            [
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0,
                1000.0,
            ]
            .into(),
        )
        .with_unit("ms")
        .with_description("The engine latencies in milliseconds")
        .build()
});

static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_sqlite_requests")
        .with_description("The number of SQL requests made")
        .build()
});

static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_sqlite_error")
        .with_description("The SQL error count")
        .build()
});

fn elapsed_millis(start: SystemTime) -> u64 {
    start
        .elapsed()
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Txn {
    name: String,
    producer_id: i64,
    producer_epoch: i16,
    status: TxnState,
}

impl TryFrom<Row> for Txn {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let name = row.get::<String>(0).inspect_err(|err| error!(?err))?;
        let producer_id = row.get::<i64>(1).inspect_err(|err| error!(?err))?;
        let producer_epoch = row.get::<i32>(2).inspect_err(|err| error!(?err))? as i16;
        let status = row
            .get::<Option<String>>(3)
            .map_err(Into::into)
            .and_then(|status| status.map_or(Ok(TxnState::Begin), TxnState::try_from))
            .inspect_err(|err| error!(?err))?;

        Ok(Self {
            name,
            producer_id,
            producer_epoch,
            status,
        })
    }
}

fn is_unique_constraint(error: &libsql::Error) -> bool {
    matches!(
        error,
        libsql::Error::SqliteFailure(SQLITE_CONSTRAINT_UNIQUE, _)
    )
}

fn value_to_system_time(value: Value) -> Result<Option<SystemTime>> {
    match value {
        Value::Null => Ok(None),
        other => LiteTimestamp::try_from(other)
            .map(SystemTime::from)
            .map(Some),
    }
}

/// LibSQL/SQLite storage engine
///
#[derive(Clone, Debug)]
pub(crate) struct Delegate {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    pool: Pool,

    schemas: Option<Registry>,

    lake: Option<House>,

    vacuum_into: Option<PathBuf>,

    maintenance: Arc<Semaphore>,
    compaction: CompactionMode,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionManager {
    db: Arc<Mutex<Database>>,
    busy_timeout: Duration,
}

pub(crate) struct PoolConnection {
    connection: Connection,
}

mod connection;
use builder::CompactionMode;
use connection::Pool;

impl Delegate {
    #[instrument(skip_all)]
    async fn connection(&self) -> Result<managed::Object<ConnectionManager>> {
        let start = SystemTime::now();

        self.pool.get().await.map_err(Into::into).inspect(|_| {
            CONNECT_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            )
        })
    }

    #[instrument(skip_all)]
    async fn idempotent_message_check(
        &self,
        _transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &PoolConnection,
    ) -> Result<()> {
        let mut rows = connection
            .query(
                "producer_epoch_current_for_producer.sql",
                (self.cluster.as_str(), deflated.producer_id),
            )
            .await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            let current_epoch = row
                .get::<i32>(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))?
                as i16;

            let row = connection
                .query_one(
                    "producer_select_for_update.sql",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                    ),
                )
                .await
                .inspect_err(|err| {
                    error!(
                        self.cluster,
                        ?topition,
                        deflated.producer_id,
                        deflated.producer_epoch,
                        ?err
                    )
                })?;

            let sequence = row.get::<i32>(0).inspect_err(|err| error!(?err))?;

            debug!(
                self.cluster,
                ?topition,
                deflated.producer_id,
                deflated.producer_epoch,
                current_epoch,
                sequence,
            );

            let increment = idempotent_sequence_check(&current_epoch, &sequence, deflated)?;

            debug!(increment);

            assert_eq!(
                1,
                connection
                    .execute(
                        "producer_detail_insert.sql",
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            deflated.producer_id,
                            deflated.producer_epoch,
                            increment,
                        ),
                    )
                    .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    #[instrument(skip_all)]
    async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        connection: &PoolConnection,
    ) -> Result<(Option<i64>, Option<i64>)> {
        debug!(?topition);

        let mut rows = connection
            .query(
                "watermark_select_no_update.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        if let Some(row) = rows
            .next()
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Ok((
                row.get::<Option<i64>>(0).inspect_err(|err| error!(?err))?,
                row.get::<Option<i64>>(1).inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    #[instrument(skip_all)]
    async fn maybe_record_leader_epoch_boundary(
        &self,
        topition: &Topition,
        epoch: i32,
        start_offset: i64,
        connection: &PoolConnection,
    ) -> Result<()> {
        let mut rows = connection
            .query(
                "leader_epoch_history.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        let mut current_epoch: Option<i32> = None;
        while let Some(row) = rows.next().await? {
            let row_epoch = row.get::<i32>(0).inspect_err(|err| error!(?err))?;
            current_epoch = Some(current_epoch.map_or(row_epoch, |current| current.max(row_epoch)));
        }

        if current_epoch.is_none_or(|current| epoch > current) {
            _ = connection
                .execute(
                    "leader_epoch_history_insert.sql",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        epoch,
                        start_offset,
                    ),
                )
                .await
                .inspect_err(|err| error!(?err, ?topition, epoch, start_offset))?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn produce_in_tx(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        connection: &PoolConnection,
    ) -> Result<i64> {
        let start = SystemTime::now();

        let topic = topition.topic();
        let partition = topition.partition();

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, connection)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        debug!(after_idempotent_check = elapsed_millis(start));

        let (low, high) = self
            .watermark_select_for_update(topition, connection)
            .await
            .inspect_err(|err| error!(?err))?;

        debug!(after_watermark_select_for_update = elapsed_millis(start));

        debug!(?low, ?high);

        let batch_leader_epoch = deflated.partition_leader_epoch;
        let append_start_offset = high.unwrap_or(0);

        self.maybe_record_leader_epoch_boundary(
            topition,
            batch_leader_epoch,
            append_start_offset,
            connection,
        )
        .await?;

        let inflated = inflated::Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        debug!(after_inflate = elapsed_millis(start));

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

        debug!(after_attributes = elapsed_millis(start));

        if !attributes.control
            && let Some(ref schemas) = self.schemas
            && self
                .describe_config(topic, ConfigResource::Topic, None)
                .await
                .map(|resources| {
                    resources
                        .configs
                        .as_ref()
                        .and_then(|configs| {
                            configs
                                .iter()
                                .inspect(|config| debug!(?config))
                                .find(|config| config.name.as_str() == "jansu.schema.validation")
                                .and_then(|config| config.value.as_deref())
                                .and_then(|value| bool::from_str(value).ok())
                        })
                        .unwrap_or(true)
                })
                .inspect(|schema_validation| debug!(schema_validation))?
        {
            schemas.validate(topition.topic(), &inflated).await?;
        }

        debug!(after_validation = elapsed_millis(start));

        let last_offset_delta = i64::from(inflated.last_offset_delta);

        if self.schemas.is_none()
            || self.lake.is_none()
            || (self.lake.is_some()
                && !self
                    .describe_config(topic, ConfigResource::Topic, None)
                    .await
                    .inspect(|resources| debug!(?resources))
                    .map(|resources| {
                        resources
                            .configs
                            .as_ref()
                            .and_then(|configs| {
                                configs
                                    .iter()
                                    .inspect(|config| debug!(?config))
                                    .find(|config| config.name.as_str() == "jansu.lake.sink")
                                    .and_then(|config| config.value.as_deref())
                                    .and_then(|value| bool::from_str(value).ok())
                            })
                            .unwrap_or(false)
                    })
                    .inspect(|jansu_lake_sink| debug!(jansu_lake_sink))?)
        {
            for (delta, record) in inflated.records.iter().enumerate() {
                debug!(delta, elapsed = elapsed_millis(start));

                let delta = i64::try_from(delta)?;
                let offset = high.unwrap_or(0) + delta;
                let key = record.key.as_deref();
                let value = record.value.as_deref();

                debug!(?delta, ?offset);

                _ = connection
                    .execute(
                        "record_insert.sql",
                        (
                            self.cluster.as_str(),
                            topic,
                            partition,
                            offset,
                            inflated.attributes,
                            if transaction_id.is_none() {
                                None
                            } else {
                                Some(inflated.producer_id)
                            },
                            if transaction_id.is_none() {
                                None
                            } else {
                                Some(inflated.producer_epoch)
                            },
                            inflated.base_timestamp + record.timestamp_delta,
                            key,
                            value,
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, ?topic, ?partition, ?offset, ?key, ?value))
                    .map_err(unique_constraint(ErrorCode::UnknownServerError))?;

                debug!(delta, after_record_insert = elapsed_millis(start));

                for header in record.headers.iter().as_ref() {
                    let key = header.key.as_deref();
                    let value = header.value.as_deref();

                    _ = connection
                        .execute(
                            "header_insert.sql",
                            (self.cluster.as_str(), topic, partition, offset, key, value),
                        )
                        .await
                        .inspect_err(|err| {
                            error!(?err, ?topic, ?partition, ?offset, ?key, ?value);
                        });
                }

                debug!(delta, after_header_insert = elapsed_millis(start));
            }

            debug!(after_record_insert = elapsed_millis(start));

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                let offset_start = high.unwrap_or(0);
                let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

                _ = connection
                        .execute(
                            "txn_produce_offset_insert.sql",
                            (
                                self.cluster.as_str(),
                                transaction_id,
                                inflated.producer_id,
                                inflated.producer_epoch,
                                topic,
                                partition,
                                offset_start,
                                offset_end,
                            ),
                        )
                        .await
                        .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                        .inspect_err(|err| error!(?err))?;
            }

            debug!(after_some_transaction_id = elapsed_millis(start));
        }

        _ = connection
            .execute(
                "watermark_update.sql",
                (
                    self.cluster.as_str(),
                    topic,
                    partition,
                    low.unwrap_or(0),
                    high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ),
            )
            .await
            .inspect(|n| debug!(?n, after_watermark_update = elapsed_millis(start)))
            .inspect_err(|err| error!(?err))?;

        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await
                .inspect_err(|err| error!(?err))?;

            lake.store(
                topition.topic(),
                topition.partition(),
                high.unwrap_or(0),
                &inflated,
                config,
            )
            .await
            .inspect_err(|err| error!(?err))?;
        }

        debug!(after_all_done = elapsed_millis(start));

        Ok(high.unwrap_or(0)).inspect(|_| {
            PRODUCE_IN_TX_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            );
        })
    }

    async fn end_in_tx(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        connection: &PoolConnection,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let mut overlaps = vec![];

        let mut rows = connection
            .query(
                "txn_select_produced_topitions.sql",
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                ),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let topic = row.get::<String>(0)?;
            let partition = row.get::<i32>(1)?;

            let topition = Topition::new(topic.clone(), partition);

            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };
            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = inflated::Batch::builder()
                .record(
                    Record::builder()
                        .key(control_batch.into())
                        .value(end_transaction_marker.into()),
                )
                .attributes(
                    BatchAttribute::default()
                        .control(true)
                        .transaction(true)
                        .into(),
                )
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .base_sequence(-1)
                .build()
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?;

            let offset = self
                .produce_in_tx(Some(transaction_id), &topition, batch, connection)
                .await?;

            debug!(offset, ?topition);

            let mut rows = connection
                .query(
                    "txn_produce_offset_select_offset_range.sql",
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        topic.as_str(),
                        partition,
                    ),
                )
                .await?;

            if let Some(row) = rows.next().await? {
                let offset_start = row.get::<i64>(0)?;
                let offset_end = row.get::<i64>(1)?;
                debug!(offset_start, offset_end);

                let mut rows = connection
                    .query(
                        "txn_produce_offset_select_overlapping_txn.sql",
                        (
                            self.cluster.as_str(),
                            transaction_id,
                            producer_id,
                            producer_epoch,
                            topic.as_str(),
                            partition,
                            offset_end,
                        ),
                    )
                    .await?;

                while let Some(row) = rows.next().await? {
                    overlaps.push(Txn::try_from(row).inspect(|txn| debug!(?txn))?);
                }
            }
        }

        if overlaps.iter().all(|txn| txn.status.is_prepared()) {
            let txns = {
                let mut txns = Vec::with_capacity(overlaps.len() + 1);

                txns.append(&mut overlaps);

                txns.push(Txn {
                    name: transaction_id.into(),
                    producer_id,
                    producer_epoch,
                    status: if committed {
                        TxnState::PrepareCommit
                    } else {
                        TxnState::PrepareAbort
                    },
                });

                txns
            };

            debug!(?txns);

            for txn in txns {
                debug!(?txn);

                _ = connection
                    .execute(
                        "txn_produce_offset_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = connection
                    .execute(
                        "txn_topition_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    let expires_at = SystemTime::now().checked_add(DEFAULT_OFFSET_RETENTION);

                    _ = connection
                        .execute(
                            "consumer_offset_insert_from_txn.sql",
                            (
                                self.cluster.as_str(),
                                txn.name.as_str(),
                                txn.producer_id,
                                txn.producer_epoch,
                                expires_at.map(LiteTimestamp::from),
                            ),
                        )
                        .await?;
                }

                _ = connection
                    .execute(
                        "txn_offset_commit_tp_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = connection
                    .execute(
                        "txn_offset_commit_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                _ = connection
                    .execute(
                        "txn_status_update.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                            outcome,
                        ),
                    )
                    .await?;
            }
        } else {
            debug!(?overlaps);

            let outcome = if committed {
                String::from(TxnState::PrepareCommit)
            } else {
                String::from(TxnState::PrepareAbort)
            };

            _ = connection
                .execute(
                    "txn_status_update.sql",
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        outcome.as_str(),
                    ),
                )
                .await
                .inspect(|n| {
                    debug!(
                        cluster = self.cluster,
                        transaction_id, producer_id, producer_epoch, outcome, n
                    )
                })?;
        }

        Ok(ErrorCode::None)
    }

    #[instrument(skip(self), ret)]
    async fn policy_compact_delete(&self, topition: i64, offset_id: i64) -> Result<u64> {
        let pc = self.connection().await?;

        pc.execute("lite/policy_compact_delete.sql", (topition, offset_id))
            .await
            .map(|rows| rows as u64)
            .inspect(|rows| debug!(rows))
            .map_err(Into::into)
    }

    #[instrument(skip(self))]
    async fn policy_compact_compaction(
        &self,
        topition: i64,
        key: &[u8],
        max_offset_id: i64,
    ) -> Result<Vec<i64>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query(
                "lite/policy_compact_compaction.sql",
                (topition, key, max_offset_id),
            )
            .await?;

        let mut offsets = Vec::new();

        while let Some(row) = rows.next().await? {
            let offset = row.get::<i64>(0)?;
            offsets.push(offset);
        }

        Ok(offsets)
    }

    #[instrument(skip(self))]
    async fn policy_compact_max_offset_id(&self, topition: i64, key: &[u8]) -> Result<Option<i64>> {
        let pc = self.connection().await?;

        if let Some(row) = pc
            .query_opt("lite/policy_compact_max_offset_id.sql", (topition, key))
            .await?
        {
            row.get::<i64>(0).map(Some).map_err(Into::into)
        } else {
            Ok(None)
        }
        .inspect(|max_offset| debug!(max_offset))
    }

    #[instrument(skip(self))]
    async fn policy_compact_distinct_k(&self, topition: i64) -> Result<BTreeSet<Vec<u8>>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query("lite/policy_compact_distinct_k.sql", [topition])
            .await?;

        let mut keys = BTreeSet::new();

        while let Some(row) = rows.next().await? {
            if let Some(key) = row.get::<Option<Vec<u8>>>(0)? {
                _ = keys.insert(key);
            }
        }

        debug!(keys = keys.len());

        Ok(keys)
    }

    #[instrument(skip(self))]
    async fn policy_compact_topitions(&self) -> Result<BTreeSet<i64>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query("lite/policy_compact_topitions.sql", [self.cluster.as_str()])
            .await?;

        let mut topitions = BTreeSet::new();

        while let Some(row) = rows.next().await? {
            let topition = row.get::<i64>(0)?;
            _ = topitions.insert(topition);
        }

        Ok(topitions)
    }

    #[instrument(skip(self))]
    async fn policy_compact(&self) -> Result<u64> {
        let start = SystemTime::now();

        match self.compaction {
            CompactionMode::Single => {
                let pc = self.connection().await?;

                pc.execute("policy_compact.sql", [self.cluster.as_str()])
                    .await
                    .map(|compacted| compacted as u64)
                    .inspect(|_| {
                        DELEGATE_REQUEST_DURATION.record(
                            elapsed_millis(start),
                            &[KeyValue::new("operation", "policy_compact_single")],
                        )
                    })
                    .map_err(Into::into)
            }
            CompactionMode::Multi => {
                let mut compacted = 0;

                for topition in self.policy_compact_topitions().await? {
                    debug!(topition);

                    for key in self.policy_compact_distinct_k(topition).await? {
                        debug!(key = ?&key[..]);

                        if let Some(max_offset_id) = self
                            .policy_compact_max_offset_id(topition, &key[..])
                            .await?
                        {
                            debug!(max_offset_id);

                            for offset in self
                                .policy_compact_compaction(topition, &key[..], max_offset_id)
                                .await?
                            {
                                debug!(offset);

                                compacted += self.policy_compact_delete(topition, offset).await?;
                            }
                        }
                    }
                }

                Ok(compacted).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "policy_compact_multi")],
                    )
                })
            }
        }
    }

    #[instrument(skip(self))]
    async fn vacuum_into(&self) -> Result<()> {
        if let Some(vacuum_into) = self
            .vacuum_into
            .as_deref()
            .inspect(|vacuum_into| debug!(vacuum_into = vacuum_into.to_str()))
        {
            let mut staging = PathBuf::from(vacuum_into);
            if staging.add_extension("staging") {
                debug!(staging = staging.to_str());

                if let Some(vacuum_staging) = staging.to_str() {
                    let pc = self.connection().await?;
                    let rows = pc.execute("lite/vacuum_into.sql", [vacuum_staging]).await? as u64;

                    rename(staging, vacuum_into).await?;
                    debug!(vacuum_into = vacuum_into.to_str(), rows);
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self, now), ret)]
    async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let start = SystemTime::now();

        let now = to_timestamp(&now)?;
        let retention_ms = u64::try_from(Duration::from_hours(7 * 24).as_millis())?;

        let pc = self.connection().await?;

        pc.execute(
            "lite/policy_delete.sql",
            (self.cluster.as_str(), now, retention_ms),
        )
        .await
        .map(|deleted| deleted as u64)
        .map_err(Into::into)
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "policy_delete")],
            )
        })
    }

    async fn topic_with_key<'a>(&self, topic: &'a str) -> Result<(&'a str, Option<&'a str>)> {
        if let Some((base, key)) = topic.split_once('/')
            && self
                .describe_config(base, ConfigResource::Topic, None)
                .await
                .map(|configs| {
                    configs
                        .configs
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .find_map(|config| {
                            if config.name == "jansu.virtual" {
                                config
                                    .value
                                    .as_deref()
                                    .and_then(|config| bool::from_str(config).ok())
                            } else {
                                None
                            }
                        })
                        .unwrap_or(false)
                })?
        {
            Ok((base, Some(key)))
        } else {
            Ok((topic, None))
        }
    }

    #[instrument(skip(self), ret)]
    async fn base_topic<'a>(&self, topic: &'a str) -> Result<&'a str> {
        self.topic_with_key(topic).await.map(|(topic, _key)| topic)
    }

    #[instrument(skip(self), ret)]
    async fn virtual_topic_id(&self, topic: &str, key: &str) -> Result<Uuid> {
        let uuid = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("tag:jansu.io,2026-04:virtual:{topic}:{key}",).as_bytes(),
        );

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let row = c
            .query_one(
                "virtual_topic_upsert.sql",
                (self.cluster.as_str(), topic, key, uuid.to_string()),
            )
            .await?;

        row.get_str(0)
            .map_err(Error::from)
            .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
            .inspect(|vt| debug!(%vt))
    }
}

#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, D> {
    cluster: C,
    node: N,
    advertised_listener: L,
    storage: D,
    schemas: Option<Registry>,
    lake: Option<House>,
    cancellation: CancellationToken,
}

impl<C, N, L, D> Builder<C, N, L, D> {
    pub(crate) fn cluster<T>(self, cluster: T) -> Builder<String, N, L, D>
    where
        T: Into<String>,
    {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn node(self, node: i32) -> Builder<C, i32, L, D> {
        debug!(node);
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, D> {
        debug!(%advertised_listener);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn storage(self, storage: Url) -> Builder<C, N, L, Url> {
        debug!(%storage);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, D> {
        Self { schemas, ..self }
    }

    pub(crate) fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }

    pub(crate) fn cancellation(self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self
        }
    }
}

static DDL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        ("010-cluster.sql", include_sql!("../ddl/010-cluster.sql")),
        (
            "020-consumer-group.sql",
            include_sql!("../ddl/020-consumer-group.sql"),
        ),
        ("020-producer.sql", include_sql!("../ddl/020-producer.sql")),
        (
            "020-scram-credential.sql",
            include_sql!("../ddl/020-scram-credential.sql"),
        ),
        ("020-topic.sql", include_sql!("../ddl/020-topic.sql")),
        (
            "030-consumer-group-detail.sql",
            include_sql!("../ddl/030-consumer-group-detail.sql"),
        ),
        (
            "030-producer-epoch.sql",
            include_sql!("../ddl/030-producer-epoch.sql"),
        ),
        (
            "030-topic-configuration.sql",
            include_sql!("../ddl/030-topic-configuration.sql"),
        ),
        ("030-topition.sql", include_sql!("../ddl/030-topition.sql")),
        ("030-txn.sql", include_sql!("../ddl/030-txn.sql")),
        (
            "030-virtual-topic.sql",
            include_sql!("../ddl/030-virtual-topic.sql"),
        ),
        (
            "040-consumer-offset.sql",
            include_sql!("../ddl/040-consumer-offset.sql"),
        ),
        ("040-header.sql", include_sql!("../ddl/040-header.sql")),
        (
            "040-producer-detail.sql",
            include_sql!("../ddl/040-producer-detail.sql"),
        ),
        ("040-record.sql", include_sql!("../ddl/040-record.sql")),
        (
            "040-leader-epoch-history.sql",
            include_sql!("../ddl/040-leader-epoch-history.sql"),
        ),
        (
            "040-txn-detail.sql",
            include_sql!("../ddl/040-txn-detail.sql"),
        ),
        (
            "040-watermark.sql",
            include_sql!("../ddl/040-watermark.sql"),
        ),
        (
            "050-txn-offset-commit.sql",
            include_sql!("../ddl/050-txn-offset-commit.sql"),
        ),
        (
            "050-txn-topition.sql",
            include_sql!("../ddl/050-txn-topition.sql"),
        ),
        (
            "060-txn-offset-commit-tp.sql",
            include_sql!("../ddl/060-txn-offset-commit-tp.sql"),
        ),
        (
            "060-txn-produce-offset.sql",
            include_sql!("../ddl/060-txn-produce-offset.sql"),
        ),
    ];

    Cache::new(BTreeMap::from(mapping))
});

pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
    Cache::new(
        crate::sql::SQL
            .iter()
            .map(|(name, sql)| fix_parameters(sql).map(|sql| (*name, sql)))
            .collect::<Result<BTreeMap<_, _>>>()
            .unwrap_or_else(|_| BTreeMap::new()),
    )
});

fn fix_parameters(sql: &str) -> Result<String> {
    Regex::new(r"\$(?<i>\d+)")
        .map(|re| re.replace_all(sql, "?$i").into_owned())
        .map_err(Into::into)
}

#[derive(Clone, Debug)]
pub struct Engine {
    #[allow(dead_code)]
    server: Arc<JoinSet<Result<(), Error>>>,
    inner: RequestChannelService,
}

mod engine;

mod builder;

fn unique_constraint(error_code: ErrorCode) -> impl Fn(libsql::Error) -> Error {
    move |err| {
        if let libsql::Error::SqliteFailure(code, ref reason) = err {
            debug!(code, reason);

            if code == SQLITE_CONSTRAINT_UNIQUE {
                Error::Api(error_code)
            } else {
                err.into()
            }
        } else {
            err.into()
        }
    }
}

mod storage;

mod timestamp;
use timestamp::LiteTimestamp;

#[cfg(test)]
mod tests;
