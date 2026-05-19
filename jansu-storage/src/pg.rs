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

//! PostgreSQL Storage engine

use std::{
    collections::BTreeMap,
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod, Transaction};
use futures::pin_mut;
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
    record::{Header, Record, deflated, inflated::Batch},
    to_system_time, to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use jansu_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use opentelemetry::metrics::Histogram;
use opentelemetry::{KeyValue, metrics::Counter};
use rand::{prelude::*, rng};
use serde_json::Value;
use tokio_postgres::{
    Config, Row, RowStream,
    binary_copy::BinaryCopyInWriter,
    error::SqlState,
    types::{BorrowToSql, ToSql, Type},
};
use tracing::{debug, error, instrument};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, DEFAULT_OFFSET_RETENTION, Error, GroupDetail, LeaderEpochRecord,
    ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetFetchRecord, OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, TopicId,
    Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version,
    sql::{default_hash, idempotent_sequence_check},
};

/// PostgreSQL Storage Engine
#[derive(Clone, Debug)]
pub struct Postgres {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    pool: Pool,
    schemas: Option<Registry>,
    lake: Option<House>,
}

/// PostgreSQL Storage Builder
#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, P> {
    cluster: C,
    node: N,
    advertised_listener: L,
    pool: P,
    schemas: Option<Registry>,
    lake: Option<House>,
}

impl<C, N, L, P> Builder<C, N, L, P> {
    pub fn cluster(self, cluster: impl Into<String>) -> Builder<String, N, L, P> {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn node(self, node: i32) -> Builder<C, i32, L, P> {
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, P> {
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, P> {
        Self { schemas, ..self }
    }

    pub fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }
}

impl Builder<String, i32, Url, Pool> {
    pub fn build(self) -> Postgres {
        Postgres {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }
}

impl<C, N> FromStr for Builder<C, N, Url, Pool>
where
    C: Default,
    N: Default,
{
    type Err = Error;

    fn from_str(config: &str) -> Result<Self, Self::Err> {
        let pg_config = Config::from_str(config).inspect(|pg_config| debug!(?pg_config))?;

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let root_store = {
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_native_certs::load_native_certs().certs {
                roots.add(cert).inspect_err(|err| debug!(?err))?;
            }
            roots
        };

        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map(|config| config.with_root_certificates(root_store))
        .map(|config| config.with_no_client_auth())?;

        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);

        let mgr = Manager::from_config(pg_config, tls, mgr_config);
        let advertised_listener = Url::parse("tcp://127.0.0.1/")?;

        Pool::builder(mgr)
            .max_size(16)
            .build()
            .map(|pool| Self {
                pool,
                advertised_listener,
                node: N::default(),
                cluster: C::default(),
                schemas: None,
                lake: None,
            })
            .map_err(Into::into)
    }
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
        let name = row
            .try_get::<_, String>(0)
            .inspect_err(|err| error!(?err))?;
        let producer_id = row.try_get::<_, i64>(1).inspect_err(|err| error!(?err))?;
        let producer_epoch = row.try_get::<_, i16>(2).inspect_err(|err| error!(?err))?;
        let status = row
            .try_get::<_, Option<String>>(3)
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

impl Postgres {
    pub fn builder(
        connection: &str,
    ) -> Result<Builder<PhantomData<String>, PhantomData<i32>, Url, Pool>> {
        debug!(connection);
        Builder::from_str(connection)
    }

    async fn connection(&self) -> Result<Object> {
        self.pool.get().await.map_err(Into::into)
    }

    fn sql_lookup(&self, key: &str) -> Result<&str> {
        crate::sql::SQL.get(key)
    }

    async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        tx: &Transaction<'_>,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        if let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "producer_epoch_current_for_producer.sql",
                &[&self.cluster, &deflated.producer_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let current_epoch = row
                .try_get::<_, i16>(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))?;

            let row = self
                .tx_prepare_query_one(
                    tx,
                    "producer_select_for_update.sql",
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                    ],
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

            let sequence = row.try_get::<_, i32>(0).inspect_err(|err| error!(?err))?;

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
                self.tx_prepare_execute(
                    tx,
                    "producer_detail_insert.sql",
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                        &increment,
                    ],
                )
                .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        tx: &Transaction<'_>,
    ) -> Result<(Option<i64>, Option<i64>)> {
        if let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "watermark_select_for_update.sql",
                &[&self.cluster, &topition.topic(), &topition.partition()],
            )
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Ok((
                row.try_get::<_, Option<i64>>(0)
                    .inspect_err(|err| error!(?err))?,
                row.try_get::<_, Option<i64>>(1)
                    .inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    fn attributes_for_error(
        &self,
        nickname: &str,
        error: &tokio_postgres::error::Error,
    ) -> Vec<KeyValue> {
        let mut attributes = vec![
            KeyValue::new("sql", nickname.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        if let Some(db_error) = error.as_db_error() {
            if let Some(schema) = db_error.schema() {
                attributes.push(KeyValue::new("schema", schema.to_owned()));
            }

            if let Some(table) = db_error.table() {
                attributes.push(KeyValue::new("table", table.to_owned()));
            }

            if let Some(constraint) = db_error.constraint() {
                attributes.push(KeyValue::new("constraint", constraint.to_owned()));
            }
        }

        if let Some(code) = error.code() {
            attributes.push(KeyValue::new("code", format!("{code:?}")));
        }

        attributes
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_execute(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        c.execute(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_query(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_query_one(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query_one(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                debug!(?err);
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_query_opt(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query_opt(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_execute(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let stmt_key = sql;
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(stmt_key, ?err))?;

        let execute_start = SystemTime::now();

        tx.execute(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query_one(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query_one(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query_opt(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query_opt(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query_raw<P, I>(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: I,
    ) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        tx.query_raw(&prepared, params).await.map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn maybe_record_leader_epoch_boundary(
        &self,
        topition: &Topition,
        epoch: i32,
        start_offset: i64,
        tx: &Transaction<'_>,
    ) -> Result<()> {
        let topic = topition.topic();
        let partition = topition.partition();

        let rows = self
            .tx_prepare_query(
                tx,
                "leader_epoch_history.sql",
                &[&self.cluster, &topic, &partition],
            )
            .await?;

        let current_epoch = rows
            .iter()
            .map(|row| row.try_get::<_, Option<i32>>(0))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .max();

        if current_epoch.is_none_or(|current| epoch > current) {
            _ = self
                .tx_prepare_execute(
                    tx,
                    "leader_epoch_history_insert.sql",
                    &[&self.cluster, &topic, &partition, &epoch, &start_offset],
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
        tx: &Transaction<'_>,
    ) -> Result<i64> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?topition, ?deflated);

        let topic = topition.topic();
        let partition = topition.partition();

        let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "topition_select_id.sql",
                &[&self.cluster, &topic, &partition],
            )
            .await
            .inspect_err(|err| debug!(?err))?
        else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        let topition_id = row.try_get::<_, i32>(0).inspect_err(|err| error!(?err))?;
        debug!(topition_id);

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, tx)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        let (low, high) = self.watermark_select_for_update(topition, tx).await?;

        debug!(?low, ?high);

        let batch_leader_epoch = deflated.partition_leader_epoch;
        let append_start_offset = high.unwrap_or(0);

        self.maybe_record_leader_epoch_boundary(
            topition,
            batch_leader_epoch,
            append_start_offset,
            tx,
        )
        .await?;

        let inflated = Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

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
            {
                let record_sink = tx.copy_in(self.sql_lookup("record_copy.sql")?).await?;

                let record_column_types = [
                    Type::INT4,
                    Type::INT8,
                    Type::INT2,
                    Type::INT8,
                    Type::INT2,
                    Type::TIMESTAMPTZ,
                    Type::BYTEA,
                    Type::BYTEA,
                ];

                let record_writer = BinaryCopyInWriter::new(record_sink, &record_column_types);
                pin_mut!(record_writer);

                for (delta, record) in inflated.records.iter().enumerate() {
                    let delta = i64::try_from(delta)?;
                    let offset = high.unwrap_or(0) + delta;
                    let attributes = inflated.attributes;
                    let key = record.key.as_deref();
                    let value = record.value.as_deref();

                    let producer_id = transaction_id.and(Some(inflated.producer_id));
                    let producer_epoch = transaction_id.and(Some(inflated.producer_epoch));
                    let ts = to_system_time(inflated.base_timestamp + record.timestamp_delta)?;

                    let mut row: Vec<&(dyn ToSql + Sync)> =
                        Vec::with_capacity(record_column_types.len());

                    row.push(&topition_id);
                    row.push(&offset);
                    row.push(&attributes);
                    row.push(&producer_id);
                    row.push(&producer_epoch);
                    row.push(&ts);
                    row.push(&key);
                    row.push(&value);

                    record_writer
                        .as_mut()
                        .write(&row)
                        .await
                        .inspect_err(|err| {
                            error!(?err, ?topic, ?partition, ?offset, ?key, ?value)
                        })?;
                }

                _ = record_writer
                    .finish()
                    .await
                    .inspect(|record_row_count| debug!(?record_row_count))
                    .inspect_err(|err| error!(?err))?;
            }

            {
                let header_sink = tx.copy_in(self.sql_lookup("header_copy.sql")?).await?;
                let header_column_types = [Type::INT4, Type::INT8, Type::BYTEA, Type::BYTEA];
                let header_writer = BinaryCopyInWriter::new(header_sink, &header_column_types);
                pin_mut!(header_writer);

                for (delta, record) in inflated.records.iter().enumerate() {
                    let delta = i64::try_from(delta)?;
                    let offset = high.unwrap_or(0) + delta;

                    for header in record.headers.iter().as_ref() {
                        let key = header.key.as_deref();
                        let value = header.value.as_deref();

                        let mut row: Vec<&(dyn ToSql + Sync)> =
                            Vec::with_capacity(header_column_types.len());

                        row.push(&topition_id);
                        row.push(&offset);
                        row.push(&key);
                        row.push(&value);

                        header_writer
                            .as_mut()
                            .write(&row)
                            .await
                            .inspect_err(|err| {
                                error!(?err, ?topic, ?partition, ?offset, ?key, ?value)
                            })?;
                    }
                }

                _ = header_writer
                    .finish()
                    .await
                    .inspect(|header_row_count| debug!(?header_row_count))
                    .inspect_err(|err| error!(?err))?;
            }

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                let offset_start = high.unwrap_or(0);
                let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

                _ = self
                .tx_prepare_execute(tx,
                    "txn_produce_offset_insert.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &inflated.producer_id,
                        &inflated.producer_epoch,
                        &topic,
                        &partition,
                        &offset_start,
                        &offset_end,
                    ],
                )
                .await
                .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                .inspect_err(|err| error!(?err))?;
            }
        }

        _ = self
            .tx_prepare_execute(
                tx,
                "watermark_update.sql",
                &[
                    &self.cluster,
                    &topic,
                    &partition,
                    &low.unwrap_or(0),
                    &high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ],
            )
            .await
            .inspect(|n| debug!(?n))
            .inspect_err(|err| error!(?err))?;

        self.lake_store(&attributes, topition, high, &inflated)
            .await?;

        Ok(high.unwrap_or(0))
    }

    #[instrument(skip_all)]
    async fn end_in_tx(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        tx: &Transaction<'_>,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let mut overlaps = vec![];

        let rows = self
            .tx_prepare_query(
                tx,
                "txn_select_produced_topitions.sql",
                &[
                    &self.cluster,
                    &transaction_id,
                    &producer_id,
                    &producer_epoch,
                ],
            )
            .await?;

        for row in rows {
            let topic = row.try_get::<_, String>(0)?;
            let partition = row.try_get::<_, i32>(1)?;

            let topition = Topition::new(topic.clone(), partition);

            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };
            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = Batch::builder()
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
                .produce_in_tx(Some(transaction_id), &topition, batch, tx)
                .await?;

            debug!(offset, ?topition);

            let row = self
                .tx_prepare_query_one(
                    tx,
                    "txn_produce_offset_select_offset_range.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &topic,
                        &partition,
                    ],
                )
                .await?;

            let offset_start = row.try_get::<_, i64>(0)?;
            let offset_end = row.try_get::<_, i64>(1)?;
            debug!(offset_start, offset_end);

            let rows = self
                .tx_prepare_query(
                    tx,
                    "txn_produce_offset_select_overlapping_txn.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &topic,
                        &partition,
                        &offset_end,
                    ],
                )
                .await?;

            for row in rows {
                overlaps.push(Txn::try_from(row).inspect(|txn| debug!(?txn))?);
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

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_produce_offset_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_topition_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    let expires_at = SystemTime::now().checked_add(DEFAULT_OFFSET_RETENTION);

                    _ = self
                        .tx_prepare_execute(
                            tx,
                            "consumer_offset_insert_from_txn.sql",
                            &[
                                &self.cluster,
                                &txn.name,
                                &txn.producer_id,
                                &txn.producer_epoch,
                                &expires_at,
                            ],
                        )
                        .await?;
                }

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_offset_commit_tp_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_offset_commit_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_status_update.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                            &outcome,
                        ],
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

            _ = self
                .tx_prepare_execute(
                    tx,
                    "txn_status_update.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &outcome,
                    ],
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

    #[instrument(skip_all)]
    async fn lake_store(
        &self,
        attributes: &BatchAttribute,
        topition: &Topition,
        high: Option<i64>,
        inflated: &Batch,
    ) -> Result<()> {
        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await?;

            lake.store(
                topition.topic(),
                topition.partition(),
                high.unwrap_or(0),
                inflated,
                config,
            )
            .await?;
        }

        Ok(())
    }

    #[instrument(skip(self), ret)]
    async fn policy_compact(&self) -> Result<u64> {
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let compacted = self
            .tx_prepare_execute(&tx, "policy_compact.sql", &[&self.cluster])
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(compacted))
    }

    #[instrument(skip(self), ret)]
    async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let retention_secs = i32::try_from(Duration::from_hours(7 * 24).as_secs())?;

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let deleted = self
            .tx_prepare_execute(
                &tx,
                "policy_delete.sql",
                &[&self.cluster, &now, &retention_secs],
            )
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(deleted))
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

        let row = self
            .prepare_query_one(
                &c,
                "virtual_topic_upsert.sql",
                &[&self.cluster, &topic, &key.as_bytes(), &uuid],
            )
            .await?;

        row.try_get::<_, Uuid>(0)
            .inspect_err(|err| error!(?err))
            .map_err(Into::into)
            .inspect(|vt| debug!(%vt))
    }
}

mod storage_dispatch;

static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_sql_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
        .build()
});

static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_sql_requests")
        .with_description("The number of SQL requests made")
        .build()
});

static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_sql_error")
        .with_description("The SQL error count")
        .build()
});
