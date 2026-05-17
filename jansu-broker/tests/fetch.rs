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

use std::collections::BTreeMap;

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, init_tracing, register_broker};
use jansu_broker::Result;
use jansu_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel, ListOffset, NULL_TOPIC_ID,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, inflated},
};
use jansu_storage::{FetchService, ListOffsetResponse, Storage, Topition};
use rama::{Context, Service};
use rand::{prelude::*, rng};
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn empty_topic<G>(cluster_id: Uuid, broker_id: i32, sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let record_count = 0;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let ctx = Context::with_state(sc);

    let fetch = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    let partitions = fetch.responses.as_deref().unwrap_or_default()[0]
        .partitions
        .as_deref()
        .unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    let current_leader = partitions[0]
        .current_leader
        .as_ref()
        .expect("current leader");
    assert_eq!(broker_id, current_leader.leader_id);
    assert_eq!(0, current_leader.leader_epoch);

    for response in fetch.responses.as_ref().unwrap_or(&vec![]) {
        for partition in response.partitions.as_ref().unwrap_or(&vec![]) {
            for batch in &partition.records.as_ref().unwrap().batches {
                assert_eq!(-1, batch.producer_id);
                assert_eq!(-1, batch.producer_epoch);
            }
        }
    }

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

#[cfg(feature = "redlinedb")]
#[tokio::test]
async fn stale_and_future_leader_epochs_are_fenced_exactly() -> Result<()> {
    let _guard = init_tracing()?;

    let storage_path = "phase08-fetch-leader-epoch.db";
    let _ = std::fs::remove_file(storage_path);
    let storage_url = Url::parse(&format!("redlinedb://{storage_path}"))?;

    let cluster_id = Uuid::now_v7().to_string();
    let broker_id = rng().random_range(0..i32::MAX);
    let topic_name = alphanumeric_string(15);

    let sc = common::storage_container(
        StorageType::RedlineDb,
        cluster_id.clone(),
        broker_id,
        storage_url,
        None,
    )
    .await?;
    register_broker(cluster_id.clone(), broker_id, sc.clone()).await?;
    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name.clone(), 0);
    for (value, leader_epoch) in [("epoch-0", 0), ("epoch-1", 1)] {
        let batch = inflated::Batch::builder()
            .partition_leader_epoch(leader_epoch)
            .record(Record::builder().value(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .build()
            .and_then(TryInto::try_into)?;

        _ = sc.produce(None, &topition, batch).await?;
    }

    let fetch_partition = |current_leader_epoch| {
        FetchPartition::default()
            .partition(0)
            .current_leader_epoch(Some(current_leader_epoch))
            .fetch_offset(0)
            .last_fetched_epoch(Some(-1))
            .log_start_offset(Some(-1))
            .partition_max_bytes(50 * 1024)
            .replica_directory_id(None)
    };
    let request = |current_leader_epoch| {
        FetchRequest::default()
            .max_wait_ms(500)
            .min_bytes(1)
            .max_bytes(Some(50 * 1024))
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .topics(Some(
                [FetchTopic::default()
                    .topic(Some(topic_name.clone()))
                    .topic_id(Some(NULL_TOPIC_ID))
                    .partitions(Some(vec![fetch_partition(current_leader_epoch)]))]
                .into(),
            ))
    };

    let response = FetchService
        .serve(Context::with_state(sc.clone()), request(-1))
        .await?;
    let partition = response.responses.as_deref().unwrap()[0]
        .partitions
        .as_deref()
        .unwrap()[0]
        .clone();
    assert_eq!(i16::from(ErrorCode::None), partition.error_code);
    assert_eq!(1, partition.current_leader.unwrap().leader_epoch);

    let response = FetchService
        .serve(Context::with_state(sc.clone()), request(0))
        .await?;
    let partition = response.responses.as_deref().unwrap()[0]
        .partitions
        .as_deref()
        .unwrap()[0]
        .clone();
    assert_eq!(
        i16::from(ErrorCode::FencedLeaderEpoch),
        partition.error_code
    );
    assert_eq!(1, partition.current_leader.unwrap().leader_epoch);

    let response = FetchService
        .serve(Context::with_state(sc), request(2))
        .await?;
    let partition = response.responses.as_deref().unwrap()[0]
        .partitions
        .as_deref()
        .unwrap()[0]
        .clone();
    assert_eq!(
        i16::from(ErrorCode::UnknownLeaderEpoch),
        partition.error_code
    );
    assert_eq!(1, partition.current_leader.unwrap().leader_epoch);

    Ok(())
}

pub async fn simple_non_txn<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(0),
            ..
        }
    ));

    let record_count = 6;

    let mut offset_producer = BTreeMap::new();
    let mut bytes = 0;

    for n in 0..record_count {
        let producer = sc
            .init_producer(None, transaction_timeout_ms, Some(-1), Some(-1))
            .await?;

        let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
        bytes += value.len();

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(value.clone().into()))
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        debug!(n, bytes, ?batch);

        let offset = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;

        debug!(offset);

        assert_eq!(None, offset_producer.insert(offset, (producer, value)));
    }

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(records),
            ..
            } if records == record_count
    ));

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let ctx = Context::with_state(sc);

    let fetch = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    let partitions = fetch.responses.as_deref().unwrap_or_default()[0]
        .partitions
        .as_deref()
        .unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(
        Some(0),
        partitions[0]
            .current_leader
            .as_ref()
            .map(|leader| leader.leader_epoch)
    );

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "redlinedb")]
mod redlinedb {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::RedlineDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::SlateDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
