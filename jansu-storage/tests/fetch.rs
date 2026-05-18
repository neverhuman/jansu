// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

mod common;

#[cfg(feature = "dynostore")]
mod phase08 {
    use bytes::Bytes;
    use jansu_sans_io::{
        FetchRequest, FetchResponse, IsolationLevel, NULL_TOPIC_ID,
        fetch_request::{FetchPartition, FetchTopic},
        record::{Record, inflated},
    };
    use jansu_storage::{FetchService, Storage, Topition};
    use rama::{Context, Service};
    use url::Url;

    use crate::common::{self, Error};

    async fn memory_storage(
        cluster_id: &str,
        node_id: i32,
    ) -> Result<std::sync::Arc<Box<dyn Storage>>, Error> {
        common::build_storage(
            cluster_id,
            node_id,
            Url::parse(&format!("memory://{cluster_id}/"))?,
        )
        .await
    }

    async fn produce_value<S>(storage: &S, topition: &Topition, value: Vec<u8>) -> Result<(), Error>
    where
        S: Storage + ?Sized,
    {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from(value))))
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, topition, batch).await?;
        Ok(())
    }

    fn request(
        topic: &str,
        partition: i32,
        max_wait_ms: i32,
        partition_max_bytes: i32,
        isolation: IsolationLevel,
    ) -> FetchRequest {
        FetchRequest::default()
            .max_wait_ms(max_wait_ms)
            .min_bytes(1)
            .max_bytes(Some(50 * 1024))
            .isolation_level(Some(isolation.into()))
            .topics(Some(
                [FetchTopic::default()
                    .topic(Some(topic.to_owned()))
                    .topic_id(Some(NULL_TOPIC_ID))
                    .partitions(Some(vec![
                        FetchPartition::default()
                            .partition(partition)
                            .current_leader_epoch(Some(-1))
                            .fetch_offset(0)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(partition_max_bytes)
                            .replica_directory_id(None),
                    ]))]
                .into(),
            ))
    }

    fn request_with_topic_id(
        topic: &str,
        topic_id: [u8; 16],
        partition: i32,
        max_wait_ms: i32,
        partition_max_bytes: i32,
        isolation: IsolationLevel,
    ) -> FetchRequest {
        FetchRequest::default()
            .max_wait_ms(max_wait_ms)
            .min_bytes(1)
            .max_bytes(Some(50 * 1024))
            .isolation_level(Some(isolation.into()))
            .topics(Some(
                [FetchTopic::default()
                    .topic(Some(topic.to_owned()))
                    .topic_id(Some(topic_id))
                    .partitions(Some(vec![
                        FetchPartition::default()
                            .partition(partition)
                            .current_leader_epoch(Some(-1))
                            .fetch_offset(0)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(partition_max_bytes)
                            .replica_directory_id(None),
                    ]))]
                .into(),
            ))
    }

    #[tokio::test]
    async fn fetch_returns_without_waiting_when_min_bytes_is_satisfied() -> Result<(), Error> {
        let cluster_id = "phase08-fetch-min-bytes";
        let node_id = 8;
        let topic = "phase08_fetch_min_bytes";
        let partition = 0;
        let storage = memory_storage(cluster_id, node_id).await?;
        common::register_broker(storage.as_ref(), cluster_id, node_id).await?;
        _ = common::create_topic(storage.as_ref(), topic, 1).await?;
        let topition = Topition::new(topic, partition);
        produce_value(storage.as_ref(), &topition, vec![b'a'; 128]).await?;

        let response = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            FetchService.serve(
                Context::with_state(storage),
                request(
                    topic,
                    partition,
                    5_000,
                    50 * 1024,
                    IsolationLevel::ReadUncommitted,
                ),
            ),
        )
        .await
        .expect("fetch should not sleep after satisfying min_bytes")?;

        let records = response.responses.as_deref().unwrap_or_default()[0]
            .partitions
            .as_deref()
            .unwrap_or_default()[0]
            .records
            .as_ref()
            .expect("records");
        assert_eq!(1, records.batches.len());

        Ok(())
    }

    #[tokio::test]
    async fn fetch_enforces_partition_max_bytes() -> Result<(), Error> {
        let cluster_id = "phase08-fetch-partition-max";
        let node_id = 8;
        let topic = "phase08_fetch_partition_max";
        let partition = 0;
        let storage = memory_storage(cluster_id, node_id).await?;
        common::register_broker(storage.as_ref(), cluster_id, node_id).await?;
        _ = common::create_topic(storage.as_ref(), topic, 1).await?;
        let topition = Topition::new(topic, partition);

        for n in 0..3 {
            produce_value(storage.as_ref(), &topition, vec![b'a' + n; 128]).await?;
        }

        let stored = storage
            .fetch(&topition, 0, 1, 50 * 1024, IsolationLevel::ReadUncommitted)
            .await?;
        assert!(stored.len() >= 2);
        assert!(stored[0].record_data.len() <= i32::MAX as usize);
        let partition_max_bytes = stored[0].record_data.len() as i32;

        let response = FetchService
            .serve(
                Context::with_state(storage),
                request(
                    topic,
                    partition,
                    500,
                    partition_max_bytes,
                    IsolationLevel::ReadUncommitted,
                ),
            )
            .await?;

        let records = response.responses.as_deref().unwrap_or_default()[0]
            .partitions
            .as_deref()
            .unwrap_or_default()[0]
            .records
            .as_ref()
            .expect("records");
        assert_eq!(1, records.batches.len());

        Ok(())
    }

    #[tokio::test]
    async fn fetch_prefers_topic_id_when_topic_name_is_stale() -> Result<(), Error> {
        let cluster_id = "phase08-fetch-topic-id";
        let node_id = 8;
        let topic = "phase08_fetch_topic_id";
        let partition = 0;
        let storage = memory_storage(cluster_id, node_id).await?;
        common::register_broker(storage.as_ref(), cluster_id, node_id).await?;
        let topic_id = common::create_topic(storage.as_ref(), topic, 1).await?;
        let topic_id_bytes = topic_id.into_bytes();
        let topition = Topition::new(topic, partition);
        produce_value(storage.as_ref(), &topition, vec![b'c'; 64]).await?;

        let response = FetchService
            .serve(
                Context::with_state(storage),
                request_with_topic_id(
                    "stale-name",
                    topic_id_bytes,
                    partition,
                    5_000,
                    50 * 1024,
                    IsolationLevel::ReadUncommitted,
                ),
            )
            .await?;

        let topic_response = response.responses.as_deref().unwrap_or_default()[0].clone();
        assert_eq!(Some(topic.to_owned()), topic_response.topic);
        assert_eq!(Some(topic_id_bytes), topic_response.topic_id);

        let records = topic_response.partitions.as_deref().unwrap_or_default()[0]
            .records
            .as_ref()
            .expect("records");
        assert_eq!(1, records.batches.len());

        Ok(())
    }

    #[tokio::test]
    async fn fetch_stops_after_global_max_bytes_is_spent() -> Result<(), Error> {
        let cluster_id = "phase08-fetch-max-bytes";
        let node_id = 8;
        let topic = "phase08_fetch_max_bytes";
        let storage = memory_storage(cluster_id, node_id).await?;
        common::register_broker(storage.as_ref(), cluster_id, node_id).await?;
        _ = common::create_topic(storage.as_ref(), topic, 2).await?;
        let left = Topition::new(topic, 0);
        let right = Topition::new(topic, 1);

        produce_value(storage.as_ref(), &left, vec![b'l'; 128]).await?;
        produce_value(storage.as_ref(), &right, vec![b'r'; 128]).await?;

        let stored = storage
            .fetch(&left, 0, 1, 50 * 1024, IsolationLevel::ReadUncommitted)
            .await?;
        let partition_max_bytes = stored
            .first()
            .expect("left partition should have a batch")
            .record_data
            .len() as i32;

        let partition = |partition| {
            FetchPartition::default()
                .partition(partition)
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(partition_max_bytes)
                .replica_directory_id(None)
        };

        let response = FetchService
            .serve(
                Context::with_state(storage),
                FetchRequest::default()
                    .max_wait_ms(500)
                    .min_bytes(1)
                    .max_bytes(Some(partition_max_bytes))
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        vec![FetchTopic::default()
                            .topic(Some(topic.to_owned()))
                            .topic_id(Some(NULL_TOPIC_ID))
                            .partitions(Some(vec![partition(0), partition(1)]))]
                        .into(),
                    )),
            )
            .await?;

        let topic_response = response.responses.as_deref().unwrap_or_default()[0].clone();
        let partitions = topic_response.partitions.as_deref().unwrap_or_default();
        assert_eq!(2, partitions.len());

        let left_records = partitions[0].records.as_ref().expect("left records");
        assert_eq!(1, left_records.batches.len());
        assert!(
            partitions[1].records.is_none(),
            "global max_bytes should prevent a second partition from consuming bytes after the first partition spends the ceiling"
        );

        Ok(())
    }

    fn record_count(fetch: &FetchResponse) -> i64 {
        fetch
            .responses
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .map(|records| {
                                records
                                    .batches
                                    .iter()
                                    .map(|batch| batch.record_count as i64)
                                    .sum::<i64>()
                            })
                            .unwrap_or(0)
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    }

    #[tokio::test]
    async fn fetch_read_committed_matches_uncommitted_when_no_transactions() -> Result<(), Error> {
        let cluster_id = "phase08-fetch-read-committed";
        let node_id = 8;
        let topic = "phase08_fetch_read_committed";
        let partition = 0;
        let storage = memory_storage(cluster_id, node_id).await?;
        common::register_broker(storage.as_ref(), cluster_id, node_id).await?;
        _ = common::create_topic(storage.as_ref(), topic, 1).await?;
        let topition = Topition::new(topic, partition);

        for n in 0..4 {
            produce_value(storage.as_ref(), &topition, vec![b'x' + n; 64]).await?;
        }

        let fetch_uncommitted = FetchService
            .serve(
                Context::with_state(storage.clone()),
                request(
                    topic,
                    partition,
                    500,
                    50 * 1024,
                    IsolationLevel::ReadUncommitted,
                ),
            )
            .await?;

        let fetch_committed = FetchService
            .serve(
                Context::with_state(storage),
                request(
                    topic,
                    partition,
                    500,
                    50 * 1024,
                    IsolationLevel::ReadCommitted,
                ),
            )
            .await?;

        assert_eq!(4, record_count(&fetch_uncommitted));
        assert_eq!(
            record_count(&fetch_uncommitted),
            record_count(&fetch_committed),
            "ReadCommitted must return the same visible records as ReadUncommitted when last_stable == high_watermark"
        );

        let pu = &fetch_uncommitted.responses.as_deref().unwrap_or_default()[0]
            .partitions
            .as_deref()
            .unwrap_or_default()[0];
        let pc = &fetch_committed.responses.as_deref().unwrap_or_default()[0]
            .partitions
            .as_deref()
            .unwrap_or_default()[0];

        assert_eq!(pu.high_watermark, pc.high_watermark);
        assert_eq!(pu.last_stable_offset, pc.last_stable_offset);
        assert_eq!(
            Some(pu.high_watermark),
            pu.last_stable_offset,
            "non-transactional log: last stable offset should equal high watermark"
        );

        Ok(())
    }
}

mod doctest_template {
    use jansu_sans_io::{
        CreateTopicsRequest, ErrorCode, FetchRequest,
        create_topics_request::CreatableTopic,
        fetch_request::{FetchPartition, FetchTopic},
    };
    use jansu_storage::{CreateTopicsService, FetchService, StorageContainer};
    use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
    use url::Url;

    use crate::common::{Error, init_tracing};

    #[tokio::test]
    async fn req() -> Result<(), Error> {
        let _guard = init_tracing()?;

        const CLUSTER_ID: &str = "jansu";
        const NODE_ID: i32 = 111;
        const HOST: &str = "localhost";
        const PORT: i32 = 9092;

        let storage = StorageContainer::builder()
            .cluster_id(CLUSTER_ID)
            .node_id(NODE_ID)
            .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
            .storage(Url::parse("memory://jansu/")?)
            .build()
            .await?;

        let create_topic = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
        };

        let name = "abcba";

        let response = create_topic
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(5)
                            .replication_factor(3)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        let fetch = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(FetchService)
        };

        let partition = 0;

        let response = fetch
            .serve(
                Context::default(),
                FetchRequest::default()
                    .topics(Some(
                        [FetchTopic::default()
                            .topic(Some(name.into()))
                            .partitions(Some(
                                [FetchPartition::default().partition(partition)].into(),
                            ))]
                        .into(),
                    ))
                    .max_bytes(Some(0))
                    .max_wait_ms(5_000),
            )
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        Ok(())
    }
}
