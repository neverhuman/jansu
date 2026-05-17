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

use std::error::Error as StdError;

use bytes::Bytes;
use jansu_broker::{BrokerHandle, broker::Broker, coordinator::group::administrator::Controller};
use jansu_client::{Client, ConnectionManager};
use jansu_sans_io::{
    ErrorCode, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, ProduceRequest,
    SyncGroupRequest,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    join_group_request::JoinGroupRequestProtocol,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
    sync_group_request::SyncGroupRequestAssignment,
};
use jansu_storage::StorageContainer;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn StdError>>;

fn topic_name() -> String {
    format!("memory-native-{}", Uuid::now_v7())
}

fn topic_batch(value: &'static [u8]) -> Result<deflated::Batch, Box<dyn StdError>> {
    inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(value).into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn start_broker() -> Result<(CancellationToken, BrokerHandle), Box<dyn StdError>> {
    let cluster_id = Uuid::now_v7().to_string();
    let node_id = 111;
    let cancellation = CancellationToken::new();
    let storage_url = Url::parse(&format!("memory://{cluster_id}/"))?;

    let broker = Broker::<Controller<StorageContainer>, StorageContainer>::builder()
        .node_id(node_id)
        .cluster_id(cluster_id.clone())
        .incarnation_id(Uuid::now_v7())
        .listener(Url::parse("tcp://127.0.0.1:0")?)
        .advertised_listener(Url::parse("tcp://127.0.0.1:0")?)
        .storage(storage_url)
        .silent(true)
        .build()
        .await?;

    let handle = broker.start(cancellation.clone()).await?;
    Ok((cancellation, handle))
}

async fn client(bootstrap: Url) -> Result<Client, Box<dyn StdError>> {
    let pool = ConnectionManager::builder(bootstrap)
        .client_id(Some("jansu-client-memory-native".into()))
        .build()
        .await?;

    Ok(Client::new(pool))
}

async fn create_topic(client: &Client, topic: &str) -> Result<(), Box<dyn StdError>> {
    let response = client
        .call(
            jansu_sans_io::CreateTopicsRequest::default()
                .topics(Some(
                    [CreatableTopic::default()
                        .name(topic.into())
                        .num_partitions(1)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into()))]
                    .into(),
                ))
                .timeout_ms(10_000)
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

async fn produce_one(
    client: &Client,
    topic: &str,
    value: &'static [u8],
) -> Result<i64, Box<dyn StdError>> {
    let batch = topic_batch(value)?;
    let response = client
        .call(
            ProduceRequest::default()
                .acks(1)
                .timeout_ms(10_000)
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(topic.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(0)
                                .records(Some(Frame {
                                    batches: vec![batch],
                                }))]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    let partitions = responses[0]
        .partition_responses
        .as_deref()
        .unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(0, partitions[0].base_offset);

    Ok(partitions[0].base_offset)
}

async fn fetch_records(
    client: &Client,
    topic: &str,
    offset: i64,
) -> Result<usize, Box<dyn StdError>> {
    let response = client
        .call(
            jansu_sans_io::FetchRequest::default()
                .replica_id(Some(-1))
                .max_wait_ms(0)
                .min_bytes(1)
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(topic.into()))
                        .partitions(Some(
                            [FetchPartition::default()
                                .partition(0)
                                .fetch_offset(offset)
                                .partition_max_bytes(50 * 1024)]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    let partitions = responses[0].partitions.as_deref().unwrap_or_default();
    let records = partitions[0].records.as_ref().expect("fetch records");

    Ok(records.batches.len())
}

async fn join_group(
    client: &Client,
    group_id: &str,
) -> Result<jansu_sans_io::JoinGroupResponse, Box<dyn StdError>> {
    let protocols = [
        JoinGroupRequestProtocol::default()
            .name("range".into())
            .metadata(Bytes::from_static(b"range-protocol")),
        JoinGroupRequestProtocol::default()
            .name("cooperative-sticky".into())
            .metadata(Bytes::from_static(b"sticky-protocol")),
    ];

    let mut member_id = String::new();

    loop {
        let response = client
            .call(
                JoinGroupRequest::default()
                    .group_id(group_id.into())
                    .session_timeout_ms(45_000)
                    .rebalance_timeout_ms(Some(30_000))
                    .member_id(member_id.clone())
                    .protocol_type("consumer".into())
                    .protocols(Some(protocols.to_vec()))
                    .group_instance_id(None)
                    .reason(None),
            )
            .await?;

        match ErrorCode::try_from(response.error_code)? {
            ErrorCode::MemberIdRequired => {
                member_id = response.member_id;
            }
            ErrorCode::None => return Ok(response),
            other => return Err(format!("unexpected join error: {other:?}").into()),
        }
    }
}

async fn sync_group(
    client: &Client,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
) -> Result<(), Box<dyn StdError>> {
    let response = client
        .call(
            SyncGroupRequest::default()
                .group_id(group_id.into())
                .generation_id(generation_id)
                .member_id(member_id.into())
                .assignments(Some(
                    [SyncGroupRequestAssignment::default()
                        .member_id(member_id.into())
                        .assignment(Bytes::new())]
                    .into(),
                )),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    Ok(())
}

async fn heartbeat(
    client: &Client,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
) -> Result<(), Box<dyn StdError>> {
    let response = client
        .call(
            HeartbeatRequest::default()
                .group_id(group_id.into())
                .generation_id(generation_id)
                .member_id(member_id.into()),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    Ok(())
}

async fn leave_group(
    client: &Client,
    group_id: &str,
    member_id: &str,
) -> Result<(), Box<dyn StdError>> {
    let response = client
        .call(
            LeaveGroupRequest::default()
                .group_id(group_id.into())
                .member_id(Some(member_id.into())),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    Ok(())
}

#[tokio::test]
async fn memory_native_round_trip_and_reconnect() -> TestResult {
    let (cancellation, handle) = start_broker().await?;
    let bootstrap = handle.bootstrap.clone();
    assert_eq!(Some(handle.local_addr.port()), bootstrap.port());
    let topic = topic_name();

    let first_client = client(bootstrap.clone()).await?;
    create_topic(&first_client, &topic).await?;
    let produced_offset = produce_one(&first_client, &topic, b"hello world").await?;
    assert_eq!(0, produced_offset);
    assert_eq!(1, fetch_records(&first_client, &topic, 0).await?);
    let group_id = format!("group-{}", Uuid::now_v7());
    let join = join_group(&first_client, &group_id).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(join.error_code)?);
    assert_eq!(join.member_id, join.leader);

    sync_group(
        &first_client,
        &group_id,
        join.generation_id,
        &join.member_id,
    )
    .await?;
    heartbeat(
        &first_client,
        &group_id,
        join.generation_id,
        &join.member_id,
    )
    .await?;
    leave_group(&first_client, &group_id, &join.member_id).await?;

    drop(first_client);

    cancellation.cancel();
    _ = handle.join().await?;

    Ok(())
}

#[tokio::test]
async fn memory_native_topic_and_group_basics() -> TestResult {
    let (cancellation, handle) = start_broker().await?;
    let bootstrap = handle.bootstrap.clone();
    let client = client(bootstrap).await?;
    let topic = topic_name();

    create_topic(&client, &topic).await?;
    assert_eq!(0, produce_one(&client, &topic, b"alpha").await?);
    assert_eq!(1, fetch_records(&client, &topic, 0).await?);

    let group_id = format!("group-{}", Uuid::now_v7());
    let join = join_group(&client, &group_id).await?;
    sync_group(&client, &group_id, join.generation_id, &join.member_id).await?;
    heartbeat(&client, &group_id, join.generation_id, &join.member_id).await?;
    leave_group(&client, &group_id, &join.member_id).await?;

    drop(client);
    cancellation.cancel();
    _ = handle.join().await?;

    Ok(())
}
