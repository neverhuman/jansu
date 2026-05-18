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

use crate::common::{Error, create_topic, init_tracing};
use bytes::Bytes;
use jansu_sans_io::{
    ApiKey as _, Body, ErrorCode, Frame, Header, IsolationLevel, ListOffset, ListOffsetsRequest,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    list_offsets_response::ListOffsetsResponse,
    record::{Record, inflated},
};
use jansu_storage::{ListOffsetsService, StorageContainer, Topition};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use std::convert::TryInto;
use url::Url;

mod common;

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("jansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(common::default_storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(move |_| storage.clone()).into_layer(ListOffsetsService);

    let topic = "abcba";

    let response = service
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(NODE_ID)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .current_leader_epoch(Some(-1))
                                .max_num_offsets(Some(3))
                                .partition_index(0)
                                .timestamp(ListOffset::Earliest.try_into()?)]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name);

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(0, partitions[0].partition_index);
    assert!(partitions[0].old_style_offsets.is_none());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(Some(-1), partitions[0].timestamp);
    assert_eq!(Some(-1), partitions[0].offset);
    assert_eq!(Some(-1), partitions[0].leader_epoch);

    Ok(())
}

#[cfg(feature = "dynostore")]
#[tokio::test]
async fn response_frame_round_trips_for_mixed_partition_errors() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("jansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(common::default_storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(move |_| storage.clone()).into_layer(ListOffsetsService);
    let topic = "abcba";

    let response = service
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(NODE_ID)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [
                                ListOffsetsPartition::default()
                                    .partition_index(0)
                                    .timestamp(ListOffset::Latest.try_into()?)
                                    .current_leader_epoch(Some(-1))
                                    .max_num_offsets(Some(1)),
                                ListOffsetsPartition::default()
                                    .partition_index(3)
                                    .timestamp(ListOffset::Latest.try_into()?)
                                    .current_leader_epoch(Some(-1))
                                    .max_num_offsets(Some(1)),
                            ]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topic = response.topics.as_deref().unwrap()[0].clone();
    let partitions = topic.partitions.as_deref().unwrap();
    let body = Body::ListOffsetsResponse(
        ListOffsetsResponse::default()
            .throttle_time_ms(Some(0))
            .topics(Some(vec![topic.clone()])),
    );
    let encoded = Frame::response(
        Header::Response { correlation_id: 0 },
        body.clone(),
        ListOffsetsRequest::KEY,
        9,
    )?;
    let decoded = Frame::response_from_bytes(&encoded[..], ListOffsetsRequest::KEY, 9)?;

    assert_eq!(body, decoded.body);
    assert_eq!(2, partitions.len());
    // Both partitions are UnknownTopicOrPartition since topic was never created.
    assert_eq!(
        Some(-1),
        partitions
            .iter()
            .find(|p| p.partition_index == 0)
            .and_then(|p| p.offset)
    );
    assert_eq!(
        Some(-1),
        partitions
            .iter()
            .find(|p| p.partition_index == 3)
            .and_then(|p| p.leader_epoch)
    );

    Ok(())
}

#[cfg(feature = "dynostore")]
#[tokio::test]
async fn response_frame_round_trips_for_exact_mixed_partition_errors() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;
    const CLUSTER_ID: &str = "jansu";

    let storage = StorageContainer::builder()
        .cluster_id(CLUSTER_ID)
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(common::default_storage_url()?)
        .build()
        .await?;

    common::register_broker(&*storage, CLUSTER_ID, NODE_ID).await?;

    let service_storage = storage.clone();
    let service =
        MapStateLayer::new(move |_| service_storage.clone()).into_layer(ListOffsetsService);
    let topic = "abcba";

    let response = service
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(-1)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [
                                ListOffsetsPartition::default()
                                    .partition_index(0)
                                    .timestamp(ListOffset::Latest.try_into()?)
                                    .current_leader_epoch(Some(-1))
                                    .max_num_offsets(Some(1)),
                                ListOffsetsPartition::default()
                                    .partition_index(6)
                                    .timestamp(ListOffset::Latest.try_into()?)
                                    .current_leader_epoch(Some(-1))
                                    .max_num_offsets(Some(1)),
                            ]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topic = response.topics.as_deref().unwrap()[0].clone();
    let body = Body::ListOffsetsResponse(
        ListOffsetsResponse::default()
            .throttle_time_ms(Some(0))
            .topics(Some(vec![topic.clone()])),
    );

    let encoded = Frame::response(
        Header::Response { correlation_id: 0 },
        body.clone(),
        ListOffsetsRequest::KEY,
        9,
    )?;
    let decoded = Frame::response_from_bytes(&encoded[..], ListOffsetsRequest::KEY, 9)?;

    assert_eq!(body, decoded.body);

    Ok(())
}

#[cfg(feature = "dynostore")]
#[tokio::test]
async fn response_frame_round_trips_for_produced_leader_epoch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;
    const CLUSTER_ID: &str = "jansu";

    let storage = StorageContainer::builder()
        .cluster_id(CLUSTER_ID)
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(common::default_storage_url()?)
        .build()
        .await?;

    common::register_broker(&*storage, CLUSTER_ID, NODE_ID).await?;

    let service_storage = storage.clone();
    let service =
        MapStateLayer::new(move |_| service_storage.clone()).into_layer(ListOffsetsService);
    let topic = "abcba";

    _ = create_topic(&*storage, topic, 1).await?;

    let topition = Topition::new(topic, 0);
    let epoch_0 = inflated::Batch::builder()
        .partition_leader_epoch(0)
        .record(Record::builder().value(Some(Bytes::from_static(b"epoch-0"))))
        .build()
        .map(TryInto::try_into)
        .and_then(|batch| batch)?;
    let epoch_1 = inflated::Batch::builder()
        .partition_leader_epoch(1)
        .record(Record::builder().value(Some(Bytes::from_static(b"epoch-1"))))
        .build()
        .map(TryInto::try_into)
        .and_then(|batch| batch)?;
    let _ = storage.produce(None, &topition, epoch_0).await?;
    let _ = storage.produce(None, &topition, epoch_1).await?;

    let response = service
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(-1)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .partition_index(0)
                                .timestamp(ListOffset::Latest.try_into()?)
                                .max_num_offsets(Some(1))]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topic = response.topics.as_deref().unwrap()[0].clone();
    let partition = topic.partitions.as_deref().unwrap().first().unwrap();
    assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
    assert_eq!(Some(2), partition.offset);
    assert_eq!(Some(1), partition.leader_epoch);
    let body = Body::ListOffsetsResponse(
        ListOffsetsResponse::default()
            .throttle_time_ms(Some(0))
            .topics(Some(vec![topic.clone()])),
    );

    let encoded = Frame::response(
        Header::Response { correlation_id: 0 },
        body.clone(),
        ListOffsetsRequest::KEY,
        9,
    )?;
    let decoded = Frame::response_from_bytes(&encoded[..], ListOffsetsRequest::KEY, 9)?;

    assert_eq!(body, decoded.body);

    let earliest_response = service
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(-1)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.name.clone())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .partition_index(0)
                                .timestamp(ListOffset::Earliest.try_into()?)
                                .max_num_offsets(Some(1))]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topic = earliest_response.topics.as_deref().unwrap()[0].clone();
    let partition = topic.partitions.as_deref().unwrap().first().unwrap();
    assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
    assert_eq!(Some(0), partition.offset);
    assert_eq!(Some(0), partition.leader_epoch);
    let body = Body::ListOffsetsResponse(
        ListOffsetsResponse::default()
            .throttle_time_ms(Some(0))
            .topics(Some(vec![topic.clone()])),
    );

    let encoded = Frame::response(
        Header::Response { correlation_id: 1 },
        body.clone(),
        ListOffsetsRequest::KEY,
        9,
    )?;
    let decoded = Frame::response_from_bytes(&encoded[..], ListOffsetsRequest::KEY, 9)?;

    assert_eq!(body, decoded.body);

    Ok(())
}
