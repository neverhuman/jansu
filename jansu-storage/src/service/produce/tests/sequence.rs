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

//! Producer sequence-number validation tests for [`ProduceService`].

use bytes::Bytes;
use jansu_sans_io::{
    ErrorCode, InitProducerIdRequest, ProduceRequest, ProduceResponse,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{Record, inflated},
};
use object_store::memory::InMemory;
use rama::{Context, Service};

use super::{init_tracing, topic_data};
use crate::{
    Result, dynostore::DynoStore, service::ProduceService,
    service::init_producer_id::InitProducerIdService,
};

#[tokio::test]
async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "abc";
    let node = 12321;
    let topic = "pqr";
    let index = 0;

    let storage = DynoStore::new(cluster, node, InMemory::new());
    let ctx = Context::with_state(storage);

    let init_producer_id = InitProducerIdService;

    let producer = init_producer_id
        .serve(
            ctx.clone(),
            InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
        )
        .await?;

    let request = ProduceService;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(0)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        request
            .serve(
                ctx.clone(),
                ProduceRequest::default()
                    .transactional_id(transactional_id.clone())
                    .acks(acks)
                    .timeout_ms(timeout_ms)
                    .topic_data(topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                )
                            )
                            .producer_id(producer.producer_id)
                    )?)
            )
            .await?
    );

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::DuplicateSequenceNumber.into())
                            .base_offset(-1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        request
            .serve(
                ctx,
                ProduceRequest::default()
                    .transactional_id(transactional_id)
                    .acks(acks)
                    .timeout_ms(timeout_ms)
                    .topic_data(topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                )
                            )
                            .producer_id(producer.producer_id)
                    )?)
            )
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "abc";
    let node = 12321;
    let topic = "pqr";
    let index = 0;

    let storage = DynoStore::new(cluster, node, InMemory::new());
    let ctx = Context::with_state(storage);

    let init_producer_id = InitProducerIdService;

    let producer = init_producer_id
        .serve(
            ctx.clone(),
            InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
        )
        .await?;

    let request = ProduceService;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(0)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        request
            .serve(
                ctx.clone(),
                ProduceRequest::default()
                    .transactional_id(transactional_id.clone())
                    .acks(acks)
                    .timeout_ms(timeout_ms)
                    .topic_data(topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                )
                            )
                            .producer_id(producer.producer_id)
                    )?)
            )
            .await?
    );

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::OutOfOrderSequenceNumber.into())
                            .base_offset(-1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        request
            .serve(
                ctx,
                ProduceRequest::default()
                    .transactional_id(transactional_id)
                    .acks(acks)
                    .timeout_ms(timeout_ms)
                    .topic_data(topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                )
                            )
                            .base_sequence(2)
                            .producer_id(producer.producer_id)
                    )?)
            )
            .await?
    );

    Ok(())
}
