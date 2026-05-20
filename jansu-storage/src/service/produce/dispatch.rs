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

//! Per-topic / per-partition produce dispatch for [`ProduceService`].

use jansu_sans_io::{
    ErrorCode,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};
use rama::Context;
use tracing::{debug, error, instrument, warn};

use super::ProduceService;
use crate::{Error, Storage, TopicId, Topition};

impl ProduceService {
    #[instrument(skip_all)]
    pub(super) async fn partition<G>(
        &self,
        ctx: &Context<G>,
        transaction_id: Option<&str>,
        name: &str,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse
    where
        G: Storage,
    {
        let Some(records) = partition.records else {
            return self.error(partition.index, ErrorCode::InvalidRecord);
        };

        let tp = Topition::new(name, partition.index);

        if self.exactness_enabled(ctx) {
            let topic_ids = [TopicId::from(&tp)];
            match ctx.state().metadata(Some(&topic_ids)).await {
                Ok(metadata) => {
                    let Some(topic) = metadata.topics().iter().find(|topic| {
                        topic
                            .name
                            .as_deref()
                            .is_some_and(|topic_name| topic_name == tp.topic())
                    }) else {
                        return self.error(partition.index, ErrorCode::UnknownTopicOrPartition);
                    };

                    if topic.error_code != i16::from(ErrorCode::None) {
                        return self.error(partition.index, ErrorCode::UnknownTopicOrPartition);
                    }

                    let Some(partitions) = topic.partitions.as_deref() else {
                        return self.error(partition.index, ErrorCode::UnknownTopicOrPartition);
                    };

                    if !partitions
                        .iter()
                        .any(|candidate| candidate.partition_index == tp.partition())
                    {
                        return self.error(partition.index, ErrorCode::UnknownTopicOrPartition);
                    }
                }

                Err(Error::Api(error_code)) => {
                    debug!(?self, ?error_code);
                    return self.error(partition.index, error_code);
                }

                Err(otherwise) => {
                    warn!(?otherwise);
                    return self.error(partition.index, ErrorCode::UnknownServerError);
                }
            }

            for batch in &records.batches {
                if let Err(error) = Self::validate_batch(batch) {
                    return self.partition_error(partition.index, error);
                }
            }
        }

        let mut base_offset = None;

        for batch in records.batches {
            match ctx
                .state()
                .produce(transaction_id, &tp, batch)
                .await
                .inspect_err(|err| match err {
                    storage_api @ Error::Api(_) => {
                        warn!(?storage_api)
                    }
                    Error::Schema(_) => warn!(?tp, event = "schema validation failed"),
                    otherwise => error!(?otherwise),
                }) {
                Ok(offset) => _ = base_offset.get_or_insert(offset),

                Err(Error::Api(error_code)) => {
                    debug!(?self, ?error_code);
                    return self.error(partition.index, error_code);
                }

                Err(Error::Schema(_)) => {
                    return self.error(partition.index, ErrorCode::InvalidRecord);
                }

                Err(otherwise) => {
                    warn!(?otherwise);
                    return self.error(partition.index, ErrorCode::UnknownServerError);
                }
            }
        }

        if let Some(base_offset) = base_offset {
            PartitionProduceResponse::default()
                .index(partition.index)
                .error_code(ErrorCode::None.into())
                .base_offset(base_offset)
                .log_append_time_ms(Some(-1))
                .log_start_offset(Some(0))
                .record_errors(Some([].into()))
                .error_message(None)
                .current_leader(None)
        } else {
            self.error(partition.index, ErrorCode::InvalidRecord)
        }
    }

    #[instrument(skip_all)]
    pub(super) async fn topic<G>(
        &self,
        ctx: &Context<G>,
        transaction_id: Option<&str>,
        topic: TopicProduceData,
    ) -> TopicProduceResponse
    where
        G: Storage,
    {
        let mut partitions = vec![];

        if let Some(partition_data) = topic.partition_data {
            for partition in partition_data {
                partitions.push(
                    self.partition(ctx, transaction_id, &topic.name, partition)
                        .await,
                )
            }
        }

        TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions))
    }
}
