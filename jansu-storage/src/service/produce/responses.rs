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

//! Response construction helpers for [`ProduceService`].

use jansu_sans_io::{
    ErrorCode, ProduceRequest, ProduceResponse,
    produce_request::TopicProduceData,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};
use tracing::warn;

use super::ProduceService;
use crate::Error;

impl ProduceService {
    pub(super) const MAGIC_V2: i8 = 2;

    pub(super) fn error(&self, index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        PartitionProduceResponse::default()
            .index(index)
            .error_code(error_code.into())
            .base_offset(-1)
            .log_append_time_ms(Some(-1))
            .log_start_offset(Some(0))
            .record_errors(Some([].into()))
            .error_message(None)
            .current_leader(None)
    }

    pub(super) fn partition_error(&self, index: i32, error: Error) -> PartitionProduceResponse {
        match error {
            Error::Api(error_code) => self.error(index, error_code),
            Error::Schema(_) => self.error(index, ErrorCode::InvalidRecord),
            otherwise => {
                warn!(?otherwise);
                self.error(index, ErrorCode::UnknownServerError)
            }
        }
    }

    pub(super) fn topic_error(
        &self,
        topic: TopicProduceData,
        error_code: ErrorCode,
    ) -> TopicProduceResponse {
        let partitions = topic.partition_data.map_or_else(Vec::new, |partitions| {
            partitions
                .into_iter()
                .map(|partition| self.error(partition.index, error_code))
                .collect()
        });

        TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions))
    }

    pub(super) fn invalid_acks_response(&self, req: ProduceRequest) -> ProduceResponse {
        let responses = req.topic_data.map_or_else(Vec::new, |topics| {
            topics
                .into_iter()
                .map(|topic| self.topic_error(topic, ErrorCode::InvalidRequiredAcks))
                .collect()
        });

        ProduceResponse::default()
            .responses(Some(responses))
            .throttle_time_ms(Some(0))
            .node_endpoints(None)
    }
}
