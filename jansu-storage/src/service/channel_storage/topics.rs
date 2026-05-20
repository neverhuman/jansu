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

//! Topic and configuration channel calls for [`RequestChannelService`].

use jansu_sans_io::{
    ConfigResource, ErrorCode, create_topics_request::CreatableTopic,
    delete_records_request::DeleteRecordsTopic, delete_records_response::DeleteRecordsTopicResult,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};
use rama::{Context, Service};
use uuid::Uuid;

use crate::service::{Request, RequestChannelService, Response};
use crate::{Error, MetadataResponse, Result, TopicId, Topition};
use tracing::instrument;

impl RequestChannelService {
    #[instrument(name = "incremental_alter_resource", skip_all)]
    pub(super) async fn channel_incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.serve(
            Context::default(),
            Request::IncrementalAlterResource(resource),
        )
        .await
        .and_then(|response| {
            if let Response::IncrementalAlterResponse(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "create_topic", skip_all)]
    pub(super) async fn channel_create_topic(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        self.serve(
            Context::default(),
            Request::CreateTopic {
                topic,
                validate_only,
            },
        )
        .await
        .and_then(|response| {
            if let Response::CreateTopic(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "delete_records", skip_all)]
    pub(super) async fn channel_delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.serve(
            Context::default(),
            Request::DeleteRecords(Vec::from(topics)),
        )
        .await
        .and_then(|response| {
            if let Response::DeleteRecords(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "delete_topic", skip_all)]
    pub(super) async fn channel_delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.serve(Context::default(), Request::DeleteTopic(topic.to_owned()))
            .await
            .and_then(|response| {
                if let Response::DeleteTopic(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "metadata", skip_all)]
    pub(super) async fn channel_metadata(
        &self,
        topics: Option<&[TopicId]>,
    ) -> Result<MetadataResponse> {
        let topics = topics.map(Vec::from);

        self.serve(Context::default(), Request::Metadata(topics))
            .await
            .and_then(|response| {
                if let Response::Metadata(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "describe_config", skip_all)]
    pub(super) async fn channel_describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let name = name.to_string();
        let keys = keys.map(Vec::from);

        self.serve(
            Context::default(),
            Request::DescribeConfig {
                name,
                resource,
                keys,
            },
        )
        .await
        .and_then(|response| {
            if let Response::DescribeConfig(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "describe_topic_partitions", skip_all)]
    pub(super) async fn channel_describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let topics = topics.map(Vec::from);

        self.serve(
            Context::default(),
            Request::DescribeTopicPartitions {
                topics,
                partition_limit,
                cursor,
            },
        )
        .await
        .and_then(|response| {
            if let Response::DescribeTopicPartitions(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }
}
