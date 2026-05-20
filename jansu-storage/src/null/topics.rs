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

//! Topic, broker and configuration operations for the `null://` [`Engine`].

use jansu_sans_io::{
    ConfigResource, ErrorCode, NULL_TOPIC_ID,
    create_topics_request::CreatableTopic,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
};
use uuid::Uuid;

use super::Engine;
use crate::{Error, MetadataResponse, Result};

impl Engine {
    pub(super) fn null_brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    pub(super) fn null_create_topic(&self, topic: CreatableTopic) -> Result<Uuid> {
        self.topics
            .lock()
            .map_err(Into::into)
            .and_then(|mut topics| {
                if topics.iter().any(|existing| existing.name == topic.name) {
                    Err(Error::Api(ErrorCode::TopicAlreadyExists))
                } else {
                    topics.push(topic);
                    Ok(Uuid::now_v7())
                }
            })
    }

    pub(super) fn null_incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        Ok(AlterConfigsResourceResponse::default()
            .error_code(ErrorCode::None.into())
            .error_message(Some(ErrorCode::None.to_string()))
            .resource_name(resource.resource_name)
            .resource_type(resource.resource_type))
    }

    pub(super) fn null_metadata(&self) -> Result<MetadataResponse> {
        let node_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        self.topics
            .lock()
            .map(|topics| {
                topics
                    .iter()
                    .map(|topic| {
                        MetadataResponseTopic::default()
                            .error_code(ErrorCode::None.into())
                            .is_internal(Some(false))
                            .name(Some(topic.name.clone()))
                            .partitions(Some(
                                (0..topic.num_partitions)
                                    .map(|partition_index| {
                                        MetadataResponsePartition::default()
                                            .leader_id(self.node)
                                            .leader_epoch(Some(-1))
                                            .partition_index(partition_index)
                                            .error_code(ErrorCode::None.into())
                                            .offline_replicas(Some([].into()))
                                            .replica_nodes(Some(vec![
                                                self.node;
                                                topic.replication_factor
                                                    as usize
                                            ]))
                                            .isr_nodes(Some(vec![
                                                self.node;
                                                topic.replication_factor as usize
                                            ]))
                                    })
                                    .collect(),
                            ))
                            .topic_id(Some(NULL_TOPIC_ID))
                            .topic_authorized_operations(Some(i32::MIN))
                    })
                    .collect()
            })
            .map(|topics| MetadataResponse {
                cluster: Some(self.cluster.clone()),
                controller: Some(self.node),
                brokers: [MetadataResponseBroker::default()
                    .node_id(node_id)
                    .host(host)
                    .port(port)
                    .rack(rack)]
                .into(),
                topics,
            })
            .map_err(Into::into)
    }

    pub(super) fn null_describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
    ) -> Result<DescribeConfigsResult> {
        Ok(DescribeConfigsResult::default()
            .configs(Some([].into()))
            .resource_name(name.to_string())
            .resource_type(resource.into())
            .error_code(ErrorCode::None.into())
            .error_message(Some(ErrorCode::None.to_string())))
    }

    pub(super) fn null_describe_topic_partitions(
        &self,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.topics.lock().map_err(Into::into).map(|existing| {
            existing
                .iter()
                .map(|existing| {
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(existing.name.clone()))
                        .partitions(Some(
                            (0..existing.num_partitions)
                                .map(|partition_index| {
                                    DescribeTopicPartitionsResponsePartition::default()
                                        .leader_id(self.node)
                                        .partition_index(partition_index)
                                        .isr_nodes(Some(vec![
                                            self.node;
                                            existing.replication_factor as usize
                                        ]))
                                })
                                .collect(),
                        ))
                })
                .collect()
        })
    }
}
