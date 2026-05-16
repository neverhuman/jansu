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

//! Metadata query Storage impl helpers: metadata, describe_config, describe_topic_partitions

use std::iter;

use jansu_sans_io::{
    ConfigResource, ConfigSource, ErrorCode,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
};

use crate::{MetadataResponse, NULL_TOPIC_ID, Result, TopicId, Topition};

use super::engine::Engine;
use super::types::TopicMetadata;

impl Engine {
    pub(super) async fn impl_metadata(
        &self,
        topics: Option<&[TopicId]>,
    ) -> Result<MetadataResponse> {
        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ];

        let existing_topics = self.get_topics().await?;

        let topic_to_response = |topic_metadata: &TopicMetadata| {
            let name = Some(topic_metadata.topic.name.to_owned());
            let error_code = ErrorCode::None.into();
            let topic_id = Some(topic_metadata.id.into_bytes());
            let is_internal = Some(false);
            let num_partitions = topic_metadata.topic.num_partitions;
            let replication_factor = topic_metadata.topic.replication_factor;

            let partitions = Some(
                (0..num_partitions)
                    .map(|partition_index| {
                        let leader_id = self.node;
                        let replica_nodes =
                            Some(iter::repeat_n(self.node, replication_factor as usize).collect());
                        let isr_nodes = replica_nodes.clone();

                        MetadataResponsePartition::default()
                            .error_code(error_code)
                            .partition_index(partition_index)
                            .leader_id(leader_id)
                            .leader_epoch(Some(0))
                            .replica_nodes(replica_nodes)
                            .isr_nodes(isr_nodes)
                            .offline_replicas(Some([].into()))
                    })
                    .collect(),
            );

            MetadataResponseTopic::default()
                .error_code(error_code)
                .name(name)
                .topic_id(topic_id)
                .is_internal(is_internal)
                .partitions(partitions)
                .topic_authorized_operations(Some(i32::MIN))
        };

        let topic_responses = match topics {
            Some(topic_ids) if !topic_ids.is_empty() => {
                // Filter by requested topics
                let mut responses = Vec::with_capacity(topic_ids.len());

                for topic_id in topic_ids {
                    match topic_id {
                        TopicId::Name(name) => {
                            if let Some(metadata) = existing_topics.get(name.as_str()) {
                                responses.push(topic_to_response(metadata));
                            } else {
                                // Topic not found - return error response
                                responses.push(
                                    MetadataResponseTopic::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .name(Some(name.clone()))
                                        .topic_id(Some(NULL_TOPIC_ID))
                                        .is_internal(Some(false))
                                        .partitions(Some([].into()))
                                        .topic_authorized_operations(Some(i32::MIN)),
                                );
                            }
                        }
                        TopicId::Id(id) => {
                            // Find topic by UUID
                            let found =
                                existing_topics.values().find(|metadata| metadata.id == *id);

                            if let Some(metadata) = found {
                                responses.push(topic_to_response(metadata));
                            } else {
                                // Topic not found - return error response
                                responses.push(
                                    MetadataResponseTopic::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .name(None)
                                        .topic_id(Some(id.into_bytes()))
                                        .is_internal(Some(false))
                                        .partitions(Some([].into()))
                                        .topic_authorized_operations(Some(i32::MIN)),
                                );
                            }
                        }
                    }
                }

                responses
            }
            _ => {
                // Return all topics
                existing_topics.values().map(topic_to_response).collect()
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: topic_responses,
        })
    }

    pub(super) async fn impl_describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        if keys.is_some() {
            tracing::warn!(
                "describe_config key filtering is not implemented, returning all configs"
            );
        }
        match resource {
            ConfigResource::Topic => match self.topic_metadata(&TopicId::Name(name.into())).await {
                Ok(Some(topic_metadata)) => {
                    let error_code = ErrorCode::None;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(topic_metadata.topic.configs.map(|configs| {
                            configs
                                .iter()
                                .map(|config| {
                                    DescribeConfigsResourceResult::default()
                                        .name(config.name.clone())
                                        .value(config.value.clone())
                                        .read_only(false)
                                        .is_default(None)
                                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                                        .is_sensitive(false)
                                        .synonyms(Some([].into()))
                                        .config_type(Some(ConfigResource::Topic.into()))
                                        .documentation(Some("".into()))
                                })
                                .collect()
                        })))
                }

                Ok(None) => {
                    let error_code = ErrorCode::UnknownTopicOrPartition;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(Some([].into())))
                }

                Err(_) => {
                    let error_code = ErrorCode::UnknownServerError;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(Some([].into())))
                }
            },
            _ => {
                // For other resource types, return empty config
                Ok(DescribeConfigsResult::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some(ErrorCode::None.to_string()))
                    .resource_type(i8::from(resource))
                    .resource_name(name.into())
                    .configs(Some([].into())))
            }
        }
    }

    pub(super) async fn impl_describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        if partition_limit > 0 || cursor.is_some() {
            tracing::warn!(
                "describe_topic_partitions pagination is not implemented, returning all partitions"
            );
        }
        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            match self.topic_metadata(topic).await {
                Ok(Some(topic_metadata)) => {
                    responses.push(
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::None.into())
                            .name(Some(topic_metadata.topic.name))
                            .topic_id(topic.into())
                            .is_internal(false)
                            .partitions(Some(
                                (0..topic_metadata.topic.num_partitions)
                                    .map(|partition_index| {
                                        DescribeTopicPartitionsResponsePartition::default()
                                            .error_code(ErrorCode::None.into())
                                            .partition_index(partition_index)
                                            .leader_id(self.node)
                                            .leader_epoch(0)
                                            .replica_nodes(Some(vec![
                                                self.node;
                                                topic_metadata.topic.replication_factor
                                                    as usize
                                            ]))
                                            .isr_nodes(Some(vec![
                                                self.node;
                                                topic_metadata.topic.replication_factor
                                                    as usize
                                            ]))
                                            .eligible_leader_replicas(Some(vec![]))
                                            .last_known_elr(Some(vec![]))
                                            .offline_replicas(Some(vec![]))
                                    })
                                    .collect(),
                            ))
                            .topic_authorized_operations(-2147483648),
                    );
                }

                Ok(None) => {
                    responses.push(
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            })
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),
                    );
                }

                Err(_) => {
                    responses.push(
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownServerError.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            })
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),
                    );
                }
            }
        }

        Ok(responses)
    }
}
