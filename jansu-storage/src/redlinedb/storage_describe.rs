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

use super::*;

impl Delegate {
    pub(super) async fn delegate_describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let start = SystemTime::now();

        debug!(?topics, partition_limit, ?cursor);

        let c = self.connection().await?;

        let mut responses = Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or(0));

        for topic in topics.unwrap_or(&[]) {
            responses.push(match topic {
                TopicId::Name(name) => {
                    match c
                        .query_opt(
                            "topic_select_name.sql",
                            (self.cluster.as_str(), name.as_str()),
                        )
                        .await
                        .inspect_err(|err| error!(?err))
                    {
                        Ok(Some(row)) => {
                            let topic_id = row
                                .get_str(0)
                                .map_err(Error::from)
                                .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                                .map(|uuid| uuid.into_bytes())?;
                            let name = row.get::<String>(1).map(Some)?;
                            let is_internal = row.get::<bool>(2).map(Some)?;
                            let partitions = row.get::<i32>(3)?;
                            let replication_factor = row.get::<i32>(4)?;

                            debug!(
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::None.into())
                                .name(name)
                                .topic_id(topic_id)
                                .is_internal(false)
                                .partitions(Some(
                                    (0..partitions)
                                        .map(|partition_index| {
                                            DescribeTopicPartitionsResponsePartition::default()
                                                .error_code(ErrorCode::None.into())
                                                .partition_index(partition_index)
                                                .leader_id(self.node)
                                                .leader_epoch(0)
                                                .replica_nodes(Some(vec![
                                                    self.node;
                                                    replication_factor
                                                        as usize
                                                ]))
                                                .isr_nodes(Some(vec![
                                                    self.node;
                                                    replication_factor as usize
                                                ]))
                                                .eligible_leader_replicas(Some(vec![]))
                                                .last_known_elr(Some(vec![]))
                                                .offline_replicas(Some(vec![]))
                                        })
                                        .collect(),
                                ))
                                .topic_authorized_operations(-2147483648)
                        }

                        Ok(None) => DescribeTopicPartitionsResponseTopic::default()
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

                        Err(reason) => {
                            debug!(?reason);
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
                                .topic_authorized_operations(-2147483648)
                        }
                    }
                }
                TopicId::Id(id) => {
                    debug!(?id);
                    match c
                        .query_one(
                            "redlinedb/topic_select_uuid.sql",
                            (self.cluster.as_str(), id.to_string().as_str()),
                        )
                        .await
                    {
                        Ok(row) => {
                            let topic_id = row
                                .get_str(0)
                                .map_err(Error::from)
                                .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                                .map(|uuid| uuid.into_bytes())?;
                            let name = row.get::<String>(1).map(Some)?;
                            let is_internal = row.get::<bool>(2).map(Some)?;
                            let partitions = row.get::<i32>(3)?;
                            let replication_factor = row.get::<i32>(4)?;

                            debug!(
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::None.into())
                                .name(name)
                                .topic_id(topic_id)
                                .is_internal(false)
                                .partitions(Some(
                                    (0..partitions)
                                        .map(|partition_index| {
                                            DescribeTopicPartitionsResponsePartition::default()
                                                .error_code(ErrorCode::None.into())
                                                .partition_index(partition_index)
                                                .leader_id(self.node)
                                                .leader_epoch(0)
                                                .replica_nodes(Some(vec![
                                                    self.node;
                                                    replication_factor
                                                        as usize
                                                ]))
                                                .isr_nodes(Some(vec![
                                                    self.node;
                                                    replication_factor as usize
                                                ]))
                                                .eligible_leader_replicas(Some(vec![]))
                                                .last_known_elr(Some(vec![]))
                                                .offline_replicas(Some(vec![]))
                                        })
                                        .collect(),
                                ))
                                .topic_authorized_operations(-2147483648)
                        }

                        Err(reason) => {
                            debug!(?reason);
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
                                .topic_authorized_operations(-2147483648)
                        }
                    }
                }
            });
        }

        Ok(responses).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "describe_topic_partitions")],
            )
        })
    }

    pub(super) async fn delegate_describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();

        debug!(?group_ids, include_authorized_operations);

        let mut results = vec![];
        let c = self.connection().await?;

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                if let Some(row) = c
                    .query_opt(
                        "consumer_group_select_by_name.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?
                {
                    let current = row
                        .get_str(1)
                        .map_err(Error::from)
                        .and_then(|s| serde_json::from_str::<GroupDetail>(s).map_err(Into::into))
                        .inspect(|current| debug!(?current))
                        .inspect_err(|err| error!(?err, group_id))?;

                    results.push(NamedGroupDetail::found(group_id.into(), current));
                } else {
                    results.push(NamedGroupDetail::found(
                        group_id.into(),
                        GroupDetail::default(),
                    ));
                }
            }
        }

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "describe_groups")],
            )
        })
    }

    pub(super) async fn delegate_delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let start = SystemTime::now();

        debug!(?group_ids);

        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

            for group_id in group_ids {
                let Some(row) = c
                    .query_opt(
                        "redlinedb/consumer_group_select_id.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?
                else {
                    results.push(
                        DeletableGroupResult::default()
                            .group_id(group_id.into())
                            .error_code(ErrorCode::GroupIdNotFound.into()),
                    );
                    continue;
                };

                let consumer_group_id = row.get::<i64>(0)?;

                _ = c
                    .execute(
                        "redlinedb/consumer_offset_delete_by_cg_id.sql",
                        (consumer_group_id,),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(
                        "redlinedb/consumer_group_detail_delete_by_cg_id.sql",
                        (consumer_group_id,),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(
                        "redlinedb/consumer_group_delete_id.sql",
                        (consumer_group_id,),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(ErrorCode::None.into()),
                );
            }
        }

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_groups")],
            )
        })
    }
}
