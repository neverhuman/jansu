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

pub(super) async fn describe_topic_partitions(
    this: &Engine,
    topics: Option<&[TopicId]>,
    partition_limit: i32,
    cursor: Option<Topition>,
) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
    debug!(?topics, partition_limit, ?cursor);
    let c = this.connection().await.inspect_err(|err| error!(?err))?;

    let mut responses =
        Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

    for topic in topics.unwrap_or_default() {
        responses.push(match topic {
            TopicId::Name(name) => {
                match this
                    .prepare_query_opt(
                        &c,
                        &sql_lookup("topic_select_name.sql")?,
                        (this.cluster.as_str(), name.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))
                {
                    Ok(Some(row)) => {
                        let topic_id =
                            row.get_value(0).map_err(Error::from).and_then(|value| {
                                value.as_text().map_or(
                                    Err(Error::UnexpectedValue(value.clone())),
                                    |value| {
                                        Uuid::parse_str(value)
                                            .map(|uuid| uuid.into_bytes())
                                            .map_err(Into::into)
                                    },
                                )
                            })?;

                        let name = row.get_value(1).map(|value| value.as_text().cloned())?;

                        let is_internal =
                            row.get_value(2).map_err(Into::into).and_then(|value| {
                                value
                                    .as_integer()
                                    .map(|i| match *i {
                                        0 => Ok(false),
                                        1 => Ok(true),
                                        _ => Err(Error::UnexpectedValue(value.clone())),
                                    })
                                    .transpose()
                            })?;

                        let partitions =
                            row.get_value(3).map_err(Into::into).and_then(|value| {
                                value
                                    .as_integer()
                                    .map(|i| *i as i32)
                                    .ok_or(Error::UnexpectedValue(value))
                            })?;

                        let replication_factor =
                            row.get_value(4).map_err(Into::into).and_then(|value| {
                                value
                                    .as_integer()
                                    .map(|i| *i as i32)
                                    .ok_or(Error::UnexpectedValue(value))
                            })?;

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
                                            .leader_id(this.node)
                                            .leader_epoch(-1)
                                            .replica_nodes(Some(vec![
                                                this.node;
                                                replication_factor
                                                    as usize
                                            ]))
                                            .isr_nodes(Some(vec![
                                                this.node;
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
                match this
                    .prepare_query_one(
                        &c,
                        &sql_lookup("topic_select_uuid.sql")?,
                        (this.cluster.as_str(), id.to_string().as_str()),
                    )
                    .await
                {
                    Ok(row) => {
                        let topic_id =
                            row.get_value(0).map_err(Error::from).and_then(|value| {
                                value.as_text().map_or(
                                    Err(Error::UnexpectedValue(value.clone())),
                                    |value| {
                                        Uuid::parse_str(value)
                                            .map(|uuid| uuid.into_bytes())
                                            .map_err(Into::into)
                                    },
                                )
                            })?;

                        let name = row.get_value(1).map(|value| value.as_text().cloned())?;

                        let is_internal =
                            row.get_value(2).map_err(Into::into).and_then(|value| {
                                value
                                    .as_integer()
                                    .map(|i| match *i {
                                        0 => Ok(false),
                                        1 => Ok(true),
                                        _ => Err(Error::UnexpectedValue(value.clone())),
                                    })
                                    .transpose()
                            })?;

                        let partitions =
                            row.get_value(3).map_err(Into::into).and_then(|value| {
                                value
                                    .as_integer()
                                    .map(|i| *i as i32)
                                    .ok_or(Error::UnexpectedValue(value))
                            })?;

                        let replication_factor =
                            row.get_value(4).map_err(Into::into).and_then(|value| {
                                value
                                    .as_integer()
                                    .map(|i| *i as i32)
                                    .ok_or(Error::UnexpectedValue(value))
                            })?;

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
                                            .leader_id(this.node)
                                            .leader_epoch(-1)
                                            .replica_nodes(Some(vec![
                                                this.node;
                                                replication_factor
                                                    as usize
                                            ]))
                                            .isr_nodes(Some(vec![
                                                this.node;
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

    Ok(responses)
}
