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

type TopicRowData = (String, String, bool, i32, i32);

fn query_topic_row(
    c: &mut PoolConnection,
    key: &str,
    params: impl ::redlinedb::Params,
) -> Result<Option<TopicRowData>> {
    let s = sql(key).map_err(Error::from)?;
    let mut rows = c
        .query(&s, params)
        .map_err(Error::from)
        .inspect_err(|err| error!(?err))?;
    match rows.step().map_err(Error::from)? {
        Step::Row(row) => {
            let uuid_str = row.get::<String>(0).map_err(Error::from)?;
            let name = row.get::<String>(1).map_err(Error::from)?;
            let is_internal = row.get::<bool>(2).map_err(Error::from)?;
            let partitions = row.get::<i32>(3).map_err(Error::from)?;
            let replication_factor = row.get::<i32>(4).map_err(Error::from)?;
            Ok(Some((
                uuid_str,
                name,
                is_internal,
                partitions,
                replication_factor,
            )))
        }
        Step::Done => Ok(None),
    }
}

fn build_topic_response(
    node: i32,
    uuid_str: &str,
    name: String,
    _is_internal: bool,
    partitions: i32,
    replication_factor: i32,
) -> Result<DescribeTopicPartitionsResponseTopic> {
    let topic_id = Uuid::parse_str(uuid_str)
        .map_err(Error::from)
        .map(|uuid| uuid.into_bytes())?;
    debug!(?topic_id, ?name, partitions, replication_factor);
    Ok(DescribeTopicPartitionsResponseTopic::default()
        .error_code(ErrorCode::None.into())
        .name(Some(name))
        .topic_id(topic_id)
        .is_internal(false)
        .partitions(Some(
            (0..partitions)
                .map(|partition_index| {
                    DescribeTopicPartitionsResponsePartition::default()
                        .error_code(ErrorCode::None.into())
                        .partition_index(partition_index)
                        .leader_id(node)
                        .leader_epoch(0)
                        .replica_nodes(Some(vec![node; replication_factor as usize]))
                        .isr_nodes(Some(vec![node; replication_factor as usize]))
                        .eligible_leader_replicas(Some(vec![]))
                        .last_known_elr(Some(vec![]))
                        .offline_replicas(Some(vec![]))
                })
                .collect(),
        ))
        .topic_authorized_operations(-2147483648))
}

impl Delegate {
    pub(super) async fn delegate_describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let start = SystemTime::now();

        debug!(?topics, partition_limit, ?cursor);

        let mut c = self.connection().await?;

        let mut responses = Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or(0));

        for topic in topics.unwrap_or(&[]) {
            let response = match topic {
                TopicId::Name(name) => {
                    match query_topic_row(
                        &mut c,
                        "topic_select_name.sql",
                        (self.cluster.as_str(), name.as_str()),
                    ) {
                        Ok(Some((
                            uuid_str,
                            topic_name,
                            is_internal,
                            partitions,
                            replication_factor,
                        ))) => build_topic_response(
                            self.node,
                            &uuid_str,
                            topic_name,
                            is_internal,
                            partitions,
                            replication_factor,
                        )?,
                        Ok(None) => DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(Some(name.into()))
                            .topic_id(NULL_TOPIC_ID)
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),
                        Err(reason) => {
                            debug!(?reason);
                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::UnknownServerError.into())
                                .name(Some(name.into()))
                                .topic_id(NULL_TOPIC_ID)
                                .is_internal(false)
                                .partitions(Some([].into()))
                                .topic_authorized_operations(-2147483648)
                        }
                    }
                }
                TopicId::Id(id) => {
                    debug!(?id);
                    match query_topic_row(
                        &mut c,
                        "redlinedb/topic_select_uuid.sql",
                        (self.cluster.as_str(), id.to_string().as_str()),
                    ) {
                        Ok(Some((
                            uuid_str,
                            topic_name,
                            is_internal,
                            partitions,
                            replication_factor,
                        ))) => build_topic_response(
                            self.node,
                            &uuid_str,
                            topic_name,
                            is_internal,
                            partitions,
                            replication_factor,
                        )?,
                        Ok(None) | Err(_) => DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(None)
                            .topic_id(id.into_bytes())
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),
                    }
                }
            };
            responses.push(response);
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
        let mut c = self.connection().await?;

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let detail_str: Option<String> = {
                    let s = sql("consumer_group_select_by_name.sql").map_err(Error::from)?;
                    let mut rows = c
                        .query(&s, (self.cluster.as_str(), group_id.as_str()))
                        .map_err(Error::from)
                        .inspect_err(|err| error!(?err, group_id))?;
                    match rows.step().map_err(Error::from)? {
                        Step::Row(row) => Some(row.get::<String>(1).map_err(Error::from)?),
                        Step::Done => None,
                    }
                };

                if let Some(detail_str) = detail_str {
                    let current = serde_json::from_str::<GroupDetail>(detail_str.as_str())
                        .map_err(Error::from)
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
            let mut c = self.connection().await?;

            for group_id in group_ids {
                let consumer_group_id = {
                    let s = sql("redlinedb/consumer_group_select_id.sql").map_err(Error::from)?;
                    let mut rows = c
                        .query(&s, (self.cluster.as_str(), group_id.as_str()))
                        .map_err(Error::from)
                        .inspect_err(|err| error!(?err, group_id))?;
                    match rows.step().map_err(Error::from)? {
                        Step::Row(row) => row.get::<i64>(0).map_err(Error::from)?,
                        Step::Done => {
                            results.push(
                                DeletableGroupResult::default()
                                    .group_id(group_id.into())
                                    .error_code(ErrorCode::GroupIdNotFound.into()),
                            );
                            continue;
                        }
                    }
                };

                _ = c
                    .execute(
                        &sql("redlinedb/consumer_offset_delete_by_cg_id.sql")
                            .map_err(Error::from)?,
                        (consumer_group_id,),
                    )
                    .map_err(Error::from)
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(
                        &sql("redlinedb/consumer_group_detail_delete_by_cg_id.sql")
                            .map_err(Error::from)?,
                        (consumer_group_id,),
                    )
                    .map_err(Error::from)
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(
                        &sql("redlinedb/consumer_group_delete_id.sql").map_err(Error::from)?,
                        (consumer_group_id,),
                    )
                    .map_err(Error::from)
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
