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

pub(super) async fn metadata(
    this: &Engine,
    topics: Option<&[TopicId]>,
) -> Result<MetadataResponse> {
    debug!(cluster = this.cluster, ?topics);

    let c = this.connection().await.inspect_err(|err| error!(?err))?;

    let brokers = vec![
        MetadataResponseBroker::default()
            .node_id(this.node)
            .host(
                this.advertised_listener
                    .host_str()
                    .unwrap_or("0.0.0.0")
                    .into(),
            )
            .port(this.advertised_listener.port().unwrap_or(9092).into())
            .rack(None),
    ];

    debug!(?brokers);

    let responses = match topics {
        Some(topics) if !topics.is_empty() => {
            let mut responses = vec![];

            for topic in topics {
                responses.push(match topic {
                    TopicId::Name(name) => {
                        let mut rows = c
                            .query(
                                &sql_lookup("topic_select_name.sql")?,
                                (this.cluster.as_str(), name.as_str()),
                            )
                            .await?;

                        match rows.next().await.inspect_err(|err| error!(?err)) {
                            Ok(Some(row)) => {
                                let error_code = ErrorCode::None.into();

                                let topic_id = row.get_value(0).map_err(Error::from).and_then(
                                    |value| {
                                        value
                                            .as_text()
                                            .map(|value| {
                                                Uuid::parse_str(value)
                                                    .map(|uuid| uuid.into_bytes())
                                                    .map_err(Into::into)
                                            })
                                            .transpose()
                                    },
                                )?;

                                let name =
                                    row.get_value(1).map(|value| value.as_text().cloned())?;

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
                                    ?error_code,
                                    ?topic_id,
                                    ?name,
                                    ?is_internal,
                                    ?partitions,
                                    ?replication_factor
                                );

                                let mut rng = rng();
                                let mut broker_ids: Vec<_> =
                                    brokers.iter().map(|broker| broker.node_id).collect();
                                broker_ids.shuffle(&mut rng);

                                let mut brokers = broker_ids.into_iter().cycle();

                                let partitions = Some(
                                    (0..partitions)
                                        .map(|partition_index| {
                                            let leader_id = brokers.next().expect("cycling");

                                            let replica_nodes = Some(
                                                (0..replication_factor)
                                                    .map(|_replica| {
                                                        brokers.next().expect("cycling")
                                                    })
                                                    .collect(),
                                            );
                                            let isr_nodes = replica_nodes.clone();

                                            MetadataResponsePartition::default()
                                                .error_code(error_code)
                                                .partition_index(partition_index)
                                                .leader_id(leader_id)
                                                .leader_epoch(Some(-1))
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
                                    .topic_authorized_operations(Some(-2147483648))
                            }

                            Ok(None) => MetadataResponseTopic::default()
                                .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                .name(Some(name.into()))
                                .topic_id(Some(NULL_TOPIC_ID))
                                .is_internal(Some(false))
                                .partitions(Some([].into()))
                                .topic_authorized_operations(Some(-2147483648)),

                            Err(reason) => {
                                debug!(?reason);
                                MetadataResponseTopic::default()
                                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                    .name(Some(name.into()))
                                    .topic_id(Some(NULL_TOPIC_ID))
                                    .is_internal(Some(false))
                                    .partitions(Some([].into()))
                                    .topic_authorized_operations(Some(-2147483648))
                            }
                        }
                    }
                    TopicId::Id(id) => {
                        debug!(?id);
                        let mut rows = c
                            .query(
                                &sql_lookup("topic_select_uuid.sql")?,
                                (this.cluster.as_str(), id.to_string().as_str()),
                            )
                            .await?;

                        match rows.next().await {
                            Ok(Some(row)) => {
                                let error_code = ErrorCode::None.into();
                                let topic_id = row.get_value(0).map_err(Error::from).and_then(
                                    |value| {
                                        value
                                            .as_text()
                                            .map(|value| {
                                                Uuid::parse_str(value)
                                                    .map(|uuid| uuid.into_bytes())
                                                    .map_err(Into::into)
                                            })
                                            .transpose()
                                    },
                                )?;

                                let name =
                                    row.get_value(1).map(|value| value.as_text().cloned())?;

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
                                    ?error_code,
                                    ?topic_id,
                                    ?name,
                                    ?is_internal,
                                    ?partitions,
                                    ?replication_factor
                                );

                                let mut rng = rng();
                                let mut broker_ids: Vec<_> =
                                    brokers.iter().map(|broker| broker.node_id).collect();
                                broker_ids.shuffle(&mut rng);

                                let mut brokers = broker_ids.into_iter().cycle();

                                let partitions = Some(
                                    (0..partitions)
                                        .map(|partition_index| {
                                            let leader_id = brokers.next().expect("cycling");

                                            let replica_nodes = Some(
                                                (0..replication_factor)
                                                    .map(|_replica| {
                                                        brokers.next().expect("cycling")
                                                    })
                                                    .collect(),
                                            );
                                            let isr_nodes = replica_nodes.clone();

                                            MetadataResponsePartition::default()
                                                .error_code(error_code)
                                                .partition_index(partition_index)
                                                .leader_id(leader_id)
                                                .leader_epoch(Some(-1))
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
                                    .topic_authorized_operations(Some(-2147483648))
                            }
                            Ok(None) => MetadataResponseTopic::default()
                                .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                .name(None)
                                .topic_id(Some(id.into_bytes()))
                                .is_internal(Some(false))
                                .partitions(Some([].into()))
                                .topic_authorized_operations(Some(-2147483648)),
                            Err(reason) => {
                                debug!(?reason);
                                MetadataResponseTopic::default()
                                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                    .name(None)
                                    .topic_id(Some(id.into_bytes()))
                                    .is_internal(Some(false))
                                    .partitions(Some([].into()))
                                    .topic_authorized_operations(Some(-2147483648))
                            }
                        }
                    }
                });
            }

            responses
        }

        _ => {
            let mut responses = vec![];

            let mut rows = c
                .query(
                    &sql_lookup("topic_by_cluster.sql")?,
                    &[this.cluster.as_str()],
                )
                .await?;

            while let Some(row) = rows.next().await? {
                let error_code = ErrorCode::None.into();
                let topic_id = row.get_value(0).map_err(Error::from).and_then(|value| {
                    value
                        .as_text()
                        .map(|value| {
                            Uuid::parse_str(value)
                                .map(|uuid| uuid.into_bytes())
                                .map_err(Into::into)
                        })
                        .transpose()
                })?;

                let name = row.get_value(1).map(|value| value.as_text().cloned())?;

                let is_internal = row.get_value(2).map_err(Into::into).and_then(|value| {
                    value
                        .as_integer()
                        .map(|i| match *i {
                            0 => Ok(false),
                            1 => Ok(true),
                            _ => Err(Error::UnexpectedValue(value.clone())),
                        })
                        .transpose()
                })?;

                let partitions = row.get_value(3).map_err(Into::into).and_then(|value| {
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
                    ?error_code,
                    ?topic_id,
                    ?name,
                    ?is_internal,
                    ?partitions,
                    ?replication_factor
                );

                let mut rng = rng();
                let mut broker_ids: Vec<_> =
                    brokers.iter().map(|broker| broker.node_id).collect();
                broker_ids.shuffle(&mut rng);

                let mut brokers = broker_ids.into_iter().cycle();

                let partitions = Some(
                    (0..partitions)
                        .map(|partition_index| {
                            let leader_id = brokers.next().expect("cycling");

                            let replica_nodes = Some(
                                (0..replication_factor)
                                    .map(|_replica| brokers.next().expect("cycling"))
                                    .collect(),
                            );
                            let isr_nodes = replica_nodes.clone();

                            MetadataResponsePartition::default()
                                .error_code(error_code)
                                .partition_index(partition_index)
                                .leader_id(leader_id)
                                .leader_epoch(Some(-1))
                                .replica_nodes(replica_nodes)
                                .isr_nodes(isr_nodes)
                                .offline_replicas(Some([].into()))
                        })
                        .collect(),
                );

                responses.push(
                    MetadataResponseTopic::default()
                        .error_code(error_code)
                        .name(name)
                        .topic_id(topic_id)
                        .is_internal(is_internal)
                        .partitions(partitions)
                        .topic_authorized_operations(Some(-2147483648)),
                );
            }

            responses
        }
    };

    Ok(MetadataResponse {
        cluster: Some(this.cluster.clone()),
        controller: Some(this.node),
        brokers,
        topics: responses,
    })
}

pub(super) async fn describe_config(
    this: &Engine,
    name: &str,
    resource: ConfigResource,
    keys: Option<&[String]>,
) -> Result<DescribeConfigsResult> {
    debug!(cluster = this.cluster, name, ?resource, ?keys);

    let c = this.connection().await.inspect_err(|err| error!(?err))?;

    let mut rows = c
        .query(
            &sql_lookup("topic_select.sql")?,
            (this.cluster.as_str(), name),
        )
        .await?;

    if rows.next().await?.is_some() {
        let mut rows = c
            .query(
                &sql_lookup("topic_configuration_select.sql")?,
                (this.cluster.as_str(), name),
            )
            .await?;

        let mut configs = vec![];

        while let Some(row) = rows.next().await? {
            let name = row
                .get_value(0)
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_text()
                        .cloned()
                        .ok_or(Error::UnexpectedValue(value))
                })
                .inspect_err(|err| error!(?err))?;

            let value = row
                .get_value(1)
                .map(|value| value.as_text().cloned())
                .inspect_err(|err| error!(?err))?;

            configs.push(
                DescribeConfigsResourceResult::default()
                    .name(name)
                    .value(value)
                    .read_only(false)
                    .is_default(None)
                    .config_source(Some(ConfigSource::DefaultConfig.into()))
                    .is_sensitive(false)
                    .synonyms(Some([].into()))
                    .config_type(Some(ConfigType::String.into()))
                    .documentation(Some("".into())),
            );
        }

        let error_code = ErrorCode::None;

        Ok(DescribeConfigsResult::default()
            .error_code(error_code.into())
            .error_message(Some(error_code.to_string()))
            .resource_type(i8::from(resource))
            .resource_name(name.into())
            .configs(Some(configs)))
    } else {
        let error_code = ErrorCode::UnknownTopicOrPartition;

        Ok(DescribeConfigsResult::default()
            .error_code(error_code.into())
            .error_message(Some(error_code.to_string()))
            .resource_type(i8::from(resource))
            .resource_name(name.into())
            .configs(Some([].into())))
    }
}
