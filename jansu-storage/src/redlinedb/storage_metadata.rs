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
    pub(super) async fn delegate_metadata(
        &self,
        topics: Option<&[TopicId]>,
    ) -> Result<MetadataResponse> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topics);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;

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

        debug!(?brokers);

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    responses.push(match topic {
                        TopicId::Name(name) => {
                            let (base_topic, key) = self
                                .topic_with_key(name.as_str())
                                .await
                                .inspect(|(base_topic, key)| debug!(base_topic, key))?;

                            let vtid = if let Some(key) = key {
                                self.virtual_topic_id(base_topic, key)
                                    .await
                                    .map(|uuid| uuid.into_bytes())
                                    .map(Some)
                            } else {
                                Ok(None)
                            }?;

                            let s = sql("topic_select_name.sql").map_err(Error::from)?;
                            let mut rows = c
                                .query(&s, (self.cluster.as_str(), base_topic))
                                .map_err(Error::from)?;

                            let step_result = rows.step().map_err(Error::from);
                            match step_result {
                                Ok(Step::Row(row)) => {
                                    let error_code = ErrorCode::None.into();

                                    let topic_id = vtid.or(
                                        Uuid::parse_str(
                                            row.get::<String>(0).map_err(Error::from)?.as_str(),
                                        )
                                        .map_err(Error::from)
                                        .map(|uuid| uuid.into_bytes())
                                        .map(Some)?,
                                    );

                                    let is_internal =
                                        row.get::<bool>(2).map_err(Error::from).map(Some)?;
                                    let partitions = row.get::<i32>(3).map_err(Error::from)?;
                                    let replication_factor =
                                        row.get::<i32>(4).map_err(Error::from)?;

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
                                                    .leader_epoch(Some(0))
                                                    .replica_nodes(replica_nodes)
                                                    .isr_nodes(isr_nodes)
                                                    .offline_replicas(Some([].into()))
                                            })
                                            .collect(),
                                    );

                                    MetadataResponseTopic::default()
                                        .error_code(error_code)
                                        .name(Some(name.to_owned()))
                                        .topic_id(topic_id)
                                        .is_internal(is_internal)
                                        .partitions(partitions)
                                        .topic_authorized_operations(Some(-2147483648))
                                }

                                Ok(Step::Done) => MetadataResponseTopic::default()
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

                            let s = sql("redlinedb/topic_select_uuid.sql").map_err(Error::from)?;
                            let mut rows = c
                                .query(&s, (self.cluster.as_str(), id.to_string().as_str()))
                                .map_err(Error::from)?;

                            let step_result = rows.step().map_err(Error::from);
                            match step_result {
                                Ok(Step::Row(row)) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = Uuid::parse_str(
                                        row.get::<String>(0).map_err(Error::from)?.as_str(),
                                    )
                                    .map_err(Error::from)
                                    .map(|uuid| uuid.into_bytes())
                                    .map(Some)?;
                                    let name =
                                        row.get::<String>(1).map_err(Error::from).map(Some)?;
                                    let is_internal =
                                        row.get::<bool>(2).map_err(Error::from).map(Some)?;
                                    let partitions = row.get::<i32>(3).map_err(Error::from)?;
                                    let replication_factor =
                                        row.get::<i32>(4).map_err(Error::from)?;

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
                                        .topic_authorized_operations(Some(-2147483648))
                                }
                                Ok(Step::Done) => MetadataResponseTopic::default()
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

                let s = sql("topic_by_cluster.sql").map_err(Error::from)?;
                let mut rows = c.query(&s, (self.cluster.as_str(),)).map_err(Error::from)?;

                while let Step::Row(row) = rows.step().map_err(Error::from)? {
                    let error_code = ErrorCode::None.into();
                    let topic_id = Uuid::parse_str(
                        row.get::<String>(0).map_err(Error::from)?.as_str(),
                    )
                    .map_err(Error::from)
                    .map(|uuid| uuid.into_bytes())
                    .map(Some)?;
                    let name = row.get::<String>(1).map_err(Error::from).map(Some)?;
                    let is_internal = row.get::<bool>(2).map_err(Error::from).map(Some)?;
                    let partitions = row.get::<i32>(3).map_err(Error::from)?;
                    let replication_factor = row.get::<i32>(4).map_err(Error::from)?;

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
                                    .leader_epoch(Some(0))
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
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "metadata")],
            )
        })
    }

    pub(super) async fn delegate_describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let mut c = self.connection().await?;

        let topic_exists = {
            let s = sql("topic_select.sql").map_err(Error::from)?;
            let mut rows = c
                .query(&s, (self.cluster.as_str(), name))
                .map_err(Error::from)?;
            matches!(rows.step().map_err(Error::from)?, Step::Row(_))
        };

        if topic_exists {
            use std::collections::{BTreeMap, BTreeSet};

            let mut configs = BTreeMap::new();

            let s2 = sql("topic_configuration_select.sql").map_err(Error::from)?;
            let mut topic_rows = c
                .query(&s2, (self.cluster.as_str(), name))
                .map_err(Error::from)?;

            while let Step::Row(row) = topic_rows.step().map_err(Error::from)? {
                let config_name = row
                    .get::<String>(0)
                    .map_err(Error::from)
                    .inspect_err(|err| error!(?err))?;
                let value = row
                    .get::<Option<String>>(1)
                    .map_err(Error::from)
                    .map(|value| value.unwrap_or(String::new()))
                    .map(Some)
                    .inspect_err(|err| error!(?err))?;

                _ = configs.insert(
                    config_name.clone(),
                    DescribeConfigsResourceResult::default()
                        .name(config_name.clone())
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

            if let Some(keys) = keys.filter(|keys| !keys.is_empty()) {
                let requested: BTreeSet<_> = keys.iter().map(|key| key.as_str()).collect();
                configs.retain(|name, _| requested.contains(name.as_str()));
            }

            let error_code = ErrorCode::None;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(configs.into_values().collect())))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_config")],
                )
            })
        } else {
            let error_code = ErrorCode::UnknownTopicOrPartition;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some([].into())))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_config")],
                )
            })
        }
    }

    pub(super) async fn delegate_list_groups(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        let start = SystemTime::now();

        debug!(?states_filter);

        let mut c = self.connection().await?;

        let mut listed_groups = vec![];

        let s = sql("consumer_group_select.sql").map_err(Error::from)?;
        let mut rows = c.query(&s, (self.cluster.as_str(),)).map_err(Error::from)?;

        while let Step::Row(row) = rows.step().map_err(Error::from)? {
            let group_id = row.get::<String>(0).map_err(Error::from)?;

            listed_groups.push(
                ListedGroup::default()
                    .group_id(group_id)
                    .protocol_type("consumer".into())
                    .group_state(Some("unknown".into()))
                    .group_type(Some("classic".into())),
            );
        }

        Ok(listed_groups).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "list_groups")],
            )
        })
    }
}
