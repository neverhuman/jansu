//! Cluster metadata `Storage` operation for the Turso `Engine`.

use super::sql::sql_lookup;
use super::*;

/// Topic row fields decoded from a `topic_select_*` query.
struct TopicRow {
    topic_id: Option<[u8; 16]>,
    name: Option<String>,
    is_internal: Option<bool>,
    partitions: i32,
    replication_factor: i32,
}

/// Decode the five common columns of a topic row.
fn decode_topic_row(row: &Row) -> Result<TopicRow> {
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

    let replication_factor = row.get_value(4).map_err(Into::into).and_then(|value| {
        value
            .as_integer()
            .map(|i| *i as i32)
            .ok_or(Error::UnexpectedValue(value))
    })?;

    Ok(TopicRow {
        topic_id,
        name,
        is_internal,
        partitions,
        replication_factor,
    })
}

/// Build the partition list for a topic, assigning leader/replica nodes.
fn metadata_partitions(
    broker_ids: &[i32],
    partitions: i32,
    replication_factor: i32,
    error_code: i16,
) -> Option<Vec<MetadataResponsePartition>> {
    let mut shuffled = broker_ids.to_vec();
    shuffled.shuffle(&mut rng());

    let mut brokers = shuffled.into_iter().cycle();

    Some(
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
    )
}

/// Construct a successful `MetadataResponseTopic` from a decoded topic row.
fn metadata_topic(decoded: TopicRow, broker_ids: &[i32]) -> MetadataResponseTopic {
    let error_code = ErrorCode::None.into();

    debug!(
        ?error_code,
        topic_id = ?decoded.topic_id,
        name = ?decoded.name,
        is_internal = ?decoded.is_internal,
        partitions = ?decoded.partitions,
        replication_factor = ?decoded.replication_factor
    );

    let partitions = metadata_partitions(
        broker_ids,
        decoded.partitions,
        decoded.replication_factor,
        error_code,
    );

    MetadataResponseTopic::default()
        .error_code(error_code)
        .name(decoded.name)
        .topic_id(decoded.topic_id)
        .is_internal(decoded.is_internal)
        .partitions(partitions)
        .topic_authorized_operations(Some(-2147483648))
}

fn unknown_topic_by_name(name: &str) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .error_code(ErrorCode::UnknownTopicOrPartition.into())
        .name(Some(name.into()))
        .topic_id(Some(NULL_TOPIC_ID))
        .is_internal(Some(false))
        .partitions(Some([].into()))
        .topic_authorized_operations(Some(-2147483648))
}

fn unknown_topic_by_id(id: &Uuid) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .error_code(ErrorCode::UnknownTopicOrPartition.into())
        .name(None)
        .topic_id(Some(id.into_bytes()))
        .is_internal(Some(false))
        .partitions(Some([].into()))
        .topic_authorized_operations(Some(-2147483648))
}

impl Engine {
    pub(super) async fn metadata_impl(
        &self,
        topics: Option<&[TopicId]>,
    ) -> Result<MetadataResponse> {
        debug!(cluster = self.cluster, ?topics);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

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

        let broker_ids: Vec<i32> = brokers.iter().map(|broker| broker.node_id).collect();

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    responses.push(match topic {
                        TopicId::Name(name) => {
                            let mut rows = c
                                .query(
                                    &sql_lookup("topic_select_name.sql")?,
                                    (self.cluster.as_str(), name.as_str()),
                                )
                                .await?;

                            match rows.next().await.inspect_err(|err| error!(?err)) {
                                Ok(Some(row)) => {
                                    metadata_topic(decode_topic_row(&row)?, &broker_ids)
                                }
                                Ok(None) => unknown_topic_by_name(name),
                                Err(reason) => {
                                    debug!(?reason);
                                    unknown_topic_by_name(name)
                                }
                            }
                        }
                        TopicId::Id(id) => {
                            debug!(?id);
                            let mut rows = c
                                .query(
                                    &sql_lookup("topic_select_uuid.sql")?,
                                    (self.cluster.as_str(), id.to_string().as_str()),
                                )
                                .await?;

                            match rows.next().await {
                                Ok(Some(row)) => {
                                    metadata_topic(decode_topic_row(&row)?, &broker_ids)
                                }
                                Ok(None) => unknown_topic_by_id(id),
                                Err(reason) => {
                                    debug!(?reason);
                                    unknown_topic_by_id(id)
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
                        &[self.cluster.as_str()],
                    )
                    .await?;

                while let Some(row) = rows.next().await? {
                    responses.push(metadata_topic(decode_topic_row(&row)?, &broker_ids));
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
    }
}
