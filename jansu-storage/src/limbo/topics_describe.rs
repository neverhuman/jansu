//! Describe-topic-partitions `Storage` operation for the Turso `Engine`.

use super::sql::sql_lookup;
use super::*;

/// Topic columns decoded from a `topic_select_*` query for describe responses.
struct DescribeTopicRow {
    topic_id: [u8; 16],
    name: Option<String>,
    partitions: i32,
    replication_factor: i32,
}

fn decode_describe_row(row: &Row) -> Result<DescribeTopicRow> {
    let topic_id = row.get_value(0).map_err(Error::from).and_then(|value| {
        value
            .as_text()
            .map_or(Err(Error::UnexpectedValue(value.clone())), |value| {
                Uuid::parse_str(value)
                    .map(|uuid| uuid.into_bytes())
                    .map_err(Into::into)
            })
    })?;

    let name = row.get_value(1).map(|value| value.as_text().cloned())?;

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

    Ok(DescribeTopicRow {
        topic_id,
        name,
        partitions,
        replication_factor,
    })
}

fn unknown_describe_topic(
    topic: &TopicId,
    error_code: ErrorCode,
) -> DescribeTopicPartitionsResponseTopic {
    DescribeTopicPartitionsResponseTopic::default()
        .error_code(error_code.into())
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

impl Engine {
    fn describe_topic(&self, decoded: DescribeTopicRow) -> DescribeTopicPartitionsResponseTopic {
        debug!(
            topic_id = ?decoded.topic_id,
            name = ?decoded.name,
            partitions = ?decoded.partitions,
            replication_factor = ?decoded.replication_factor
        );

        let replication_factor = decoded.replication_factor as usize;

        DescribeTopicPartitionsResponseTopic::default()
            .error_code(ErrorCode::None.into())
            .name(decoded.name)
            .topic_id(decoded.topic_id)
            .is_internal(false)
            .partitions(Some(
                (0..decoded.partitions)
                    .map(|partition_index| {
                        DescribeTopicPartitionsResponsePartition::default()
                            .error_code(ErrorCode::None.into())
                            .partition_index(partition_index)
                            .leader_id(self.node)
                            .leader_epoch(-1)
                            .replica_nodes(Some(vec![self.node; replication_factor]))
                            .isr_nodes(Some(vec![self.node; replication_factor]))
                            .eligible_leader_replicas(Some(vec![]))
                            .last_known_elr(Some(vec![]))
                            .offline_replicas(Some(vec![]))
                    })
                    .collect(),
            ))
            .topic_authorized_operations(-2147483648)
    }

    pub(super) async fn describe_topic_partitions_impl(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        debug!(?topics, partition_limit, ?cursor);
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut responses = Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or(0));

        for topic in topics.unwrap_or(&[]) {
            responses.push(match topic {
                TopicId::Name(name) => {
                    match self
                        .prepare_query_opt(
                            &c,
                            &sql_lookup("topic_select_name.sql")?,
                            (self.cluster.as_str(), name.as_str()),
                        )
                        .await
                        .inspect_err(|err| error!(?err))
                    {
                        Ok(Some(row)) => self.describe_topic(decode_describe_row(&row)?),

                        Ok(None) => {
                            unknown_describe_topic(topic, ErrorCode::UnknownTopicOrPartition)
                        }

                        Err(reason) => {
                            debug!(?reason);
                            unknown_describe_topic(topic, ErrorCode::UnknownServerError)
                        }
                    }
                }
                TopicId::Id(id) => {
                    debug!(?id);
                    match self
                        .prepare_query_one(
                            &c,
                            &sql_lookup("topic_select_uuid.sql")?,
                            (self.cluster.as_str(), id.to_string().as_str()),
                        )
                        .await
                    {
                        Ok(row) => self.describe_topic(decode_describe_row(&row)?),

                        Err(reason) => {
                            debug!(?reason);
                            unknown_describe_topic(topic, ErrorCode::UnknownTopicOrPartition)
                        }
                    }
                }
            });
        }

        Ok(responses)
    }
}
