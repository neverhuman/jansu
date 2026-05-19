use super::sql::{sql_lookup, unique_constraint};
use super::*;

impl Engine {
    pub(super) async fn register_broker_impl(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        debug!(?broker_registration);

        let connection = self.connection().await?;

        self.prepare_execute(
            &connection,
            &sql_lookup("register_broker.sql")?,
            &[broker_registration.cluster_id],
        )
        .await
        .map_err(Into::into)
        .and(Ok(()))
    }

    pub(super) async fn brokers_impl(&self) -> Result<Vec<DescribeClusterBroker>> {
        debug!(cluster = self.cluster);

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

    pub(super) async fn create_topic_impl(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut connection = self.connection().await.inspect_err(|err| error!(?err))?;

        let tx = connection.transaction().await?;

        let uuid = {
            let uuid = Uuid::new_v4();

            let parameters = (
                self.cluster.as_str(),
                topic.name.as_str(),
                uuid.to_string(),
                topic.num_partitions,
                (topic.replication_factor as i32),
            );

            self.prepare_query_one(&tx, &sql_lookup("topic_insert.sql")?, parameters.clone())
                .await
                .inspect_err(|err| error!(?err))
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
                .inspect(|row| debug!(?parameters, ?row))
                .and_then(|row| {
                    row.get_value(0)
                        .map(|value| value.as_text().cloned().unwrap())
                        .inspect_err(|err| error!(?err))
                        .map_err(Into::into)
                })
                .and_then(|id| Uuid::parse_str(id.as_str()).map_err(Into::into))
        }
        .inspect(|uuid| debug!(?uuid))
        .inspect_err(|err| error!(?err))?;

        for partition in 0..topic.num_partitions {
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            _ = self
                .prepare_query_one(&tx, &sql_lookup("topition_insert.sql")?, params)
                .await
                .map(|row| row.get_value(0))
                .inspect(|topition| debug!(?topition))?;

            _ = self
                .prepare_query_one(&tx, &sql_lookup("watermark_insert.sql")?, params)
                .await
                .map(|row| row.get_value(0))
                .inspect(|watermark| debug!(?watermark))?;

            _ = self
                .prepare_execute(
                    &tx,
                    &sql_lookup("leader_epoch_history_insert.sql")?,
                    (self.cluster.as_str(), topic.name.as_str(), partition, 0, 0),
                )
                .await
                .inspect_err(|err| error!(?err, ?topic, ?partition))?;
        }

        if let Some(configs) = topic.configs {
            for config in configs {
                debug!(?config);

                let params = (
                    self.cluster.as_str(),
                    topic.name.as_str(),
                    config.name.as_str(),
                    config.value.as_deref(),
                );

                _ = self
                    .prepare_query_one(&tx, &sql_lookup("topic_configuration_upsert.sql")?, params)
                    .await
                    .map(|row| row.get_value(0))
                    .inspect_err(|err| error!(?err, ?config))
                    .inspect(|id| debug!(?id, ?config))?;
            }
        }

        tx.commit().await.map_err(Into::into).and(Ok(uuid))
    }

    pub(super) async fn delete_records_impl(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);
        let c = self.connection().await?;
        let mut responses = Vec::with_capacity(topics.len());

        for topic in topics {
            let topic_exists = self
                .prepare_query_opt(
                    &c,
                    &sql_lookup("topic_select_name.sql")?,
                    (self.cluster.as_str(), topic.name.as_str()),
                )
                .await?
                .is_some();
            let mut partition_responses = vec![];

            let partitions = match topic.partitions.as_deref() {
                Some(partitions) => partitions,
                None => &[],
            };

            for partition in partitions {
                let (error_code, low_watermark) = if !topic_exists {
                    (ErrorCode::UnknownTopicOrPartition, 0)
                } else if self
                    .prepare_query_opt(
                        &c,
                        &sql_lookup("topition_select.sql")?,
                        (
                            self.cluster.as_str(),
                            topic.name.as_str(),
                            partition.partition_index,
                        ),
                    )
                    .await?
                    .is_none()
                {
                    (ErrorCode::UnknownTopicOrPartition, 0)
                } else {
                    let watermark = self
                        .prepare_query_one(
                            &c,
                            &sql_lookup("watermark_select.sql")?,
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                            ),
                        )
                        .await?;
                    let current_low = match watermark.get_value(0)?.as_integer().copied() {
                        Some(current_low) => current_low,
                        None => 0,
                    };
                    let high = match watermark.get_value(1)?.as_integer().copied() {
                        Some(high) => high,
                        None => 0,
                    };
                    let low = current_low.max(partition.offset.clamp(0, high));

                    _ = self
                        .prepare_execute(
                            &c,
                            &sql_lookup("record_delete_before_offset.sql")?,
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                                low,
                            ),
                        )
                        .await?;

                    _ = self
                        .prepare_execute(
                            &c,
                            &sql_lookup("watermark_update.sql")?,
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                                low,
                                high,
                            ),
                        )
                        .await?;

                    (ErrorCode::None, low)
                };

                partition_responses.push(
                    DeleteRecordsPartitionResult::default()
                        .partition_index(partition.partition_index)
                        .low_watermark(low_watermark)
                        .error_code(error_code.into()),
                );
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }

    pub(super) async fn delete_topic_impl(&self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(cluster = self.cluster, ?topic);

        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let mut rows = match topic {
            TopicId::Id(id) => {
                tx.query(
                    &sql_lookup("topic_select_uuid.sql")?,
                    (self.cluster.as_str(), id.to_string().as_str()),
                )
                .await?
            }

            TopicId::Name(name) => {
                tx.query(
                    &sql_lookup("topic_select_name.sql")?,
                    (self.cluster.as_str(), name.as_str()),
                )
                .await?
            }
        };

        let Some(row) = rows.next().await? else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let value = row.get_value(1)?;
        let topic_name = value
            .as_text()
            .map(|topic_name| topic_name.as_str())
            .ok_or(Error::UnexpectedValue(value.clone()))?;

        for sql in [
            "consumer_offset_delete_by_topic.sql",
            "topic_configuration_delete_by_topic.sql",
            "watermark_delete_by_topic.sql",
            "header_delete_by_topic.sql",
            "record_delete_by_topic.sql",
            "txn_offset_commit_tp_delete_by_topic.sql",
            "txn_produce_offset_delete_by_topic.sql",
            "txn_topition_delete_by_topic.sql",
            "producer_detail_delete_by_topic.sql",
            "topition_delete_by_topic.sql",
        ] {
            let rows = self
                .prepare_execute(&tx, &sql_lookup(sql)?, (self.cluster.as_str(), topic_name))
                .await?;

            debug!(?topic, rows, sql)
        }

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("topic_delete_by.sql")?,
                (self.cluster.as_str(), topic_name),
            )
            .await?;

        tx.commit()
            .await
            .map_err(Into::into)
            .and(Ok(ErrorCode::None))
    }

    pub(super) async fn incremental_alter_resource_impl(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        debug!(?resource);
        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Group => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::ClientMetric => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::BrokerLogger => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Broker => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Topic => {
                let mut error_code = ErrorCode::None;

                for config in resource.configs.unwrap_or_else(Vec::new) {
                    let operation = OpType::try_from(config.config_operation)?;
                    match operation {
                        OpType::Set => {
                            let c = self.connection().await?;

                            if c.query(
                                &sql_lookup("topic_configuration_upsert.sql")?,
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                    config.value.as_deref(),
                                ),
                            )
                            .await
                            .inspect_err(|err| error!(?err))
                            .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Delete => {
                            let c = self.connection().await?;

                            if c.query(
                                &sql_lookup("topic_configuration_delete.sql")?,
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                ),
                            )
                            .await
                            .inspect_err(|err| error!(?err))
                            .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Append | OpType::Subtract => {
                            let c = self.connection().await?;
                            let mut statement = c
                                .prepare(&sql_lookup("topic_configuration_select.sql")?)
                                .await?;
                            let mut rows = statement
                                .query((self.cluster.as_str(), resource.resource_name.as_str()))
                                .await?;

                            let mut current = None;
                            while let Some(row) = rows.next().await? {
                                let name = row.get_value(0)?;
                                if name
                                    .as_text()
                                    .is_some_and(|name| name.as_str() == config.name)
                                {
                                    let value = row.get_value(1)?;
                                    current =
                                        value.as_text().map(|value| value.as_str().to_owned());
                                    break;
                                }
                            }

                            let updated = match operation {
                                OpType::Append => crate::append_config_tokens(
                                    current.as_deref(),
                                    config.value.as_deref(),
                                ),
                                OpType::Subtract => crate::subtract_config_tokens(
                                    current.as_deref(),
                                    config.value.as_deref(),
                                ),
                                OpType::Set | OpType::Delete => None,
                            };

                            let outcome = if let Some(updated) = updated {
                                let updated = Some(updated);
                                c.query(
                                    &sql_lookup("topic_configuration_upsert.sql")?,
                                    (
                                        self.cluster.as_str(),
                                        resource.resource_name.as_str(),
                                        config.name.as_str(),
                                        updated.as_deref(),
                                    ),
                                )
                                .await
                                .map(|_| ())
                            } else {
                                c.query(
                                    &sql_lookup("topic_configuration_delete.sql")?,
                                    (
                                        self.cluster.as_str(),
                                        resource.resource_name.as_str(),
                                        config.name.as_str(),
                                    ),
                                )
                                .await
                                .map(|_| ())
                            };

                            if outcome.inspect_err(|err| error!(?err)).is_err() {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                    }
                }

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(error_code.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

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
                                    (self.cluster.as_str(), id.to_string().as_str()),
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
                        &[self.cluster.as_str()],
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
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
    }

    pub(super) async fn describe_config_impl(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut rows = c
            .query(
                &sql_lookup("topic_select.sql")?,
                (self.cluster.as_str(), name),
            )
            .await?;

        if rows.next().await?.is_some() {
            let mut rows = c
                .query(
                    &sql_lookup("topic_configuration_select.sql")?,
                    (self.cluster.as_str(), name),
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
                                                .leader_id(self.node)
                                                .leader_epoch(-1)
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
                    match self
                        .prepare_query_one(
                            &c,
                            &sql_lookup("topic_select_uuid.sql")?,
                            (self.cluster.as_str(), id.to_string().as_str()),
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
                                                .leader_id(self.node)
                                                .leader_epoch(-1)
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

        Ok(responses)
    }
}
