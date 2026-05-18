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

//! Feature/topic lifecycle, broker state, and describe operations for DynoStore.

use super::*;

impl DynoStore {
    pub(super) async fn brokers_inner(&self) -> Result<Vec<DescribeClusterBroker>> {
        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .map_or("0.0.0.0", |h| h)
            .into();
        let port = self.advertised_listener.port().map_or(9092, |p| p).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    pub(super) async fn register_broker_inner(
        &self,
        _broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        Ok(())
    }

    pub(super) async fn incremental_alter_resource_inner(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let _ = resource;

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
                let result = self
                    .meta
                    .with_mut(&self.object_store, |meta| {
                        let configs: Vec<_> = resource
                            .configs
                            .iter()
                            .flat_map(|v| v.iter())
                            .cloned()
                            .collect();
                        meta.alter_topic(resource.resource_name.as_str(), &configs)
                    })
                    .await;

                match result {
                    Ok(()) => Ok(AlterConfigsResourceResponse::default()
                        .error_code(ErrorCode::None.into())
                        .error_message(Some("".into()))
                        .resource_type(resource.resource_type)
                        .resource_name(resource.resource_name)),
                    Err(Error::Api(error_code)) => Ok(AlterConfigsResourceResponse::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(resource.resource_type)
                        .resource_name(resource.resource_name)),
                    Err(error) => Err(error),
                }
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    #[instrument(skip_all, fields(topic = %topic.name))]
    pub(super) async fn create_topic_inner(
        &self,
        topic: CreatableTopic,
        _validate_only: bool,
    ) -> Result<Uuid> {
        Meta::validate_topic_configs(topic.configs.as_deref())?;

        match self
            .meta
            .with_mut(&self.object_store, |meta| {
                if meta.topics.contains_key(topic.name.as_str()) {
                    return Err(Error::Api(ErrorCode::TopicAlreadyExists));
                }

                let id = Uuid::now_v7();
                debug!(%id);

                let td = TopicMetadata {
                    id,
                    topic: topic.clone(),
                };

                assert_eq!(None, meta.topics.insert(topic.name.clone(), td));

                // Initialize leader epoch 0 at offset 0 for each partition.
                for partition in 0..topic.num_partitions {
                    let key = format!("{}:{}", topic.name, partition);
                    _ = meta
                        .leader_epoch_history
                        .entry(key)
                        .or_insert_with(|| vec![(0, 0)]);
                }

                Ok(id)
            })
            .await
        {
            Ok(id) => {
                for partition in 0..topic.num_partitions {
                    let topition = Topition::new(topic.name.as_str(), partition);

                    let watermark = self.watermarks.lock().map(|mut locked| {
                        locked
                            .entry(topition.to_owned())
                            .or_insert(OptiCon::<Watermark>::new(self.cluster.as_str(), &topition))
                            .to_owned()
                    })?;

                    watermark
                        .with_mut(&self.object_store, |watermark| {
                            _ = watermark.high.take();
                            _ = watermark.low.take();

                            Ok(())
                        })
                        .await?;
                }

                Ok(id)
            }

            error @ Err(_) => error,
        }
    }

    pub(super) async fn delete_records_inner(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let mut results = vec![];

        for topic in topics {
            let mut partitions = vec![];

            for partition in topic.partitions.as_ref().unwrap_or(&vec![]) {
                let partition_index = partition.partition_index;
                let offset = partition.offset;

                let topition = Topition::new(topic.name.as_str(), partition_index);

                // Update low watermark
                let low_watermark = {
                    let watermark = self.watermarks.lock().map(|mut locked| {
                        locked
                            .entry(topition.to_owned())
                            .or_insert(OptiCon::<Watermark>::new(self.cluster.as_str(), &topition))
                            .to_owned()
                    })?;

                    watermark
                        .with_mut(&self.object_store, |watermark| {
                            if watermark.low.unwrap_or(0) < offset {
                                watermark.low = Some(offset);
                            }
                            Ok(watermark.low.unwrap_or(0))
                        })
                        .await?
                };

                // Delete objects < offset
                let location = Path::from(format!(
                    "clusters/{}/topics/{}/partitions/{:0>10}/records/",
                    self.cluster, topic.name, partition_index
                ));

                let locations = self
                    .object_store
                    .list(Some(&location))
                    .filter_map(move |m| async move {
                        m.map_or(None, |m| {
                            let part = m.location.parts().next_back()?;
                            if let Ok(record_offset) = i64::from_str(&part.as_ref()[0..20])
                                && record_offset < offset
                            {
                                return Some(Ok(m.location.clone()));
                            }
                            None
                        })
                    })
                    .boxed();

                _ = self
                    .object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await;

                partitions.push(
                    DeleteRecordsPartitionResult::default()
                        .partition_index(partition_index)
                        .low_watermark(low_watermark)
                        .error_code(i16::from(ErrorCode::None)),
                );
            }

            results.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partitions)),
            );
        }

        Ok(results)
    }

    pub(super) async fn delete_topic_inner(&self, topic: &TopicId) -> Result<ErrorCode> {
        if let Some(metadata) = self.topic_metadata(topic).await? {
            self.meta
                .with_mut(&self.object_store, |meta| {
                    _ = meta.topics.remove(metadata.topic.name.as_str());
                    Ok(())
                })
                .await?;

            let prefix = Path::from(format!(
                "clusters/{}/topics/{}/",
                self.cluster, metadata.topic.name,
            ));

            let locations = self
                .object_store
                .list(Some(&prefix))
                .map_ok(|m| m.location)
                .boxed();

            _ = self
                .object_store
                .delete_stream(locations)
                .try_collect::<Vec<Path>>()
                .await?;

            let prefix = Path::from(format!("clusters/{}/groups/consumers/", self.cluster));

            let topic_name = metadata.topic.name.clone();
            let prefix_clone = prefix.clone();
            let locations = self
                .object_store
                .list(Some(&prefix))
                .filter_map(move |m| {
                    let prefix = prefix_clone.clone();
                    let topic_name = topic_name.clone();
                    async move {
                        m.map_or(None, |m| {
                            debug!(?m.location);

                            m.location.prefix_match(&prefix).and_then(|mut i| {
                                // skip over the consumer group name
                                _ = i.next();

                                let sub = Path::from_iter(i);
                                debug!(?sub);

                                if sub.prefix_matches(&Path::from(format!(
                                    "offsets/{}/partitions/",
                                    topic_name
                                ))) {
                                    Some(Ok(m.location.clone()))
                                } else {
                                    None
                                }
                            })
                        })
                    }
                })
                .boxed();

            _ = self
                .object_store
                .delete_stream(locations)
                .try_collect::<Vec<Path>>()
                .await?;
            self.meta
                .with_mut(&self.object_store, |meta| {
                    meta.leader_epoch_history
                        .retain(|key, _| !key.starts_with(metadata.topic.name.as_str()));
                    Ok(())
                })
                .await?;
            Ok(ErrorCode::None)
        } else {
            Ok(ErrorCode::UnknownTopicOrPartition)
        }
    }

    pub(super) async fn maintain_inner(&self, now: SystemTime) -> Result<()> {
        if let Some(ref lake) = self.lake {
            lake.maintain()
                .await
                .inspect(|maintain| debug!(?maintain))
                .inspect_err(|err| debug!(?err))
                .map_err(Error::from)?;
        }

        let deleted = self.policy_delete(now).await?;
        debug!(deleted);

        let compacted = self.policy_compact().await?;
        debug!(compacted);

        let prefix = Path::from(format!("clusters/{}/groups/consumers/", self.cluster));
        let mut list_stream = self.object_store.list(Some(&prefix));

        while let Some(meta) = list_stream.next().await.transpose()? {
            let location = meta.location;
            let location_str = location.to_string();

            if !location_str.contains("/offsets/") {
                continue;
            }

            let record = match self.object_store.get(&location).await {
                Ok(get_result) => {
                    get_result
                        .bytes()
                        .await
                        .map_err(Error::from)
                        .and_then(|encoded| {
                            serde_json::from_slice::<OffsetFetchRecord>(&encoded[..])
                                .map_err(Error::from)
                        })
                }

                Err(object_store::Error::NotFound { .. }) => continue,

                Err(error) => {
                    debug!(?error, ?location);
                    continue;
                }
            }?;

            if record.expired(now) {
                self.object_store
                    .delete(&location)
                    .await
                    .inspect(|outcome| {
                        debug!(?location, ?outcome);
                    })?;
            }
        }

        Ok(())
    }

    pub(super) async fn cluster_id_inner(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    pub(super) async fn node_inner(&self) -> Result<i32> {
        Ok(self.node)
    }

    pub(super) async fn advertised_listener_inner(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    #[instrument(skip_all)]
    pub(super) async fn delete_user_scram_credential_inner(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Ok(())
    }

    pub(super) async fn upsert_user_scram_credential_inner(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Ok(())
    }

    pub(super) async fn user_scram_credential_inner(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Ok(None)
    }

    #[instrument(skip_all)]
    pub(super) async fn ping_inner(&self) -> Result<()> {
        // Verify connectivity by listing objects at the root
        let _ = self.object_store.list(Some(&Path::from("/"))).next().await;
        Ok(())
    }
}
