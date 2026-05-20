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

//! Topic create/delete, config alter and record-deletion dispatch.
//!
//! Inherent `DynoStore` methods backing the `Storage` trait implementation.

use super::*;

impl DynoStore {
    pub(crate) async fn incremental_alter_resource_dispatch(
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
            ConfigResource::Topic => self
                .meta
                .with_mut(&self.object_store, |meta| {
                    let configs = match resource.configs.as_deref() {
                        Some(configs) => configs,
                        None => &[],
                    };

                    meta.alter_topic(resource.resource_name.as_str(), configs)
                })
                .await
                .map(|()| {
                    AlterConfigsResourceResponse::default()
                        .error_code(ErrorCode::None.into())
                        .error_message(Some("".into()))
                        .resource_type(resource.resource_type)
                        .resource_name(resource.resource_name)
                }),
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    pub(crate) async fn create_topic_dispatch(
        &self,
        topic: CreatableTopic,
        _validate_only: bool,
    ) -> Result<Uuid> {
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

    pub(crate) async fn delete_records_dispatch(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let mut results = Vec::with_capacity(topics.len());

        for topic in topics {
            let metadata = self
                .topic_metadata(&TopicId::Name(topic.name.clone()))
                .await?;
            let mut partition_results = vec![];

            let Some(partitions) = topic.partitions.as_deref() else {
                results.push(
                    DeleteRecordsTopicResult::default()
                        .name(topic.name.clone())
                        .partitions(Some(partition_results)),
                );
                continue;
            };

            for partition in partitions {
                let (error_code, low_watermark) = if let Some(metadata) = metadata.as_ref() {
                    if partition.partition_index < 0
                        || partition.partition_index >= metadata.topic.num_partitions
                    {
                        (ErrorCode::UnknownTopicOrPartition, 0)
                    } else {
                        let topition =
                            Topition::new(topic.name.as_str(), partition.partition_index);
                        let watermark = self.watermarks.lock().map(|mut locked| {
                            locked
                                .entry(topition.to_owned())
                                .or_insert_with(|| {
                                    OptiCon::<Watermark>::new(self.cluster.as_str(), &topition)
                                })
                                .to_owned()
                        })?;

                        let low_watermark = watermark
                            .with_mut(&self.object_store, |watermark| {
                                let high = match watermark.high {
                                    Some(high) => high,
                                    None => 0,
                                };
                                let requested = partition.offset.clamp(0, high);
                                let low = match watermark.low {
                                    Some(low) => low.max(requested),
                                    None => requested,
                                };
                                watermark.low = Some(low);
                                if let Some(timestamps) = watermark.timestamps.as_mut() {
                                    timestamps.retain(|_, offset| *offset >= low);
                                }
                                Ok(low)
                            })
                            .await?;

                        let prefix = Path::from(format!(
                            "clusters/{}/topics/{}/partitions/{:0>10}/records/",
                            self.cluster, topic.name, partition.partition_index,
                        ));
                        let mut list_stream = self.object_store.list(Some(&prefix));
                        while let Some(meta) = list_stream.next().await.transpose()? {
                            let Some(offset) = meta
                                .location
                                .parts()
                                .next_back()
                                .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                            else {
                                continue;
                            };

                            if offset < low_watermark {
                                self.object_store.delete(&meta.location).await?;
                            }
                        }

                        (ErrorCode::None, low_watermark)
                    }
                } else {
                    (ErrorCode::UnknownTopicOrPartition, 0)
                };

                partition_results.push(
                    DeleteRecordsPartitionResult::default()
                        .partition_index(partition.partition_index)
                        .low_watermark(low_watermark)
                        .error_code(error_code.into()),
                );
            }

            results.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_results)),
            );
        }

        Ok(results)
    }

    pub(crate) async fn delete_topic_dispatch(&self, topic: &TopicId) -> Result<ErrorCode> {
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
}
