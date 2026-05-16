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

//! Broker and topic admin Storage impl helpers: register_broker, brokers, create_topic,
//! delete_records, delete_topic, incremental_alter_resource

use std::collections::BTreeMap;

use jansu_sans_io::{
    ConfigResource, ErrorCode, OpType,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};
use tracing::debug;
use uuid::Uuid;

use crate::{BrokerRegistrationRequest, Error, Result, TopicId};

use super::engine::Engine;
use super::types::{
    BatchKey, BatchKeyPrefix, BrokerInfo, Brokers, LeaderEpochKey, LeaderEpochKeyPrefix,
    LeaderEpochValue, OffsetCommitKey, Producers, Topics, Transactions, Watermark, WatermarkKey,
};

impl Engine {
    pub(super) async fn impl_register_broker(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        debug!(?broker_registration);

        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut brokers: Brokers = self.load_metadata(&tx, Self::BROKERS).await?;

        // Persist broker info to storage
        // NOTE: This is stored permanently - no cleanup mechanism exists yet
        let broker_info = BrokerInfo {
            broker_id: self.node,
            host: self
                .advertised_listener
                .host_str()
                .unwrap_or("0.0.0.0")
                .into(),
            port: self.advertised_listener.port().unwrap_or(9092).into(),
            rack: broker_registration.rack,
        };

        _ = brokers.insert(self.node, broker_info);
        self.save_metadata(&tx, Self::BROKERS, &brokers)?;

        tx.commit().await.map_err(Error::from)?;

        Ok(())
    }

    pub(super) async fn impl_brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let stored_brokers = self
            .db
            .get(Self::BROKERS)
            .await
            .map_err(Error::from)
            .and_then(|brokers| {
                brokers.map_or(Ok(Brokers::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        if stored_brokers.is_empty() {
            // Return self as the only broker if no registrations yet
            let broker_id = self.node;
            let host = self
                .advertised_listener
                .host_str()
                .unwrap_or("0.0.0.0")
                .into();
            let port = self.advertised_listener.port().unwrap_or(9092).into();

            Ok(vec![
                DescribeClusterBroker::default()
                    .broker_id(broker_id)
                    .host(host)
                    .port(port)
                    .rack(None),
            ])
        } else {
            Ok(stored_brokers
                .values()
                .map(|info| {
                    DescribeClusterBroker::default()
                        .broker_id(info.broker_id)
                        .host(info.host.clone())
                        .port(info.port)
                        .rack(info.rack.clone())
                })
                .collect())
        }
    }

    pub(super) async fn impl_create_topic(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        let _ = validate_only;
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        // NOTE: Contention Hotspot
        // Reading the entire TOPICS map creates a serialization bottleneck and high conflict rate
        // for concurrent topic creation.
        let mut topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let name = topic.name.clone();

        if topics.contains_key(&name[..]) {
            return Err(Error::Api(ErrorCode::TopicAlreadyExists));
        }

        let id = Uuid::now_v7();
        let num_partitions = topic.num_partitions;
        let td = super::types::TopicMetadata { id, topic };

        _ = topics.insert(name, td);
        self.save_metadata(&tx, Self::TOPICS, &topics)?;

        for partition in 0..num_partitions {
            let key = postcard::to_stdvec(&LeaderEpochKey::new(id, partition, 0))?;
            let value = postcard::to_stdvec(&LeaderEpochValue { start_offset: 0 })?;
            tx.put(key, value)?;
        }

        tx.commit().await.map_err(Error::from).and(Ok(id))
    }

    /// Delete records up to a specified offset.
    ///
    /// Physically deletes batch data below the specified offset and updates
    /// the low watermark. This aligns with PG's delete_records implementation.
    pub(super) async fn impl_delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let all_topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let mut results = Vec::with_capacity(topics.len());

        for topic in topics {
            let mut partition_results = vec![];

            let topic_metadata = all_topics.get(&topic.name[..]);

            if let Some(partitions) = topic.partitions.as_ref() {
                for partition in partitions {
                    let (error_code, low_watermark) = if let Some(metadata) = topic_metadata {
                        if partition.partition_index < 0
                            || partition.partition_index >= metadata.topic.num_partitions
                        {
                            (ErrorCode::UnknownTopicOrPartition, 0)
                        } else {
                            // Delete batches below the specified offset
                            let batch_prefix = postcard::to_stdvec(&BatchKeyPrefix::new(
                                metadata.id,
                                partition.partition_index,
                            ))?;
                            let scan_start = postcard::to_stdvec(&BatchKey::scan_from(
                                metadata.id,
                                partition.partition_index,
                                0,
                            ))?;

                            let mut scan = self.db.scan(scan_start..).await?;
                            while let Some(kv) = scan.next().await? {
                                if !kv.key.starts_with(&batch_prefix) {
                                    break;
                                }

                                let batch_key: BatchKey = match postcard::from_bytes(&kv.key) {
                                    Ok(key) => key,
                                    Err(_) => continue,
                                };

                                // Delete batches with offset < specified offset
                                if batch_key.offset >= partition.offset {
                                    break;
                                }

                                tx.delete(&kv.key)?;
                            }

                            // The new low watermark is the requested offset
                            let new_low_watermark = partition.offset;

                            // Update the watermark
                            let watermark_key = postcard::to_stdvec(&WatermarkKey::new(
                                metadata.id,
                                partition.partition_index,
                            ))?;

                            let mut watermark =
                                tx.get(&watermark_key).await.map_err(Error::from).and_then(
                                    |watermark| {
                                        watermark.map_or(Ok(Watermark::default()), |encoded| {
                                            postcard::from_bytes(&encoded[..]).map_err(Into::into)
                                        })
                                    },
                                )?;

                            watermark.low = Some(new_low_watermark);

                            // Remove timestamps before the new low watermark
                            if let Some(ref mut timestamps) = watermark.timestamps {
                                timestamps.retain(|_, offset| *offset >= new_low_watermark);
                            }

                            let watermark_value = postcard::to_stdvec(&watermark)?;
                            tx.put(&watermark_key, watermark_value)?;

                            (ErrorCode::None, new_low_watermark)
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
            }

            results.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_results)),
            );
        }

        tx.commit().await.map_err(Error::from)?;

        Ok(results)
    }

    /// Delete a topic from the cluster.
    ///
    /// Deletes all associated data: batches, watermarks, consumer offsets,
    /// producer sequences, and transaction data. This aligns with PG's
    /// delete_topic implementation.
    pub(super) async fn impl_delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let (topic_name, topic_metadata) = match topic {
            TopicId::Name(name) => {
                if let Some(metadata) = topics.get(&name[..]) {
                    (name.clone(), metadata.clone())
                } else {
                    return Ok(ErrorCode::UnknownTopicOrPartition);
                }
            }
            TopicId::Id(id) => {
                if let Some((name, metadata)) = topics.iter().find(|(_, tm)| tm.id == *id) {
                    (name.clone(), metadata.clone())
                } else {
                    return Ok(ErrorCode::UnknownTopicOrPartition);
                }
            }
        };

        // 1. Delete all batches for this topic
        for partition in 0..topic_metadata.topic.num_partitions {
            let batch_prefix =
                postcard::to_stdvec(&BatchKeyPrefix::new(topic_metadata.id, partition))?;
            let scan_start =
                postcard::to_stdvec(&BatchKey::scan_from(topic_metadata.id, partition, 0))?;

            let mut scan = self.db.scan(scan_start..).await?;
            while let Some(kv) = scan.next().await? {
                if !kv.key.starts_with(&batch_prefix) {
                    break;
                }
                tx.delete(&kv.key)?;
            }
        }

        // 2. Delete all watermarks for this topic
        for partition in 0..topic_metadata.topic.num_partitions {
            let watermark_key =
                postcard::to_stdvec(&WatermarkKey::new(topic_metadata.id, partition))?;
            tx.delete(&watermark_key)?;
        }

        // 3. Delete all leader epoch history for this topic
        for partition in 0..topic_metadata.topic.num_partitions {
            let leader_epoch_prefix =
                postcard::to_stdvec(&LeaderEpochKeyPrefix::new(topic_metadata.id, partition))?;

            let mut scan = self.db.scan(leader_epoch_prefix.clone()..).await?;
            while let Some(kv) = scan.next().await? {
                if !kv.key.starts_with(&leader_epoch_prefix) {
                    break;
                }
                tx.delete(&kv.key)?;
            }
        }

        // 4. Delete consumer offsets for this topic (scan all groups)
        // Use just the prefix character 'c' to scan all consumer offsets
        let scan_start = vec![b'c'];
        let mut scan = self.db.scan(scan_start..).await?;
        while let Some(kv) = scan.next().await? {
            // Stop if we've moved past the 'c' prefix
            if kv.key.first() != Some(&b'c') {
                break;
            }
            // Try to decode and check if it's for this topic
            if let Ok(key) = postcard::from_bytes::<OffsetCommitKey>(&kv.key)
                && key.topic == topic_name
            {
                tx.delete(&kv.key)?;
            }
        }

        // 5. Clean up producer sequences for this topic
        let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;
        for producer_detail in producers.values_mut() {
            for epoch_sequences in producer_detail.sequences.values_mut() {
                _ = epoch_sequences.remove(&topic_name);
            }
        }
        self.save_metadata(&tx, Self::PRODUCERS, &producers)?;

        // 6. Clean up transaction data for this topic
        let mut transactions: Transactions = self.load_metadata(&tx, Self::TRANSACTIONS).await?;
        for txn in transactions.values_mut() {
            for txn_detail in txn.epochs.values_mut() {
                _ = txn_detail.produces.remove(&topic_name);
                for group_offsets in txn_detail.offsets.values_mut() {
                    _ = group_offsets.remove(&topic_name);
                }
            }
        }
        self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

        // 7. Remove topic from metadata
        _ = topics.remove(&topic_name);
        self.save_metadata(&tx, Self::TOPICS, &topics)?;

        tx.commit().await.map_err(Error::from)?;

        Ok(ErrorCode::None)
    }

    pub(super) async fn impl_incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Topic => {
                let tx = self
                    .db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .inspect_err(|err| debug!(?err))?;

                let mut topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

                if let Some(metadata) = topics.get_mut(&resource.resource_name[..]) {
                    // Build current config map
                    let mut configuration: BTreeMap<String, Option<String>> = metadata
                        .topic
                        .configs
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .fold(BTreeMap::new(), |mut acc, item| {
                            _ = acc.insert(item.name.clone(), item.value.clone());
                            acc
                        });

                    // Apply changes
                    for change in resource.configs.as_deref().unwrap_or_default() {
                        match OpType::try_from(change.config_operation)? {
                            OpType::Set => {
                                _ = configuration
                                    .insert(change.name.clone(), change.value.clone());
                            }
                            OpType::Delete => {
                                _ = configuration.remove(change.name.as_str());
                            }
                            OpType::Append => {
                                if let Some(new_val) = &change.value {
                                    let mut list = configuration
                                        .get(change.name.as_str())
                                        .and_then(|v| v.as_deref())
                                        .map(|s| s.split(',').map(str::trim).filter(|s| !s.is_empty()).collect::<Vec<_>>())
                                        .unwrap_or_default();
                                    
                                    if !list.contains(&new_val.as_str()) {
                                        list.push(new_val.as_str());
                                    }
                                    
                                    _ = configuration.insert(change.name.clone(), Some(list.join(",")));
                                }
                            }
                            OpType::Subtract => {
                                if let Some(del_val) = &change.value {
                                    let list = configuration
                                        .get(change.name.as_str())
                                        .and_then(|v| v.as_deref())
                                        .map(|s| s.split(',').map(str::trim).filter(|s| !s.is_empty() && *s != del_val.as_str()).collect::<Vec<_>>())
                                        .unwrap_or_default();
                                        
                                    if list.is_empty() {
                                        _ = configuration.remove(change.name.as_str());
                                    } else {
                                        _ = configuration.insert(change.name.clone(), Some(list.join(",")));
                                    }
                                }
                            }
                        }
                    }

                    // Convert back to configs vec
                    _ = metadata.topic.configs.replace(
                        configuration
                            .into_iter()
                            .map(|(key, value)| {
                                CreatableTopicConfig::default()
                                    .name(key)
                                    .value(value)
                            })
                            .collect(),
                    );

                    self.save_metadata(&tx, Self::TOPICS, &topics)?;
                    tx.commit().await.map_err(Error::from)?;
                }

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
            }
            // For other resource types, just return success
            _ => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }
}
