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

//! Topic lifecycle and configuration operations for the SlateDB engine.

use std::collections::BTreeMap;

use jansu_sans_io::{
    ConfigResource, ErrorCode, OpType,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};
use tracing::debug;
use uuid::Uuid;

use crate::{Error, Result, TopicId};

use super::super::engine::Engine;
use super::super::types::{
    LeaderEpochKey, LeaderEpochKeyPrefix, LeaderEpochValue, OffsetCommitKey, Producers,
    TopicMetadata, Topics, Transactions,
};

impl Engine {
    pub(in crate::slate) async fn create_topic_op(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
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

        if validate_only {
            return Ok(Uuid::nil());
        }

        let id = Uuid::now_v7();
        let num_partitions = topic.num_partitions;
        let td = TopicMetadata { id, topic };

        _ = topics.insert(name, td);
        self.save_metadata(&tx, Self::TOPICS, &topics)?;

        for partition in 0..num_partitions {
            let key = postcard::to_stdvec(&LeaderEpochKey::new(id, partition, 0))?;
            let value = postcard::to_stdvec(&LeaderEpochValue { start_offset: 0 })?;
            tx.put(key, value)?;
        }

        tx.commit().await.map_err(Error::from).and(Ok(id))
    }

    pub(in crate::slate) async fn delete_topic_op(&self, topic: &TopicId) -> Result<ErrorCode> {
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
            self.delete_partition_batches(&tx, topic_metadata.id, partition)
                .await?;
        }

        // 2. Delete all watermarks for this topic
        for partition in 0..topic_metadata.topic.num_partitions {
            let watermark_key = postcard::to_stdvec(&super::super::types::WatermarkKey::new(
                topic_metadata.id,
                partition,
            ))?;
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

    async fn delete_partition_batches(
        &self,
        tx: &slatedb::DbTransaction,
        topic_id: Uuid,
        partition: i32,
    ) -> Result<()> {
        use super::super::types::{BatchKey, BatchKeyPrefix};

        let batch_prefix = postcard::to_stdvec(&BatchKeyPrefix::new(topic_id, partition))?;
        let scan_start = postcard::to_stdvec(&BatchKey::scan_from(topic_id, partition, 0))?;

        let mut scan = self.db.scan(scan_start..).await?;
        while let Some(kv) = scan.next().await? {
            if !kv.key.starts_with(&batch_prefix) {
                break;
            }
            tx.delete(&kv.key)?;
        }
        Ok(())
    }

    pub(in crate::slate) async fn incremental_alter_resource_op(
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
                    let mut configuration: BTreeMap<String, Option<String>> = metadata
                        .topic
                        .configs
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .fold(BTreeMap::new(), |mut acc, item| {
                            _ = acc.insert(item.name.clone(), item.value.clone());
                            acc
                        });

                    for change in resource.configs.as_deref().unwrap_or(&[]) {
                        match OpType::try_from(change.config_operation)? {
                            OpType::Set => {
                                _ = configuration.insert(change.name.clone(), change.value.clone());
                            }
                            OpType::Delete => {
                                _ = configuration.remove(change.name.as_str());
                            }
                            OpType::Append => {
                                if let Some(updated) = crate::append_config_tokens(
                                    configuration
                                        .get(change.name.as_str())
                                        .and_then(|value| value.as_deref()),
                                    change.value.as_deref(),
                                ) {
                                    _ = configuration.insert(change.name.clone(), Some(updated));
                                }
                            }
                            OpType::Subtract => {
                                if let Some(updated) = crate::subtract_config_tokens(
                                    configuration
                                        .get(change.name.as_str())
                                        .and_then(|value| value.as_deref()),
                                    change.value.as_deref(),
                                ) {
                                    _ = configuration.insert(change.name.clone(), Some(updated));
                                } else {
                                    _ = configuration.remove(change.name.as_str());
                                }
                            }
                        }
                    }

                    _ = metadata.topic.configs.replace(
                        configuration
                            .into_iter()
                            .map(|(key, value)| {
                                CreatableTopicConfig::default().name(key).value(value)
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
