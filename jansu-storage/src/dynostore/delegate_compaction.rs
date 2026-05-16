use super::*;
use jansu_sans_io::record::inflated::Batch;
use std::collections::{HashMap, HashSet};

impl DynoStore {
    pub(super) async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let mut deleted = 0;
        let prefix = Path::from(format!("clusters/{}/topics/", self.cluster));
        let mut stream = self.object_store.list(Some(&prefix));

        let mut topics = HashMap::new();
        while let Some(meta) = stream.next().await.transpose().map_err(Error::from)? {
            let path_str = meta.location.to_string();
            if !path_str.contains("/records/") {
                continue;
            }
            let parts: Vec<&str> = path_str.split('/').collect();
            if parts.len() < 8 { continue; }
            let topic_name = parts[3];
            
            if !topics.contains_key(topic_name) {
                let mut retention_ms = Some(Duration::from_secs(7 * 24 * 60 * 60)); // Default 7 days
                if let Ok(Some(metadata)) = self.topic_metadata(&TopicId::Name(topic_name.into())).await {
                    if let Some(configs) = &metadata.topic.configs {
                        for config in configs {
                            if config.name == "retention.ms" {
                                if let Some(val) = &config.value {
                                    if let Ok(ms) = val.parse::<i64>() {
                                        if ms < 0 {
                                            retention_ms = None;
                                        } else {
                                            retention_ms = Some(Duration::from_millis(ms as u64));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                topics.insert(topic_name.to_string(), retention_ms);
            }
            
            if let Some(Some(retention)) = topics.get(topic_name) {
                if let Ok(age) = now.duration_since(meta.last_modified.into()) {
                    if age > *retention {
                        self.object_store.delete(&meta.location).await.map_err(Error::from)?;
                        deleted += 1;
                    }
                }
            }
        }
        Ok(deleted)
    }

    pub(super) async fn policy_compact(&self) -> Result<u64> {
        let mut compacted = 0;
        let prefix = Path::from(format!("clusters/{}/topics/", self.cluster));
        let mut stream = self.object_store.list(Some(&prefix));

        let mut latest_keys: HashMap<(String, i32, Vec<u8>), i64> = HashMap::new();
        let mut all_records = Vec::new();

        while let Some(meta) = stream.next().await.transpose().map_err(Error::from)? {
            let path_str = meta.location.to_string();
            if !path_str.contains("/records/") {
                continue;
            }
            let parts: Vec<&str> = path_str.split('/').collect();
            if parts.len() < 8 { continue; }
            let topic_name = parts[3];
            let partition: i32 = parts[5].parse().unwrap_or(0);
            
            let mut is_compact = false;
            if let Ok(Some(metadata)) = self.topic_metadata(&TopicId::Name(topic_name.into())).await {
                if let Some(configs) = &metadata.topic.configs {
                    for config in configs {
                        if config.name == "cleanup.policy" {
                            if let Some(val) = &config.value {
                                if val.contains("compact") {
                                    is_compact = true;
                                }
                            }
                        }
                    }
                }
            }
            if !is_compact { continue; }

            if let Ok(res) = self.object_store.get(&meta.location).await {
                if let Ok(bytes) = res.bytes().await {
                    if let Ok(mut batch) = self.decode(bytes) {
                        batch.base_offset = parts[7].split('.').next().unwrap().parse().unwrap_or(0);
                        all_records.push((meta.location.clone(), topic_name.to_string(), partition, batch.clone()));
                        if let Ok(inflated_batch) = jansu_sans_io::record::inflated::Batch::try_from(&batch) {
                            for record in inflated_batch.records {
                                if let Some(key) = record.key {
                                    let map_key = (topic_name.to_string(), partition, key.to_vec());
                                    let current = latest_keys.entry(map_key).or_insert(-1);
                                    if batch.base_offset + record.offset_delta as i64 > *current {
                                        *current = batch.base_offset + record.offset_delta as i64;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        for (location, topic_name, partition, batch) in all_records {
            let mut keep = false;
            if let Ok(inflated_batch) = jansu_sans_io::record::inflated::Batch::try_from(&batch) {
                for record in &inflated_batch.records {
                    if let Some(key) = &record.key {
                        let map_key = (topic_name.clone(), partition, key.to_vec());
                        if let Some(latest_offset) = latest_keys.get(&map_key) {
                            if batch.base_offset + record.offset_delta as i64 >= *latest_offset {
                                keep = true;
                            }
                        }
                    } else {
                        keep = true; // no key, keep it
                    }
                }
            } else {
                keep = true; // if we can't inflate it, keep it
            }
            if !keep {
                if self.object_store.delete(&location).await.is_ok() {
                    compacted += 1;
                }
            }
        }

        Ok(compacted)
    }
}
