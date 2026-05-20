//! Metered delegation for the libSQL channel `Engine` (metadata, group, and txn APIs).
//!
//! Companion to `engine_dispatch_a.rs`; see that file for the delegation pattern.

use super::*;

impl Engine {
    pub(super) async fn metadata_metered(
        &self,
        topics: Option<&[TopicId]>,
    ) -> Result<MetadataResponse> {
        let start = SystemTime::now();
        self.inner.metadata(topics).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "metadata")],
            )
        })
    }

    pub(super) async fn describe_config_metered(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let start = SystemTime::now();
        self.inner
            .describe_config(name, resource, keys)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_config")],
                )
            })
    }

    pub(super) async fn describe_topic_partitions_metered(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let start = SystemTime::now();
        self.inner
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_topic_partitions")],
                )
            })
    }

    pub(super) async fn list_groups_metered(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        let start = SystemTime::now();
        self.inner.list_groups(states_filter).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "list_groups")],
            )
        })
    }

    pub(super) async fn delete_groups_metered(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let start = SystemTime::now();
        self.inner.delete_groups(group_ids).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_groups")],
            )
        })
    }

    pub(super) async fn describe_groups_metered(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();
        self.inner
            .describe_groups(group_ids, include_authorized_operations)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_groups")],
                )
            })
    }

    pub(super) async fn update_group_metered(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let start = SystemTime::now();
        self.inner
            .update_group(group_id, detail, version)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "update_group")],
                )
            })
    }

    pub(super) async fn init_producer_metered(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let start = SystemTime::now();
        self.inner
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "init_producer")],
                )
            })
    }

    pub(super) async fn txn_add_offsets_metered(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "txn_add_offsets")],
                )
            })
    }

    pub(super) async fn txn_add_partitions_metered(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let start = SystemTime::now();
        self.inner
            .txn_add_partitions(partitions)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "txn_add_partitions")],
                )
            })
    }

    pub(super) async fn txn_offset_commit_metered(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let start = SystemTime::now();
        self.inner.txn_offset_commit(offsets).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_offset_commit")],
            )
        })
    }

    pub(super) async fn txn_end_metered(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "txn_end")],
                )
            })
    }

    pub(super) async fn maintain_metered(&self, now: SystemTime) -> Result<()> {
        let start = SystemTime::now();
        self.inner.maintain(now).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "maintain")],
            )
        })
    }

    pub(super) async fn cluster_id_metered(&self) -> Result<String> {
        let start = SystemTime::now();
        self.inner.cluster_id().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "cluster_id")],
            )
        })
    }

    pub(super) async fn node_metered(&self) -> Result<i32> {
        let start = SystemTime::now();
        self.inner.node().await.inspect(|_| {
            ENGINE_REQUEST_DURATION
                .record(elapsed_millis(start), &[KeyValue::new("operation", "node")])
        })
    }

    pub(super) async fn advertised_listener_metered(&self) -> Result<Url> {
        let start = SystemTime::now();
        self.inner.advertised_listener().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "advertised_listener")],
            )
        })
    }

    pub(super) async fn delete_user_scram_credential_metered(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .delete_user_scram_credential(user, mechanism)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "delete_user_scram_credential")],
                )
            })
    }

    pub(super) async fn upsert_user_scram_credential_metered(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "upsert_user_scram_credential")],
                )
            })
    }

    pub(super) async fn user_scram_credential_metered(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let start = SystemTime::now();
        self.inner
            .user_scram_credential(user, mechanism)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "user_scram_credential")],
                )
            })
    }

    pub(super) async fn ping_metered(&self) -> Result<()> {
        let start = SystemTime::now();
        self.inner.ping().await.inspect(|_| {
            ENGINE_REQUEST_DURATION
                .record(elapsed_millis(start), &[KeyValue::new("operation", "ping")])
        })
    }
}
