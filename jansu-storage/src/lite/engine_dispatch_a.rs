//! Metered delegation for the libSQL channel `Engine` (cluster, topic, and record APIs).
//!
//! Each inherent method records `ENGINE_REQUEST_DURATION` and forwards the request
//! to the wrapped `RequestChannelService`. The `Storage` trait impl in `engine.rs`
//! is a thin set of delegators to these methods.

use super::*;

impl Engine {
    pub(super) async fn register_broker_metered(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .register_broker(broker_registration)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "register_broker")],
                )
            })
    }

    pub(super) async fn brokers_metered(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();
        self.inner.brokers().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "brokers")],
            )
        })
    }

    pub(super) async fn create_topic_metered(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> Result<Uuid> {
        let start = SystemTime::now();
        self.inner
            .create_topic(topic, validate_only)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "create_topic")],
                )
            })
    }

    pub(super) async fn delete_records_metered(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let start = SystemTime::now();
        self.inner.delete_records(topics).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_records")],
            )
        })
    }

    pub(super) async fn delete_topic_metered(&self, topic: &TopicId) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner.delete_topic(topic).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_topic")],
            )
        })
    }

    pub(super) async fn incremental_alter_resource_metered(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let start = SystemTime::now();
        self.inner
            .incremental_alter_resource(resource)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "incremental_alter_resource")],
                )
            })
    }

    pub(super) async fn produce_metered(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let start = SystemTime::now();
        self.inner
            .produce(transaction_id, topition, deflated)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "produce")],
                )
            })
    }

    pub(super) async fn fetch_metered(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let start = SystemTime::now();
        self.inner
            .fetch(topition, offset, min_bytes, max_bytes, isolation_level)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "fetch")],
                )
            })
    }

    pub(super) async fn offset_stage_metered(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();
        self.inner.offset_stage(topition).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_stage")],
            )
        })
    }

    pub(super) async fn offset_commit_metered(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let start = SystemTime::now();
        self.inner
            .offset_commit(group, retention, offsets)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "offset_commit")],
                )
            })
    }

    pub(super) async fn committed_offset_topitions_metered(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();
        self.inner
            .committed_offset_topitions(group_id)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "committed_offset_topitions")],
                )
            })
    }

    pub(super) async fn offset_for_leader_epoch_metered(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        let start = SystemTime::now();
        self.inner
            .offset_for_leader_epoch(topition, leader_epoch)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "offset_for_leader_epoch")],
                )
            })
    }

    pub(super) async fn leader_epoch_history_metered(
        &self,
        topition: &Topition,
    ) -> Result<Vec<LeaderEpochRecord>> {
        let start = SystemTime::now();
        self.inner
            .leader_epoch_history(topition)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "leader_epoch_history")],
                )
            })
    }

    pub(super) async fn offset_fetch_metered(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();
        self.inner
            .offset_fetch(group_id, topics, require_stable)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "offset_fetch")],
                )
            })
    }

    pub(super) async fn list_offsets_metered(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();
        self.inner
            .list_offsets(isolation_level, offsets)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "list_offsets")],
                )
            })
    }
}
