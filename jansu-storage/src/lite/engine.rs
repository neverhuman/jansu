use super::*;

impl Engine {
    pub fn builder()
    -> Builder<PhantomData<String>, PhantomData<i32>, PhantomData<Url>, PhantomData<Url>> {
        Builder::default()
    }
}

#[async_trait]
impl Storage for Engine {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.register_broker_metered(broker_registration).await
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.brokers_metered().await
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.create_topic_metered(topic, validate_only).await
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.delete_records_metered(topics).await
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.delete_topic_metered(topic).await
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.incremental_alter_resource_metered(resource).await
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        self.produce_metered(transaction_id, topition, deflated)
            .await
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        self.fetch_metered(topition, offset, min_bytes, max_bytes, isolation_level)
            .await
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.offset_stage_metered(topition).await
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.offset_commit_metered(group, retention, offsets).await
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        self.committed_offset_topitions_metered(group_id).await
    }

    #[instrument(skip_all)]
    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        self.offset_for_leader_epoch_metered(topition, leader_epoch)
            .await
    }

    #[instrument(skip_all)]
    async fn leader_epoch_history(&self, topition: &Topition) -> Result<Vec<LeaderEpochRecord>> {
        self.leader_epoch_history_metered(topition).await
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.offset_fetch_metered(group_id, topics, require_stable)
            .await
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.list_offsets_metered(isolation_level, offsets).await
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.metadata_metered(topics).await
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.describe_config_metered(name, resource, keys).await
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.describe_topic_partitions_metered(topics, partition_limit, cursor)
            .await
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.list_groups_metered(states_filter).await
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.delete_groups_metered(group_ids).await
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.describe_groups_metered(group_ids, include_authorized_operations)
            .await
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.update_group_metered(group_id, detail, version).await
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.init_producer_metered(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        )
        .await
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.txn_add_offsets_metered(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.txn_add_partitions_metered(partitions).await
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.txn_offset_commit_metered(offsets).await
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.txn_end_metered(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.maintain_metered(now).await
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        self.cluster_id_metered().await
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        self.node_metered().await
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        self.advertised_listener_metered().await
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.delete_user_scram_credential_metered(user, mechanism)
            .await
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.upsert_user_scram_credential_metered(user, mechanism, credential)
            .await
    }

    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.user_scram_credential_metered(user, mechanism).await
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        self.ping_metered().await
    }
}
