    use std::sync::Arc;

    use super::*;
    use async_trait::async_trait;
    use jansu_sans_io::{
        ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
        create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        list_groups_response::ListedGroup, record::deflated,
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;
    use url::Url;
    use uuid::Uuid;

    #[derive(Clone, Debug, Default)]
    struct BlockingFetchStorage {
        gate: Arc<Notify>,
    }

    #[async_trait]
    impl Storage for BlockingFetchStorage {
        async fn register_broker(
            &self,
            _broker_registration: BrokerRegistrationRequest,
        ) -> Result<()> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn create_topic(&self, _topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn incremental_alter_resource(
            &self,
            _resource: AlterConfigsResource,
        ) -> Result<AlterConfigsResourceResponse> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn delete_records(
            &self,
            _topics: &[DeleteRecordsTopic],
        ) -> Result<Vec<DeleteRecordsTopicResult>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn produce(
            &self,
            _transaction_id: Option<&str>,
            _topition: &Topition,
            _batch: deflated::Batch,
        ) -> Result<i64> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn fetch(
            &self,
            _topition: &Topition,
            _offset: i64,
            _min_bytes: u32,
            _max_bytes: u32,
            _isolation: IsolationLevel,
        ) -> Result<Vec<deflated::Batch>> {
            self.gate.notified().await;
            Ok(vec![])
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn list_offsets(
            &self,
            _isolation_level: IsolationLevel,
            _offsets: &[(Topition, ListOffset)],
        ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn offset_commit(
            &self,
            _group_id: &str,
            _retention_time_ms: Option<Duration>,
            _offsets: &[(Topition, OffsetCommitRequest)],
        ) -> Result<Vec<(Topition, ErrorCode)>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn offset_for_leader_epoch(
            &self,
            _topition: &Topition,
            _leader_epoch: i32,
        ) -> Result<Option<(i32, i64)>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn offset_fetch(
            &self,
            _group_id: Option<&str>,
            _topics: &[Topition],
            _require_stable: Option<bool>,
        ) -> Result<BTreeMap<Topition, i64>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn offset_fetch_records(
            &self,
            _group_id: Option<&str>,
            _topics: &[Topition],
            _require_stable: Option<bool>,
        ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn committed_offset_topitions(
            &self,
            _group_id: &str,
        ) -> Result<BTreeMap<Topition, i64>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn upsert_user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
            _credential: ScramCredential,
        ) -> Result<()> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn delete_user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
        ) -> Result<()> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
        ) -> Result<Option<ScramCredential>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn describe_config(
            &self,
            _name: &str,
            _resource: ConfigResource,
            _keys: Option<&[String]>,
        ) -> Result<DescribeConfigsResult> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn delete_groups(
            &self,
            _group_ids: Option<&[String]>,
        ) -> Result<Vec<DeletableGroupResult>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn describe_groups(
            &self,
            _group_ids: Option<&[String]>,
            _include_authorized_operations: bool,
        ) -> Result<Vec<NamedGroupDetail>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn describe_topic_partitions(
            &self,
            _topics: Option<&[TopicId]>,
            _partition_limit: i32,
            _cursor: Option<Topition>,
        ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn update_group(
            &self,
            _group_id: &str,
            _detail: GroupDetail,
            _version: Option<Version>,
        ) -> Result<Version, UpdateError<GroupDetail>> {
            Err(UpdateError::Error(Error::Api(ErrorCode::UnknownServerError)))
        }

        async fn init_producer(
            &self,
            _transaction_id: Option<&str>,
            _transaction_timeout_ms: i32,
            _producer_id: Option<i64>,
            _producer_epoch: Option<i16>,
        ) -> Result<ProducerIdResponse> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn txn_add_offsets(
            &self,
            _transaction_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
            _group_id: &str,
        ) -> Result<ErrorCode> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn txn_add_partitions(
            &self,
            _partitions: TxnAddPartitionsRequest,
        ) -> Result<TxnAddPartitionsResponse> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn txn_offset_commit(
            &self,
            _offsets: TxnOffsetCommitRequest,
        ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn txn_end(
            &self,
            _transaction_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
            _committed: bool,
        ) -> Result<ErrorCode> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn cluster_id(&self) -> Result<String> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn node(&self) -> Result<i32> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn advertised_listener(&self) -> Result<Url> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }

        async fn ping(&self) -> Result<()> {
            Err(Error::Api(ErrorCode::UnknownServerError))
        }
    }

    #[tokio::test]
    async fn fetch_request_is_cancelled() {
        let storage = BlockingFetchStorage::default();
        let service = RequestStorageService::new(storage);
        let mut ctx = Context::default();
        let cancellation = CancellationToken::new();
        _ = ctx.insert(cancellation.clone());

        let request = Request::Fetch {
            topition: Topition::new(String::from("topic"), 0),
            offset: 0,
            min_bytes: 1,
            max_bytes: 1024,
            isolation: IsolationLevel::ReadUncommitted,
        };

        let handle = tokio::spawn(async move { service.serve(ctx, request).await });

        tokio::task::yield_now().await;
        cancellation.cancel();

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("fetch request should finish promptly")
            .expect("join should succeed");

        assert!(matches!(result, Err(Error::Cancelled)));
    }
