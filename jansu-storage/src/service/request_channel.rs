use super::*;
#[derive(Clone, Debug)]
pub struct RequestChannelService {
    tx: RequestSender,
}

impl RequestChannelService {
    pub fn new(tx: RequestSender) -> Self {
        Self { tx }
    }

    fn elapsed_millis(&self, start: SystemTime) -> u64 {
        start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64)
    }
}

static STORAGE_CHANNEL_CAPACITY: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("jansu_storage_channel_capacity")
        .with_description("Storage channel capacity")
        .build()
});

impl<State> Service<State, Request> for RequestChannelService
where
    State: Send + Sync + 'static,
{
    type Response = Response;
    type Error = ServiceError;

    #[instrument(skip_all)]
    async fn serve(
        &self,
        ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        let _ = ctx;
        let (resp_tx, resp_rx) = oneshot::channel();

        let start = SystemTime::now();

        let operation = req.to_string();
        let attributes = [KeyValue::new("operation", operation.clone())];

        let capacity = self.tx.capacity();
        STORAGE_CHANNEL_CAPACITY.record(capacity as u64, &attributes);
        debug!(operation, capacity);

        self.tx
            .reserve()
            .await
            .map(|permit| permit.send((req, resp_tx)))
            .inspect(|_| {
                let permit_elapsed = self.elapsed_millis(start);
                STORAGE_CHANNEL_PERMIT_DURATION.record(permit_elapsed, &attributes);
                debug!(operation, permit_elapsed);
            })
            .inspect_err(|err| {
                error!(operation, ?err);
                STORAGE_CHANNEL_ERROR.add(1, &attributes);
            })?;

        resp_rx
            .await
            .map_err(|_| Error::OneshotRecv.into())
            .inspect(|_| {
                let elapsed_millis = self.elapsed_millis(start);
                STORAGE_CHANNEL_REQUEST_DURATION.record(elapsed_millis, &attributes);
                debug!(operation, elapsed_millis);
            })
            .inspect_err(|err| {
                error!(operation, ?err);
                STORAGE_CHANNEL_ERROR.add(1, &attributes);
            })
    }
}

static STORAGE_CHANNEL_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_storage_channel_request_duration")
        .with_unit("ms")
        .with_description("Storage channel request latency in milliseconds")
        .build()
});

static STORAGE_CHANNEL_PERMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("jansu_storage_channel_permit_duration")
        .with_unit("ms")
        .with_description("Storage channel permit latency in milliseconds")
        .build()
});

static STORAGE_CHANNEL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_storage_channel_error")
        .with_description("Storage channel error count")
        .build()
});

#[async_trait]
impl Storage for RequestChannelService {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.serve(
            Context::default(),
            Request::RegisterBroker(broker_registration),
        )
        .await
        .and_then(|response| {
            if let Response::RegisterBroker(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.serve(
            Context::default(),
            Request::IncrementalAlterResource(resource),
        )
        .await
        .and_then(|response| {
            if let Response::IncrementalAlterResponse(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.serve(
            Context::default(),
            Request::CreateTopic {
                topic,
                validate_only,
            },
        )
        .await
        .and_then(|response| {
            if let Response::CreateTopic(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.serve(
            Context::default(),
            Request::DeleteRecords(Vec::from(topics)),
        )
        .await
        .and_then(|response| {
            if let Response::DeleteRecords(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.serve(Context::default(), Request::DeleteTopic(topic.to_owned()))
            .await
            .and_then(|response| {
                if let Response::DeleteTopic(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.serve(Context::default(), Request::Brokers)
            .await
            .and_then(|response| {
                if let Response::Brokers(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let transaction_id = transaction_id.map(|s| s.to_string());
        let topition = topition.to_owned();

        self.serve(
            Context::default(),
            Request::Produce {
                transaction_id,
                topition,
                batch,
            },
        )
        .await
        .and_then(|response| {
            if let Response::Produce(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let topition = topition.to_owned();

        self.serve(
            Context::default(),
            Request::Fetch {
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation,
            },
        )
        .await
        .and_then(|response| {
            if let Response::Fetch(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.serve(
            Context::default(),
            Request::OffsetStage(topition.to_owned()),
        )
        .await
        .and_then(|response| {
            if let Response::OffsetStage(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let offsets = Vec::from(offsets);

        self.serve(
            Context::default(),
            Request::ListOffsets {
                isolation_level,
                offsets,
            },
        )
        .await
        .and_then(|response| {
            if let Response::ListOffsets(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let group_id = group_id.to_string();
        let offsets = Vec::from(offsets);

        self.serve(
            Context::default(),
            Request::OffsetCommit {
                group_id,
                retention_time_ms,
                offsets,
            },
        )
        .await
        .and_then(|response| {
            if let Response::OffsetCommit(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let group_id = group_id.to_string();

        self.serve(
            Context::default(),
            Request::CommittedOffsetTopitions(group_id),
        )
        .await
        .and_then(|response| {
            if let Response::CommittedOffsetTopitions(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.offset_fetch_records(group_id, topics, require_stable)
            .await
            .map(|offsets| {
                offsets
                    .into_iter()
                    .map(|(topition, record)| (topition, record.committed_offset()))
                    .collect()
            })
    }

    #[instrument(skip_all)]
    async fn offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        let group_id = group_id.map(|s| s.to_string());
        let topics = Vec::from(topics);

        self.serve(
            Context::default(),
            Request::OffsetFetch {
                group_id,
                topics,
                require_stable,
            },
        )
        .await
        .and_then(|response| {
            if let Response::OffsetFetch(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        self.serve(
            Context::default(),
            Request::OffsetForLeaderEpoch {
                topition: topition.to_owned(),
                leader_epoch,
            },
        )
        .await
        .and_then(|response| {
            if let Response::OffsetForLeaderEpoch(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let topics = topics.map(Vec::from);

        self.serve(Context::default(), Request::Metadata(topics))
            .await
            .and_then(|response| {
                if let Response::Metadata(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let name = name.to_string();
        let keys = keys.map(Vec::from);

        self.serve(
            Context::default(),
            Request::DescribeConfig {
                name,
                resource,
                keys,
            },
        )
        .await
        .and_then(|response| {
            if let Response::DescribeConfig(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let topics = topics.map(Vec::from);

        self.serve(
            Context::default(),
            Request::DescribeTopicPartitions {
                topics,
                partition_limit,
                cursor,
            },
        )
        .await
        .and_then(|response| {
            if let Response::DescribeTopicPartitions(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let states_filter = states_filter.map(Vec::from);

        self.serve(Context::default(), Request::ListGroups(states_filter))
            .await
            .and_then(|response| {
                if let Response::ListGroups(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let group_ids = group_ids.map(Vec::from);

        self.serve(Context::default(), Request::DeleteGroups(group_ids))
            .await
            .and_then(|response| {
                if let Response::DeleteGroups(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let group_ids = group_ids.map(Vec::from);

        self.serve(
            Context::default(),
            Request::DescribeGroups {
                group_ids,
                include_authorized_operations,
            },
        )
        .await
        .and_then(|response| {
            if let Response::DescribeGroups(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let group_id = group_id.to_string();

        self.serve(
            Context::default(),
            Request::UpdateGroup {
                group_id,
                detail,
                version,
            },
        )
        .await
        .and_then(|response| {
            if let Response::UpdateGroup(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let transaction_id = transaction_id.map(|transaction_id| transaction_id.to_owned());

        self.serve(
            Context::default(),
            Request::InitProducer {
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            },
        )
        .await
        .and_then(|response| {
            if let Response::InitProducer(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let transaction_id = transaction_id.to_string();
        let group_id = group_id.to_string();

        self.serve(
            Context::default(),
            Request::TxnAddOffsets {
                transaction_id,
                producer_id,
                producer_epoch,
                group_id,
            },
        )
        .await
        .and_then(|response| {
            if let Response::TxnAddOffsets(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.serve(Context::default(), Request::TxnAddPartitions(partitions))
            .await
            .and_then(|response| {
                if let Response::TxnAddPartitions(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.serve(Context::default(), Request::TxnOffsetCommit(offsets))
            .await
            .and_then(|response| {
                if let Response::TxnOffsetCommit(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let transaction_id = transaction_id.to_string();

        self.serve(
            Context::default(),
            Request::TxnEnd {
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
            },
        )
        .await
        .and_then(|response| {
            if let Response::TxnEnd(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.serve(Context::default(), Request::Maintain(now))
            .await
            .and_then(|response| {
                if let Response::Maintain(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        self.serve(Context::default(), Request::ClusterId)
            .await
            .and_then(|response| {
                if let Response::ClusterId(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        self.serve(Context::default(), Request::Node)
            .await
            .and_then(|response| {
                if let Response::Node(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        self.serve(Context::default(), Request::AdvertisedListener)
            .await
            .and_then(|response| {
                if let Response::AdvertisedListener(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let user = user.to_string();

        self.serve(
            Context::default(),
            Request::DeleteUserScramCredential { user, mechanism },
        )
        .await
        .and_then(|response| {
            if let Response::DeleteUserScramCredential(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let user = user.to_string();

        self.serve(
            Context::default(),
            Request::UpsertUserScramCredential {
                user,
                mechanism,
                credential,
            },
        )
        .await
        .and_then(|response| {
            if let Response::UpsertUserScramCredential(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let user = user.to_string();

        self.serve(
            Context::default(),
            Request::UserScramCredential { user, mechanism },
        )
        .await
        .and_then(|response| {
            if let Response::UserScramCredential(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        self.serve(Context::default(), Request::Ping)
            .await
            .and_then(|response| {
                if let Response::Ping(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }
}

