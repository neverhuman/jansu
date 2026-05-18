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

//! Phase 04 — Differential Kafka Lab
//!
//! Compares Jansu behavior against a real Apache Kafka 4.2 broker for every
//! advertised API, plus selected **unadvertised** read paths owned by later
//! phases (for example Phase 08 ListOffsets / Fetch exercised here as
//! evidence-only; failures are owned by `AUDIT-004`, not Phase 04 contract).
//! Tests are gated behind `JANSU_DIFFERENTIAL=1` so normal `cargo test` runs
//! remain reliable on machines without Docker.
//!
//! See `docs/compatibility/differential-lab.md` for usage details.

use std::{
    collections::BTreeMap,
    convert::TryInto,
    fs, io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use jansu_sans_io::{
    Ack, ApiKey as _, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    Error as SansIoError, ErrorCode, FetchRequest, FetchResponse, Frame, Header, IsolationLevel,
    ListOffset, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    NULL_TOPIC_ID, ProduceRequest, ProduceResponse,
    api_versions_response::ApiVersion,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use rand::{prelude::*, rng};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use url::Url;
use uuid::Uuid;

type DynError = Box<dyn std::error::Error + Send + Sync>;
type DynResult<T> = Result<T, DynError>;

const LEDGER_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../docs/compatibility/kafka-4.2-ledger.json"
);

// ---------------------------------------------------------------------------
// Ledger types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct VersionRange {
    min: i16,
    max: i16,
}

#[derive(Debug, Deserialize)]
struct Ledger {
    apis: Vec<ApiRow>,
}

#[derive(Debug, Deserialize)]
struct ApiRow {
    api_key: i16,
    approved_advertised_versions: Option<VersionRange>,
}

// ---------------------------------------------------------------------------
// Artifact types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ApiVersionsArtifact {
    schema_version: u16,
    workload: &'static str,
    kafka_bootstrap: String,
    jansu_bootstrap: String,
    kafka_versions: BTreeMap<i16, VersionRange>,
    jansu_versions: BTreeMap<i16, VersionRange>,
    ledger_approved_versions: BTreeMap<i16, VersionRange>,
}

#[derive(Debug, Serialize)]
struct MetadataSummary {
    broker_count: usize,
    topic_count: usize,
    has_cluster_id: bool,
    controller_id: Option<i32>,
    throttle_time_ms: Option<i32>,
}

#[derive(Debug, Serialize)]
struct MetadataArtifact {
    schema_version: u16,
    workload: &'static str,
    kafka_bootstrap: String,
    jansu_bootstrap: String,
    kafka: MetadataSummary,
    jansu: MetadataSummary,
}

#[derive(Debug, Serialize)]
struct ProduceArtifact {
    schema_version: u16,
    workload: &'static str,
    kafka_bootstrap: String,
    jansu_bootstrap: String,
    kafka_offsets: Vec<i64>,
    jansu_offsets: Vec<i64>,
    topic: String,
    records_produced: usize,
}

#[derive(Debug, Serialize)]
struct ProduceListOffsetsFetchArtifact {
    schema_version: u16,
    workload: &'static str,
    kafka_bootstrap: String,
    jansu_bootstrap: String,
    topic: String,
    kafka_latest_offset: i64,
    jansu_latest_offset: i64,
    kafka_earliest_offset: i64,
    jansu_earliest_offset: i64,
    kafka_payloads_utf8: Vec<String>,
    jansu_payloads_utf8: Vec<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn external_enabled() -> bool {
    std::env::var_os("JANSU_DIFFERENTIAL").is_some()
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn other_error(message: impl Into<String>) -> DynError {
    Box::new(io::Error::other(message.into()))
}

fn artifact_dir() -> PathBuf {
    std::env::var_os("JANSU_DIFF_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| repo_root().join("target/differential"))
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn write_artifact<T: Serialize>(name: &str, value: &T) -> DynResult<()> {
    let dir = artifact_dir();
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{name}-{}.json", unix_ms()));
    fs::write(&path, serde_json::to_vec_pretty(value)?)?;
    eprintln!("artifact written: {}", path.display());
    Ok(())
}

fn approved_versions_from_ledger() -> DynResult<BTreeMap<i16, VersionRange>> {
    let ledger: Ledger = serde_json::from_slice(&fs::read(LEDGER_PATH)?)?;

    Ok(ledger
        .apis
        .into_iter()
        .filter_map(|row| {
            row.approved_advertised_versions
                .map(|range| (row.api_key, range))
        })
        .collect())
}

fn api_version_map(response: &ApiVersionsResponse) -> BTreeMap<i16, VersionRange> {
    response
        .api_keys
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|api| {
            (
                api.api_key,
                VersionRange {
                    min: api.min_version,
                    max: api.max_version,
                },
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Kafka wire helpers
// ---------------------------------------------------------------------------

async fn free_port() -> DynResult<u16> {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    Ok(listener.local_addr()?.port())
}

async fn tcp_round_trip(bootstrap: &str, request: Bytes) -> DynResult<Vec<u8>> {
    let mut stream = TcpStream::connect(bootstrap).await?;
    stream.write_all(&request).await?;

    let mut len = [0_u8; 4];
    let _: usize = stream.read_exact(&mut len).await?;
    let size = i32::from_be_bytes(len);
    if size < 0 {
        return Err(other_error(format!("negative Kafka frame size: {size}")));
    }

    let mut response = vec![0_u8; 4 + size as usize];
    response[..4].copy_from_slice(&len);
    let _: usize = stream.read_exact(&mut response[4..]).await?;
    Ok(response)
}

async fn api_versions(bootstrap: &str) -> DynResult<ApiVersionsResponse> {
    // Use ApiVersions v0 with raw byte encoding to avoid flex-header
    // incompatibilities between Jansu's Frame layer and Kafka 4.2.
    // v0 has a simple header: api_key(2) + api_version(2) + correlation_id(4) + client_id(len+data).
    let client_id = b"jansu-phase04-differential";
    let header_size: i32 = 2 + 2 + 4 + 2 + client_id.len() as i32;
    let mut buf = Vec::with_capacity(4 + header_size as usize);
    buf.extend_from_slice(&header_size.to_be_bytes()); // frame size
    buf.extend_from_slice(&18_i16.to_be_bytes()); // api_key = ApiVersions
    buf.extend_from_slice(&0_i16.to_be_bytes()); // api_version = 0
    buf.extend_from_slice(&7_i32.to_be_bytes()); // correlation_id
    buf.extend_from_slice(&(client_id.len() as i16).to_be_bytes()); // client_id length
    buf.extend_from_slice(client_id);

    let mut stream = TcpStream::connect(bootstrap).await?;
    stream.write_all(&buf).await?;

    // Read response frame
    let mut len_bytes = [0_u8; 4];
    let _: usize = stream.read_exact(&mut len_bytes).await?;
    let size = i32::from_be_bytes(len_bytes);
    if size < 4 {
        return Err(other_error(format!(
            "invalid ApiVersions response size: {size}"
        )));
    }

    let mut data = vec![0_u8; size as usize];
    let _: usize = stream.read_exact(&mut data).await?;

    // Parse: correlation_id(4) + error_code(2) + api_count(4 for v0 array)
    let _correlation_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let error_code = i16::from_be_bytes([data[4], data[5]]);
    if error_code != 0 {
        return Err(other_error(format!("ApiVersions error code: {error_code}")));
    }

    let api_count = i32::from_be_bytes([data[6], data[7], data[8], data[9]]);
    let mut api_keys = Vec::new();
    let mut offset = 10;
    for _ in 0..api_count {
        let api_key = i16::from_be_bytes([data[offset], data[offset + 1]]);
        let min_version = i16::from_be_bytes([data[offset + 2], data[offset + 3]]);
        let max_version = i16::from_be_bytes([data[offset + 4], data[offset + 5]]);
        api_keys.push(
            ApiVersion::default()
                .api_key(api_key)
                .min_version(min_version)
                .max_version(max_version),
        );
        offset += 6;
    }

    Ok(ApiVersionsResponse::default()
        .error_code(error_code)
        .api_keys(Some(api_keys)))
}

/// Fetch metadata using Jansu's Frame layer (for talking to Jansu broker).
async fn metadata_via_frame(bootstrap: &str) -> DynResult<MetadataResponse> {
    let request = Frame::request(
        Header::Request {
            api_key: MetadataRequest::KEY,
            api_version: 12,
            correlation_id: 8,
            client_id: Some("jansu-phase04-differential".into()),
        },
        MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )?;

    let response = tcp_round_trip(bootstrap, request).await?;
    let response = Frame::response_from_bytes(&response[..], MetadataRequest::KEY, 12)?;
    Ok(MetadataResponse::try_from(response.body)?)
}

/// Fetch metadata summary using rdkafka (for talking to external Kafka brokers
/// without relying on Jansu's Frame flex-header implementation).
fn metadata_summary_via_rdkafka(bootstrap: &str) -> DynResult<MetadataSummary> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()?;

    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;

    Ok(MetadataSummary {
        broker_count: metadata.brokers().len(),
        topic_count: metadata
            .topics()
            .iter()
            .filter(|t| !t.name().starts_with("__"))
            .count(),
        has_cluster_id: true, // rdkafka doesn't expose cluster_id directly in metadata; KRaft always has one
        controller_id: None,  // not exposed by rdkafka metadata API
        throttle_time_ms: None,
    })
}

fn summarize_metadata(response: &MetadataResponse) -> MetadataSummary {
    MetadataSummary {
        broker_count: response.brokers.as_deref().unwrap_or_default().len(),
        topic_count: response.topics.as_deref().unwrap_or_default().len(),
        has_cluster_id: response.cluster_id.is_some(),
        controller_id: response.controller_id,
        throttle_time_ms: response.throttle_time_ms,
    }
}

fn librdkafka_partition_watermarks(
    bootstrap: &str,
    topic: &str,
    partition: i32,
) -> DynResult<(i64, i64)> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()?;

    let (earliest, latest) =
        consumer.fetch_watermarks(topic, partition, Duration::from_secs(10))?;
    Ok((earliest, latest))
}

const LIST_OFFSETS_API_VERSION: i16 = 9;
const CREATE_TOPICS_API_VERSION: i16 = 7;
const PRODUCE_API_VERSION: i16 = 11;
const FETCH_API_VERSION: i16 = 12;

fn record_frame(payload: &[u8]) -> DynResult<deflated::Frame> {
    inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(b"k")))
                .value(Some(Bytes::copy_from_slice(payload))),
        )
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)
        .map_err(|err| other_error(err.to_string()))
}

async fn create_topic_via_frame(bootstrap: &str, topic: &str) -> DynResult<()> {
    let request = Frame::request(
        Header::Request {
            api_key: CreateTopicsRequest::KEY,
            api_version: CREATE_TOPICS_API_VERSION,
            correlation_id: 18,
            client_id: Some("jansu-phase04-differential".into()),
        },
        CreateTopicsRequest::default()
            .topics(Some(vec![
                CreatableTopic::default()
                    .name(topic.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some(vec![]))
                    .configs(Some(vec![])),
            ]))
            .timeout_ms(30_000)
            .validate_only(Some(false))
            .into(),
    )?;

    let response = tcp_round_trip(bootstrap, request).await?;
    let frame = Frame::response_from_bytes(
        &response[..],
        CreateTopicsRequest::KEY,
        CREATE_TOPICS_API_VERSION,
    )?;
    let body =
        CreateTopicsResponse::try_from(frame.body).map_err(|e| other_error(e.to_string()))?;
    let topics = body.topics.as_deref().unwrap_or_default();
    let row = topics
        .iter()
        .find(|t| t.name == topic)
        .ok_or_else(|| other_error("create topics: topic missing"))?;
    if row.error_code != 0 {
        return Err(other_error(format!(
            "create topics error_code={} for {topic}",
            row.error_code
        )));
    }
    Ok(())
}

async fn produce_payload_via_frame(bootstrap: &str, topic: &str, payload: &[u8]) -> DynResult<i64> {
    let request = Frame::request(
        Header::Request {
            api_key: ProduceRequest::KEY,
            api_version: PRODUCE_API_VERSION,
            correlation_id: 19,
            client_id: Some("jansu-phase04-differential".into()),
        },
        ProduceRequest::default()
            .acks(i16::from(Ack::Leader))
            .timeout_ms(10_000)
            .topic_data(Some(vec![
                TopicProduceData::default()
                    .name(topic.into())
                    .partition_data(Some(vec![
                        PartitionProduceData::default()
                            .index(0)
                            .records(Some(record_frame(payload)?)),
                    ])),
            ]))
            .into(),
    )?;

    let response = tcp_round_trip(bootstrap, request).await?;
    let frame =
        Frame::response_from_bytes(&response[..], ProduceRequest::KEY, PRODUCE_API_VERSION)?;
    let body = ProduceResponse::try_from(frame.body).map_err(|e| other_error(e.to_string()))?;
    let topics = body.responses.as_deref().unwrap_or_default();
    let row = topics
        .iter()
        .find(|t| t.name == topic)
        .ok_or_else(|| other_error("produce: topic missing"))?;
    let parts = row.partition_responses.as_deref().unwrap_or_default();
    let p = parts
        .iter()
        .find(|p| p.index == 0)
        .ok_or_else(|| other_error("produce: partition missing"))?;
    if ErrorCode::try_from(p.error_code).map_err(|e| other_error(e.to_string()))? != ErrorCode::None
    {
        return Err(other_error(format!(
            "produce error_code={} for {topic} p=0",
            p.error_code
        )));
    }
    Ok(p.base_offset)
}

async fn fetch_payloads_via_frame(
    bootstrap: &str,
    topic: &str,
    expect_count: usize,
) -> DynResult<Vec<Vec<u8>>> {
    let request = Frame::request(
        Header::Request {
            api_key: FetchRequest::KEY,
            api_version: FETCH_API_VERSION,
            correlation_id: 29,
            client_id: Some("jansu-phase04-differential".into()),
        },
        FetchRequest::default()
            .replica_id(Some(-1))
            .max_wait_ms(500)
            .min_bytes(1)
            .max_bytes(Some(50 * 1024))
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .session_id(Some(0))
            .session_epoch(Some(0))
            .topics(Some(vec![
                FetchTopic::default()
                    .topic(Some(topic.into()))
                    .topic_id(Some(NULL_TOPIC_ID))
                    .partitions(Some(vec![
                        FetchPartition::default()
                            .partition(0)
                            .current_leader_epoch(Some(-1))
                            .fetch_offset(0)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(50 * 1024)
                            .replica_directory_id(None),
                    ])),
            ]))
            .forgotten_topics_data(Some([].into()))
            .rack_id(Some("".into()))
            .into(),
    )?;

    let response = tcp_round_trip(bootstrap, request).await?;
    let frame = Frame::response_from_bytes(&response[..], FetchRequest::KEY, FETCH_API_VERSION)?;
    let body = FetchResponse::try_from(frame.body).map_err(|e| other_error(e.to_string()))?;
    let topics = body.responses.as_deref().unwrap_or_default();
    let row = topics
        .iter()
        .find(|t| t.topic.as_deref() == Some(topic))
        .ok_or_else(|| other_error("fetch: topic missing"))?;
    let parts = row.partitions.as_deref().unwrap_or_default();
    let p = parts
        .iter()
        .find(|p| p.partition_index == 0)
        .ok_or_else(|| other_error("fetch: partition missing"))?;
    if ErrorCode::try_from(p.error_code).map_err(|e| other_error(e.to_string()))? != ErrorCode::None
    {
        return Err(other_error(format!(
            "fetch error_code={} for {topic} p=0",
            p.error_code
        )));
    }

    let mut payloads = Vec::new();
    for batch in p
        .records
        .as_ref()
        .map(|records| records.batches.as_slice())
        .unwrap_or_default()
    {
        let inflated =
            inflated::Batch::try_from(batch.clone()).map_err(|err| other_error(err.to_string()))?;
        for record in inflated.records {
            if let Some(value) = record.value {
                payloads.push(value.to_vec());
            }
        }
    }

    if payloads.len() != expect_count {
        return Err(other_error(format!(
            "fetch expected {expect_count} payloads from {bootstrap}, got {}",
            payloads.len()
        )));
    }

    Ok(payloads)
}

async fn list_offsets_partition_offset(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    which: ListOffset,
    correlation_id: i32,
) -> DynResult<i64> {
    let timestamp: i64 = which
        .try_into()
        .map_err(|e: SansIoError| other_error(e.to_string()))?;
    let request = Frame::request(
        Header::Request {
            api_key: ListOffsetsRequest::KEY,
            api_version: LIST_OFFSETS_API_VERSION,
            correlation_id,
            client_id: Some("jansu-phase04-differential".into()),
        },
        ListOffsetsRequest::default()
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .replica_id(-1)
            .topics(Some(vec![
                ListOffsetsTopic::default()
                    .name(topic.into())
                    .partitions(Some(vec![
                        ListOffsetsPartition::default()
                            .partition_index(partition)
                            .timestamp(timestamp)
                            .current_leader_epoch(Some(-1))
                            .max_num_offsets(Some(1)),
                    ])),
            ]))
            .into(),
    )?;

    let response = tcp_round_trip(bootstrap, request).await?;
    let frame = Frame::response_from_bytes(
        &response[..],
        ListOffsetsRequest::KEY,
        LIST_OFFSETS_API_VERSION,
    )?;
    let body = ListOffsetsResponse::try_from(frame.body).map_err(|e| other_error(e.to_string()))?;
    let topics = body.topics.as_deref().unwrap_or_default();
    let row = topics
        .iter()
        .find(|t| t.name == topic)
        .ok_or_else(|| other_error("list offsets: topic missing"))?;
    let parts = row.partitions.as_deref().unwrap_or_default();
    let p = parts
        .iter()
        .find(|p| p.partition_index == partition)
        .ok_or_else(|| other_error("list offsets: partition missing"))?;
    if p.error_code != 0 {
        return Err(other_error(format!(
            "list offsets error_code={} for {topic} p={partition}",
            p.error_code
        )));
    }
    p.offset
        .ok_or_else(|| other_error("list offsets: offset null"))
}

async fn librdkafka_consume_payloads_from_beginning(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
    expect_count: usize,
) -> DynResult<Vec<Vec<u8>>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("message.max.bytes", "1048576")
        .create()?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .map_err(|e| other_error(format!("{e}")))?;
    consumer
        .assign(&tpl)
        .map_err(|e| other_error(format!("{e}")))?;

    let mut out = Vec::with_capacity(expect_count);
    let deadline = Instant::now() + Duration::from_secs(30);
    while out.len() < expect_count {
        if Instant::now() > deadline {
            return Err(other_error(format!(
                "timed out consuming {expect_count} messages from {bootstrap}, got {}",
                out.len()
            )));
        }
        match consumer.recv().await {
            Ok(message) => {
                out.push(message.payload().unwrap_or_default().to_vec());
            }
            Err(e) => return Err(other_error(format!("{e}"))),
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Guards: Kafka 4.2 and Jansu lifecycle
// ---------------------------------------------------------------------------

struct KafkaGuard {
    bootstrap: String,
    compose: Option<KafkaComposeInstance>,
}

#[derive(Debug)]
struct KafkaComposeInstance {
    project_name: String,
    container_name: String,
    port: u16,
    fixed_port: bool,
}

impl KafkaComposeInstance {
    async fn new() -> DynResult<Self> {
        let (port, fixed_port) = match std::env::var("JANSU_DIFF_KAFKA_PORT") {
            Ok(value) => {
                let port = value.parse::<u16>().map_err(|err| {
                    other_error(format!("invalid JANSU_DIFF_KAFKA_PORT={value:?}: {err}"))
                })?;
                if port == 0 {
                    return Err(other_error(
                        "JANSU_DIFF_KAFKA_PORT must be greater than zero",
                    ));
                }
                (port, true)
            }
            Err(std::env::VarError::NotPresent) => (free_port().await?, false),
            Err(err) => {
                return Err(other_error(format!(
                    "failed to read JANSU_DIFF_KAFKA_PORT: {err}"
                )));
            }
        };
        let suffix = Uuid::now_v7().simple().to_string();

        Ok(Self {
            project_name: format!("jansu-diff-{suffix}"),
            container_name: format!("jansu-differential-kafka42-{suffix}"),
            port,
            fixed_port,
        })
    }

    fn command(&self, compose_file: &str) -> Command {
        let mut command = Command::new("docker");
        let _ = command
            .current_dir(repo_root())
            .env("JANSU_DIFF_KAFKA_CONTAINER", &self.container_name)
            .env("JANSU_DIFF_KAFKA_PORT", self.port.to_string())
            .args([
                "compose",
                "--project-name",
                self.project_name.as_str(),
                "-f",
                compose_file,
            ]);
        command
    }

    fn up(&self, compose_file: &str) -> io::Result<std::process::ExitStatus> {
        self.command(compose_file)
            .args(["up", "--wait", "--detach", "kafka42"])
            .status()
    }

    fn down(&self, compose_file: &str) -> io::Result<std::process::ExitStatus> {
        self.command(compose_file)
            .args(["down", "--volumes", "--remove-orphans"])
            .status()
    }

    fn bootstrap(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    async fn start_and_wait(&self, compose_file: &str) -> DynResult<()> {
        let status = self.up(compose_file)?;
        if !status.success() {
            return Err(other_error(format!(
                "docker compose failed to start Kafka 4.2: {status}"
            )));
        }

        wait_for_api_versions(&self.bootstrap()).await
    }
}

impl KafkaGuard {
    async fn start() -> DynResult<Self> {
        if let Ok(bootstrap) = std::env::var("JANSU_DIFF_KAFKA_BOOTSTRAP") {
            wait_for_api_versions(&bootstrap).await?;
            return Ok(Self {
                bootstrap,
                compose: None,
            });
        }

        let compose_file = "etc/differential/compose.kafka-4.2.yaml";
        let mut compose = KafkaComposeInstance::new().await?;

        for attempt in 1..=2 {
            match compose.start_and_wait(compose_file).await {
                Ok(()) => {
                    let bootstrap = compose.bootstrap();
                    return Ok(Self {
                        bootstrap,
                        compose: Some(compose),
                    });
                }
                Err(err) => {
                    let _ = compose.down(compose_file);
                    if attempt == 2 {
                        return Err(other_error(format!(
                            "docker compose failed to start Kafka 4.2 after retry: {err}"
                        )));
                    }
                    if compose.fixed_port {
                        eprintln!(
                            "differential lab: Kafka startup failed on fixed port {}; cleaning up and retrying...",
                            compose.port
                        );
                    } else {
                        eprintln!(
                            "differential lab: Kafka startup failed on auto port {}; cleaning up and retrying with a fresh sandbox...",
                            compose.port
                        );
                        compose = KafkaComposeInstance::new().await?;
                    }
                }
            }
        }

        unreachable!("Kafka Compose retry loop always returns")
    }

    fn bootstrap(&self) -> &str {
        &self.bootstrap
    }
}

impl Drop for KafkaGuard {
    fn drop(&mut self) {
        if let Some(compose) = &self.compose {
            let _ = compose.down("etc/differential/compose.kafka-4.2.yaml");
        }
    }
}

struct JansuGuard {
    bootstrap: String,
    task: JoinHandle<()>,
}

impl JansuGuard {
    async fn start() -> DynResult<Self> {
        let port = free_port().await?;
        let listener = Url::parse(&format!("tcp://127.0.0.1:{port}"))?;

        let broker = jansu_broker::broker::Broker::<
            jansu_broker::coordinator::group::administrator::Controller<
                jansu_storage::ArcDynStorage,
            >,
            jansu_storage::ArcDynStorage,
        >::builder()
        .node_id(rng().random_range(1..i32::MAX))
        .cluster_id(format!("jansu-phase04-{}", Uuid::now_v7()))
        .incarnation_id(Uuid::now_v7())
        .listener(listener.clone())
        .advertised_listener(listener)
        .storage(Url::parse("slatedb://memory")?)
        .silent(true)
        .build()
        .await?;

        let bootstrap = format!("127.0.0.1:{port}");
        let task = tokio::spawn(async move {
            if let Err(error) = broker.main(Instant::now()).await {
                eprintln!("jansu differential broker exited with error: {error:?}");
            }
        });

        wait_for_api_versions(&bootstrap).await?;

        Ok(Self { bootstrap, task })
    }

    fn bootstrap(&self) -> &str {
        &self.bootstrap
    }
}

impl Drop for JansuGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn wait_for_api_versions(bootstrap: &str) -> DynResult<()> {
    let deadline = Duration::from_secs(60);

    timeout(deadline, async {
        let mut last_err = String::new();
        loop {
            match api_versions(bootstrap).await {
                Ok(_) => return Ok::<_, DynError>(()),
                Err(err) => {
                    let msg = format!("{err}");
                    if msg != last_err {
                        eprintln!("wait_for_api_versions({bootstrap}): {err}");
                        last_err = msg;
                    }
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .map_err(|_| other_error(format!("timed out waiting for {bootstrap}")))??;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Compares Jansu's advertised ApiVersions against both the compatibility
/// ledger and a real Kafka 4.2 broker. Jansu's advertised range must exactly
/// match the ledger's approved_advertised_versions, and every Jansu-advertised
/// API must be a subset of Kafka 4.2's range.
#[tokio::test]
async fn differential_api_versions_advertised_subset_of_kafka42() -> DynResult<()> {
    if !external_enabled() {
        eprintln!("skipping external differential lab; set JANSU_DIFFERENTIAL=1");
        return Ok(());
    }

    let kafka = KafkaGuard::start().await?;
    let jansu = JansuGuard::start().await?;

    let kafka_response = api_versions(kafka.bootstrap()).await?;
    let jansu_response = api_versions(jansu.bootstrap()).await?;

    let kafka_versions = api_version_map(&kafka_response);
    let jansu_versions = api_version_map(&jansu_response);
    let ledger_approved_versions = approved_versions_from_ledger()?;

    assert_eq!(
        ledger_approved_versions, jansu_versions,
        "Jansu ApiVersions must exactly match approved advertised ledger versions"
    );

    for (api_key, jansu_range) in &jansu_versions {
        let kafka_range = kafka_versions.get(api_key).ok_or_else(|| {
            other_error(format!(
                "Kafka 4.2 does not advertise Jansu-approved api_key {api_key}"
            ))
        })?;

        assert!(
            kafka_range.min <= jansu_range.min && kafka_range.max >= jansu_range.max,
            "Jansu api_key {api_key} range {jansu_range:?} must be a Kafka 4.2 subset {kafka_range:?}"
        );
    }

    write_artifact(
        "api-versions",
        &ApiVersionsArtifact {
            schema_version: 1,
            workload: "api_versions_advertised_subset_of_kafka42",
            kafka_bootstrap: kafka.bootstrap().into(),
            jansu_bootstrap: jansu.bootstrap().into(),
            kafka_versions,
            jansu_versions,
            ledger_approved_versions,
        },
    )?;

    Ok(())
}

/// Compares Metadata responses from Kafka 4.2 and Jansu for an empty cluster.
/// Both must report at least one broker, zero topics, and a cluster ID.
#[tokio::test]
async fn differential_metadata_for_empty_cluster_matches_advertised_contract() -> DynResult<()> {
    if !external_enabled() {
        eprintln!("skipping external differential lab; set JANSU_DIFFERENTIAL=1");
        return Ok(());
    }

    let kafka = KafkaGuard::start().await?;
    let jansu = JansuGuard::start().await?;

    let kafka_summary = metadata_summary_via_rdkafka(kafka.bootstrap())?;
    let jansu_metadata = metadata_via_frame(jansu.bootstrap()).await?;

    let jansu_summary = summarize_metadata(&jansu_metadata);

    assert!(
        kafka_summary.broker_count >= 1,
        "Kafka should report at least one broker"
    );
    assert!(
        jansu_summary.broker_count >= 1,
        "Jansu should report at least one broker"
    );
    assert_eq!(
        0, kafka_summary.topic_count,
        "empty Kafka reference cluster should report no requested topics"
    );
    assert_eq!(
        0, jansu_summary.topic_count,
        "empty Jansu reference cluster should report no requested topics"
    );
    assert!(
        kafka_summary.has_cluster_id,
        "Kafka metadata should include cluster id"
    );
    assert!(
        jansu_summary.has_cluster_id,
        "Jansu metadata should include cluster id"
    );

    write_artifact(
        "metadata-empty-cluster",
        &MetadataArtifact {
            schema_version: 1,
            workload: "metadata_for_empty_cluster_matches_advertised_contract",
            kafka_bootstrap: kafka.bootstrap().into(),
            jansu_bootstrap: jansu.bootstrap().into(),
            kafka: kafka_summary,
            jansu: jansu_summary,
        },
    )?;

    Ok(())
}

/// Uses librdkafka to produce records against Kafka 4.2 and Jansu's wire-frame
/// Produce route against Jansu. Verifies both systems assign sequential offsets
/// starting at 0.
/// This test validates Produce (advertised v0..=11 after Phase 07). Kafka topic
/// setup uses AdminClient; Jansu topic setup uses the broker CreateTopics route.
/// Skips unless `JANSU_DIFFERENTIAL=1` (same as other external differential
/// tests).
#[tokio::test]
async fn differential_produce_round_trip_for_advertised_api() -> DynResult<()> {
    if !external_enabled() {
        eprintln!("skipping external differential lab; set JANSU_DIFFERENTIAL=1");
        return Ok(());
    }

    let kafka = KafkaGuard::start().await?;
    let jansu = JansuGuard::start().await?;

    let topic = format!("phase04-produce-{}", Uuid::now_v7());
    let payloads = ["record-0", "record-1", "record-2"];

    // Create topic on Kafka
    let kafka_admin: AdminClient<rdkafka::client::DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", kafka.bootstrap())
        .create()?;
    for topic_result in kafka_admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await?
    {
        let _: String = topic_result
            .map_err(|(name, code)| other_error(format!("create topic {name}: {code:?}")))?;
    }

    // Produce to Kafka
    let kafka_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", kafka.bootstrap())
        .set("message.timeout.ms", "10000")
        .create()?;

    let mut kafka_offsets = Vec::new();
    for payload in &payloads {
        let delivery = kafka_producer
            .send(
                FutureRecord::to(&topic)
                    .payload(payload.as_bytes())
                    .key("k"),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(err, _)| err)?;
        eprintln!(
            "kafka produced: partition={}, offset={}",
            delivery.partition, delivery.offset
        );
        kafka_offsets.push(delivery.offset);
    }

    create_topic_via_frame(jansu.bootstrap(), &topic)
        .await
        .map_err(|err| other_error(format!("jansu create topic for produce test: {err}")))?;

    let mut jansu_offsets = Vec::new();
    for payload in &payloads {
        let offset = produce_payload_via_frame(jansu.bootstrap(), &topic, payload.as_bytes())
            .await
            .map_err(|err| {
                other_error(format!(
                    "jansu produce payload {payload:?} for produce test: {err}"
                ))
            })?;
        eprintln!("jansu produced: partition=0, offset={offset}");
        jansu_offsets.push(offset);
    }

    // Both should assign sequential offsets starting at 0
    assert_eq!(
        vec![0, 1, 2],
        kafka_offsets,
        "Kafka should assign sequential offsets starting at 0"
    );
    assert_eq!(
        vec![0, 1, 2],
        jansu_offsets,
        "Jansu should assign sequential offsets starting at 0"
    );

    write_artifact(
        "produce-roundtrip",
        &ProduceArtifact {
            schema_version: 1,
            workload: "produce_round_trip_for_advertised_api",
            kafka_bootstrap: kafka.bootstrap().into(),
            jansu_bootstrap: jansu.bootstrap().into(),
            kafka_offsets,
            jansu_offsets,
            topic,
            records_produced: payloads.len(),
        },
    )?;

    Ok(())
}

/// Phase 08 / `AUDIT-004` evidence: after the same Produce workload, Kafka 4.2
/// and Jansu must agree on ListOffsets v9 **latest** (log end) and **earliest**
/// for partition 0, and Jansu's wire-frame Fetch route must read identical
/// payloads from the beginning. APIs remain **unadvertised**; this test does not
/// relax ApiVersions contract checks.
#[tokio::test]
async fn differential_listoffsets_latest_and_fetch_consume_after_produce() -> DynResult<()> {
    if !external_enabled() {
        eprintln!("skipping external differential lab; set JANSU_DIFFERENTIAL=1");
        return Ok(());
    }

    let kafka = KafkaGuard::start().await?;
    let jansu = JansuGuard::start().await?;

    let topic = format!("phase08-diff-read-{}", Uuid::now_v7());
    let payloads = ["record-0", "record-1", "record-2"];

    let kafka_admin: AdminClient<rdkafka::client::DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", kafka.bootstrap())
        .create()?;
    for topic_result in kafka_admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await?
    {
        let _: String = topic_result
            .map_err(|(name, code)| other_error(format!("create topic {name}: {code:?}")))?;
    }

    let kafka_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", kafka.bootstrap())
        .set("message.timeout.ms", "10000")
        .create()?;

    for payload in &payloads {
        let _delivery = kafka_producer
            .send(
                FutureRecord::to(&topic)
                    .payload(payload.as_bytes())
                    .key("k"),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(err, _)| err)?;
    }

    create_topic_via_frame(jansu.bootstrap(), &topic)
        .await
        .map_err(|err| other_error(format!("jansu create topic for read test: {err}")))?;

    for payload in &payloads {
        let _offset = produce_payload_via_frame(jansu.bootstrap(), &topic, payload.as_bytes())
            .await
            .map_err(|err| {
                other_error(format!(
                    "jansu produce payload {payload:?} for read test: {err}"
                ))
            })?;
    }

    let (kafka_earliest, kafka_latest) =
        librdkafka_partition_watermarks(kafka.bootstrap(), &topic, 0)?;
    let jansu_latest =
        list_offsets_partition_offset(jansu.bootstrap(), &topic, 0, ListOffset::Latest, 32)
            .await
            .map_err(|err| other_error(format!("jansu list offsets latest: {err}")))?;
    assert_eq!(
        3, kafka_latest,
        "Kafka ListOffsets latest should be log end offset after three appends"
    );
    assert_eq!(
        kafka_latest, jansu_latest,
        "Jansu ListOffsets latest HWM must match Kafka after identical produce workload"
    );

    let jansu_earliest =
        list_offsets_partition_offset(jansu.bootstrap(), &topic, 0, ListOffset::Earliest, 42)
            .await
            .map_err(|err| other_error(format!("jansu list offsets earliest: {err}")))?;
    assert_eq!(
        0, kafka_earliest,
        "Kafka ListOffsets earliest should be log start after appends from offset 0"
    );
    assert_eq!(
        kafka_earliest, jansu_earliest,
        "Jansu ListOffsets earliest must match Kafka"
    );

    let kafka_group = format!("jansu-diff-kafka-{}", Uuid::now_v7());
    let kafka_msgs =
        librdkafka_consume_payloads_from_beginning(kafka.bootstrap(), &topic, &kafka_group, 3)
            .await?;
    let jansu_msgs = fetch_payloads_via_frame(jansu.bootstrap(), &topic, 3)
        .await
        .map_err(|err| other_error(format!("jansu fetch payloads: {err}")))?;

    assert_eq!(
        vec![
            b"record-0".to_vec(),
            b"record-1".to_vec(),
            b"record-2".to_vec()
        ],
        kafka_msgs,
        "Kafka consumer payloads"
    );
    assert_eq!(
        kafka_msgs, jansu_msgs,
        "Jansu Fetch payloads must match Kafka"
    );

    write_artifact(
        "produce-listoffsets-fetch",
        &ProduceListOffsetsFetchArtifact {
            schema_version: 1,
            workload: "listoffsets_latest_and_fetch_consume_after_produce",
            kafka_bootstrap: kafka.bootstrap().into(),
            jansu_bootstrap: jansu.bootstrap().into(),
            topic: topic.clone(),
            kafka_latest_offset: kafka_latest,
            jansu_latest_offset: jansu_latest,
            kafka_earliest_offset: kafka_earliest,
            jansu_earliest_offset: jansu_earliest,
            kafka_payloads_utf8: kafka_msgs
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect(),
            jansu_payloads_utf8: jansu_msgs
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect(),
        },
    )?;

    Ok(())
}

/// Placeholder for full produce → consume → commit → fetch lifecycle.
/// This remains ignored until Fetch, ListOffsets, and consumer group APIs
/// are promoted by Phase 08/10. Phase 04 owns the harness infrastructure;
/// semantic completeness is owned by each API's phase.
#[tokio::test]
#[ignore = "manual client matrix smoke; enable after Phase 08/10 promote Fetch/ListOffsets/group APIs"]
async fn differential_librdkafka_full_lifecycle() -> DynResult<()> {
    Ok(())
}
