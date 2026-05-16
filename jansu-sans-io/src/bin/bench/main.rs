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

use bytes::{Buf, Bytes};
use clap::Parser;
use jansu_sans_io::{Frame, Result};

mod api_versions;
mod create_topics;
mod delete_topics;
mod describe_configs;
mod describe_groups;
mod describe_topic_partitions;
mod fetch;
mod find_coordinator;
mod group;
mod list;
mod metadata;
mod offset;
mod produce_large;
mod produce_small;

trait CodecTest {
    fn name(&self) -> &str;
    fn test(&self) -> Result<()>;
}

enum Codec {
    Request(CodecRequest),
    Response(CodecResponse),
}

impl Codec {
    fn is_request(&self) -> bool {
        matches!(self, Self::Request(_))
    }

    fn is_response(&self) -> bool {
        matches!(self, Self::Response(_))
    }

    fn api_key(&self) -> i16 {
        match self {
            Codec::Request(codec_request) => codec_request.api_key(),
            Codec::Response(codec_response) => codec_response.api_key(),
        }
    }
}

impl From<CodecRequest> for Codec {
    fn from(value: CodecRequest) -> Self {
        Self::Request(value)
    }
}

impl From<CodecResponse> for Codec {
    fn from(value: CodecResponse) -> Self {
        Self::Response(value)
    }
}

impl CodecTest for Codec {
    fn name(&self) -> &str {
        match self {
            Codec::Request(codec_request) => codec_request.name(),
            Codec::Response(codec_response) => codec_response.name(),
        }
    }

    fn test(&self) -> Result<()> {
        match self {
            Codec::Request(codec_request) => codec_request.test(),
            Codec::Response(codec_response) => codec_response.test(),
        }
    }
}

struct CodecRequest {
    expected: Bytes,
    name: String,
}

impl CodecRequest {
    fn api_key(&self) -> i16 {
        self.expected.slice(4..6).get_i16()
    }
}

impl CodecTest for CodecRequest {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn test(&self) -> Result<()> {
        let frame = Frame::request_from_bytes(self.expected.clone())?;
        let actual = Frame::request(frame.header, frame.body)?;
        assert_eq!(self.expected, &actual[..]);
        Ok(())
    }
}

struct CodecResponse {
    expected: Bytes,
    name: String,
    api_key: i16,
    api_version: i16,
}

impl CodecResponse {
    fn api_key(&self) -> i16 {
        self.api_key
    }
}

impl CodecTest for CodecResponse {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn test(&self) -> Result<()> {
        let frame =
            Frame::response_from_bytes(self.expected.clone(), self.api_key, self.api_version)?;
        let actual = Frame::response(frame.header, frame.body, self.api_key, self.api_version)?;
        assert_eq!(self.expected, &actual[..]);
        Ok(())
    }
}

#[derive(Clone, Debug, Parser)]
struct Arg {
    #[arg(long, default_value = "10000")]
    iterations: u32,

    #[arg(long, default_value = "true")]
    request: bool,

    #[arg(long, default_value = "true")]
    response: bool,

    #[arg(long)]
    api_key: Option<i16>,
}

pub fn main() -> Result<()> {
    let benches = vec![
        api_versions::api_versions_request_v0_000(),
        api_versions::api_versions_request_v3_000(),
        api_versions::api_versions_response_v1_000(),
        api_versions::api_versions_response_v3_000(),
        create_topics::create_topics_request_v7_000(),
        create_topics::create_topics_response_v7_000(),
        delete_topics::delete_topics_request_v6_000(),
        delete_topics::describe_cluster_request_v1_000(),
        describe_configs::describe_configs_request_v4_000(),
        describe_configs::describe_configs_request_v4_001(),
        describe_configs::describe_configs_request_v4_002(),
        describe_configs::describe_configs_response_v4_001(),
        describe_configs::describe_configs_response_v4_002(),
        describe_groups::describe_groups_request_v1_000(),
        describe_groups::describe_groups_response_v1_000(),
        describe_topic_partitions::describe_topic_partitions_request_v0_000(),
        describe_topic_partitions::describe_topic_partitions_response_v0_000(),
        fetch::fetch_request_v12_000(),
        fetch::fetch_request_v15_000(),
        fetch::fetch_request_v16_000(),
        fetch::fetch_request_v16_001(),
        fetch::fetch_request_v6_000(),
        fetch::fetch_response_v12_000(),
        fetch::fetch_response_v12_001(),
        fetch::fetch_response_v12_002(),
        fetch::fetch_response_v16_001(),
        fetch::fetch_response_v16_002(),
        fetch::fetch_response_v17_body_1024(),
        find_coordinator::find_coordinator_request_v1_000(),
        find_coordinator::find_coordinator_request_v1_001(),
        find_coordinator::find_coordinator_request_v2_000(),
        find_coordinator::find_coordinator_request_v4_000(),
        find_coordinator::find_coordinator_response_v1_000(),
        find_coordinator::find_coordinator_response_v1_001(),
        group::heartbeat_request_v4_000(),
        group::init_producer_id_request_v4_000(),
        group::join_group_request_v5_000(),
        group::join_group_request_v5_001(),
        group::join_group_request_v9_000(),
        group::join_group_response_v5_000(),
        group::leave_group_request_v5_000(),
        list::list_groups_request_v4_000(),
        list::list_offsets_response_v0_000(),
        list::list_partition_reassignments_request_v0_000(),
        list::list_transactions_response_v1_000(),
        metadata::metadata_request_v12_000(),
        metadata::metadata_request_v12_001(),
        metadata::metadata_request_v12_002(),
        metadata::metadata_request_v1_000(),
        metadata::metadata_request_v1_001(),
        metadata::metadata_request_v1_002(),
        metadata::metadata_request_v7_000(),
        metadata::metadata_response_v12_000(),
        metadata::metadata_response_v12_002(),
        metadata::metadata_response_v1_000(),
        metadata::metadata_response_v1_001(),
        metadata::metadata_response_v1_002(),
        metadata::metadata_response_v7_000(),
        offset::offset_commit_request_v9_000(),
        offset::offset_fetch_request_v3_000(),
        offset::offset_fetch_request_v7_000(),
        offset::offset_fetch_request_v9_000(),
        offset::offset_fetch_response_v7_000(),
        offset::offset_for_leader_request_v0_000(),
        produce_small::produce_request_v0_000(),
        produce_small::produce_request_v10_000(),
        produce_small::produce_request_v10_001(),
        produce_small::produce_request_v10_001(),
        produce_small::produce_request_v10_002(),
        produce_small::produce_request_v10_003(),
        produce_large::produce_request_v11_body_1024(),
        produce_large::produce_request_v11_body_2048(),
        produce_small::produce_request_v3_000(),
        produce_small::produce_request_v7_000(),
        produce_small::produce_request_v9_000(),
        produce_small::produce_request_v9_001(),
        produce_small::produce_response_v9_000(),
        group::sync_group_request_v5_000(),
    ];

    let cli = Arg::parse();

    for _iteration in 0..cli.iterations {
        for bench in benches
            .iter()
            .filter(|bench| {
                bench.is_request() == cli.request || bench.is_response() == cli.response
            })
            .filter(|bench| cli.api_key.is_none_or(|api_key| api_key == bench.api_key()))
        {
            bench
                .test()
                .inspect_err(|err| eprintln!("{}: {err:?}", bench.name()))?
        }
    }

    Ok(())
}
