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

use bytes::Bytes;
use jansu_sans_io::{ApiKey, ListOffsetsResponse, ListTransactionsResponse};

use super::{Codec, CodecRequest, CodecResponse};

pub(super) fn list_groups_request_v4_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 26, 0, 16, 0, 4, 0, 0, 0, 84, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105,
            101, 110, 116, 45, 49, 0, 1, 0,
        ]),
        name: "list_groups_request_v4_000".into(),
    }
    .into()
}

pub(super) fn list_offsets_response_v0_000() -> Codec {
    let api_key = ListOffsetsResponse::KEY;
    let api_version = 0;

    let expected = Bytes::from_static(&[
        0, 0, 0, 67, 0, 0, 0, 0, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98, 99, 97, 98, 99, 97, 98, 0,
        0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 18, 37, 164, 0, 0, 0, 0, 0, 17, 233,
        252, 0, 0, 0, 0, 0, 17, 198, 100, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);

    CodecResponse {
        expected,
        name: "list_offsets_response_v0_000".into(),
        api_key,
        api_version,
    }
    .into()
}

pub(super) fn list_partition_reassignments_request_v0_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 49, 0, 46, 0, 0, 0, 0, 0, 7, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 0, 0, 117, 48, 2, 5, 116, 101, 115, 116, 4, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0, 2, 0, 0,
        ]),
        name: "list_partition_reassignments_request_v0_000".into(),
    }
    .into()
}

pub(super) fn list_transactions_response_v1_000() -> Codec {
    let api_key = ListTransactionsResponse::KEY;
    let api_version = 1;

    let expected = Bytes::from_static(&[
        0, 0, 0, 70, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 2, 32, 108, 105, 98, 114, 100, 107, 97,
        102, 107, 97, 95, 116, 114, 97, 110, 115, 97, 99, 116, 105, 111, 110, 115, 95, 101, 120,
        97, 109, 112, 108, 101, 0, 0, 0, 0, 0, 0, 0, 0, 15, 67, 111, 109, 112, 108, 101, 116, 101,
        67, 111, 109, 109, 105, 116, 0, 0,
    ]);

    CodecResponse {
        expected,
        name: "list_transactions_response_v1_000".into(),
        api_key,
        api_version,
    }
    .into()
}
