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
use jansu_sans_io::{ApiKey, OffsetFetchResponse};

use super::{Codec, CodecRequest, CodecResponse};

pub(super) fn offset_fetch_request_v3_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 65, 0, 9, 0, 3, 0, 0, 0, 0, 255, 255, 0, 3, 97, 98, 99, 0, 0, 0, 2, 0, 5, 116,
            101, 115, 116, 50, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 5, 116, 101, 115,
            116, 49, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2,
        ]),
        name: "offset_fetch_request_v3_000".into(),
    }
    .into()
}

pub(super) fn offset_fetch_request_v7_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 87, 0, 9, 0, 7, 0, 0, 0, 8, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 26, 101,
            120, 97, 109, 112, 108, 101, 95, 99, 111, 110, 115, 117, 109, 101, 114, 95, 103, 114,
            111, 117, 112, 95, 105, 100, 2, 10, 98, 101, 110, 99, 104, 109, 97, 114, 107, 8, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 1, 0,
        ]),
        name: "offset_fetch_request_v7_000".into(),
    }
    .into()
}

pub(super) fn offset_fetch_response_v7_000() -> Codec {
    let api_key = OffsetFetchResponse::KEY;
    let api_version = 7;

    let expected = Bytes::from_static(&[
        0, 0, 0, 165, 0, 0, 0, 8, 0, 0, 0, 0, 0, 2, 10, 98, 101, 110, 99, 104, 109, 97, 114, 107,
        8, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0,
        0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0,
        6, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 5, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 4, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 3, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 2, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 0,
    ]);

    CodecResponse {
        expected,
        name: "offset_fetch_response_v7_000".into(),
        api_key,
        api_version,
    }
    .into()
}

pub(super) fn offset_fetch_request_v9_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 76, 0, 9, 0, 9, 0, 0, 0, 7, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
            111, 110, 115, 117, 109, 101, 114, 0, 2, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115,
            117, 109, 101, 114, 45, 103, 114, 111, 117, 112, 0, 255, 255, 255, 255, 2, 5, 116, 101,
            115, 116, 4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 1, 0,
        ]),
        name: "offset_fetch_request_v9_000".into(),
    }
    .into()
}

pub(super) fn offset_commit_request_v9_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 120, 0, 8, 0, 9, 0, 0, 0, 10, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
            111, 110, 115, 117, 109, 101, 114, 0, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115,
            117, 109, 101, 114, 45, 103, 114, 111, 117, 112, 0, 0, 0, 0, 5, 49, 48, 48, 48, 0, 2,
            5, 116, 101, 115, 116, 4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 1, 0, 0, 0,
        ]),
        name: "offset_commit_request_v9_000".into(),
    }
    .into()
}

pub(super) fn offset_for_leader_request_v0_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 31, 0, 23, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98,
            99, 97, 98, 99, 97, 98, 0, 0, 0, 0,
        ]),
        name: "offset_for_leader_request_v0_000".into(),
    }
    .into()
}
