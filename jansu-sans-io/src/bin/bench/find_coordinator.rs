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
use jansu_sans_io::{ApiKey, FindCoordinatorResponse};

use super::{Codec, CodecRequest, CodecResponse};

pub(super) fn find_coordinator_request_v1_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 19, 0, 10, 0, 1, 0, 0, 0, 0, 255, 255, 0, 6, 97, 98, 99, 100, 101, 102, 0,
        ]),
        name: "find_coordinator_request_v1_000".into(),
    }
    .into()
}

pub(super) fn find_coordinator_response_v1_000() -> Codec {
    let api_key = FindCoordinatorResponse::KEY;
    let api_version = 1;

    CodecResponse {
        expected: Bytes::from_static(&[
            0, 0, 0, 62, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 3, 234, 0, 40, 105, 112, 45,
            49, 48, 45, 50, 45, 57, 49, 45, 54, 54, 46, 101, 117, 45, 119, 101, 115, 116, 45, 49,
            46, 99, 111, 109, 112, 117, 116, 101, 46, 105, 110, 116, 101, 114, 110, 97, 108, 0, 0,
            35, 132,
        ]),
        name: "find_coordinator_response_v1_000".into(),
        api_key,
        api_version,
    }
    .into()
}

pub(super) fn find_coordinator_request_v1_001() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 36, 0, 10, 0, 1, 0, 0, 0, 2, 0, 15, 97, 105, 111, 107, 97, 102, 107, 97, 45,
            48, 46, 49, 50, 46, 48, 0, 8, 109, 121, 45, 103, 114, 111, 117, 112, 0,
        ]),
        name: "find_coordinator_request_v1_001".into(),
    }
    .into()
}

pub(super) fn find_coordinator_response_v1_001() -> Codec {
    let expected = Bytes::from_static(&[
        0, 0, 0, 35, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 4, 78, 79, 78, 69, 0, 0, 0, 111, 0, 9, 108,
        111, 99, 97, 108, 104, 111, 115, 116, 0, 0, 35, 132,
    ]);
    let api_key = FindCoordinatorResponse::KEY;
    let api_version = 1;

    CodecResponse {
        expected,
        name: "find_coordinator_response_v1_001".into(),
        api_key,
        api_version,
    }
    .into()
}

pub(super) fn find_coordinator_request_v2_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 45, 0, 10, 0, 2, 0, 0, 0, 3, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 25,
            101, 120, 97, 109, 112, 108, 101, 95, 99, 111, 110, 115, 117, 109, 101, 114, 95, 103,
            114, 111, 117, 112, 95, 105, 100, 0,
        ]),
        name: "find_coordinator_request_v2_000".into(),
    }
    .into()
}

pub(super) fn find_coordinator_request_v4_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 50, 0, 10, 0, 4, 0, 0, 0, 0, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
            111, 110, 115, 117, 109, 101, 114, 0, 0, 2, 20, 116, 101, 115, 116, 45, 99, 111, 110,
            115, 117, 109, 101, 114, 45, 103, 114, 111, 117, 112, 0,
        ]),
        name: "find_coordinator_request_v4_000".into(),
    }
    .into()
}
