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
use jansu_sans_io::{ApiKey, MetadataResponse};

use super::{Codec, CodecRequest, CodecResponse};

pub(super) fn metadata_request_v1_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 30, 0, 3, 0, 1, 0, 0, 0, 1, 0, 5, 115, 97, 109, 115, 97, 0, 0, 0, 1, 0, 9, 98,
            101, 110, 99, 104, 109, 97, 114, 107,
        ]),
        name: "metadata_request_v1_000".into(),
    }
    .into()
}

pub(super) fn metadata_response_v1_000() -> Codec {
    let api_key = MetadataResponse::KEY;
    let api_version = 1;

    let expected = Bytes::from_static(&[
        0, 0, 0, 237, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 9, 108, 111, 99, 97, 108, 104, 111,
        115, 116, 0, 0, 35, 132, 255, 255, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 9, 98, 101, 110, 99,
        104, 109, 97, 114, 107, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0,
        1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0,
        1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0,
        0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 0,
        0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1,
    ]);

    CodecResponse {
        expected,
        name: "metadata_response_v1_000".into(),
        api_key,
        api_version,
    }
    .into()
}

pub(super) fn metadata_request_v1_001() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 39, 0, 3, 0, 1, 0, 0, 0, 3, 0, 15, 97, 105, 111, 107, 97, 102, 107, 97, 45,
            48, 46, 49, 50, 46, 48, 0, 0, 0, 1, 0, 8, 99, 117, 115, 116, 111, 109, 101, 114,
        ]),
        name: "metadata_request_v1_001".into(),
    }
    .into()
}

pub(super) fn metadata_response_v1_001() -> Codec {
    let api_key = MetadataResponse::KEY;
    let api_version = 1;

    let expected = Bytes::from_static(&[
        0, 0, 0, 132, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 111, 0, 9, 108, 111, 99, 97, 108, 104, 111,
        115, 116, 0, 0, 35, 132, 255, 255, 0, 0, 0, 111, 0, 0, 0, 1, 0, 0, 0, 8, 99, 117, 115, 116,
        111, 109, 101, 114, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 111, 0, 0, 0, 1, 0, 0, 0,
        111, 0, 0, 0, 1, 0, 0, 0, 111, 0, 0, 0, 0, 0, 1, 0, 0, 0, 111, 0, 0, 0, 1, 0, 0, 0, 111, 0,
        0, 0, 1, 0, 0, 0, 111, 0, 0, 0, 0, 0, 2, 0, 0, 0, 111, 0, 0, 0, 1, 0, 0, 0, 111, 0, 0, 0,
        1, 0, 0, 0, 111,
    ]);

    CodecResponse {
        expected,
        name: "metadata_response_v1_001".into(),
        api_key,
        api_version,
    }
    .into()
}

pub(super) fn metadata_request_v1_002() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 39, 0, 3, 0, 1, 0, 0, 0, 3, 0, 15, 97, 105, 111, 107, 97, 102, 107, 97, 45,
            48, 46, 49, 50, 46, 48, 0, 0, 0, 1, 0, 8, 99, 117, 115, 116, 111, 109, 101, 114,
        ]),
        name: "metadata_request_v1_002".into(),
    }
    .into()
}

pub(super) fn metadata_response_v1_002() -> Codec {
    let api_key = 3;
    let api_version = 1;

    let expected = Bytes::from_static(&[
        0, 0, 0, 132, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 111, 0, 9, 108, 111, 99, 97, 108, 104, 111,
        115, 116, 0, 0, 35, 132, 255, 255, 0, 0, 0, 111, 0, 0, 0, 1, 0, 0, 0, 8, 99, 117, 115, 116,
        111, 109, 101, 114, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 111, 0, 0, 0, 1, 0, 0, 0,
        111, 0, 0, 0, 1, 0, 0, 0, 111, 0, 0, 0, 0, 0, 1, 0, 0, 0, 111, 0, 0, 0, 1, 0, 0, 0, 111, 0,
        0, 0, 1, 0, 0, 0, 111, 0, 0, 0, 0, 0, 2, 0, 0, 0, 111, 0, 0, 0, 1, 0, 0, 0, 111, 0, 0, 0,
        1, 0, 0, 0, 111,
    ]);

    CodecResponse {
        expected,
        name: "metadata_response_v1_002".into(),
        api_key,
        api_version,
    }
    .into()
}

pub(super) fn metadata_request_v7_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 21, 0, 3, 0, 7, 0, 0, 0, 0, 0, 6, 115, 97, 114, 97, 109, 97, 255, 255, 255,
            255, 0,
        ]),
        name: "metadata_request_v7_000".into(),
    }
    .into()
}

pub(super) fn metadata_response_v7_000() -> Codec {
    let api_key = MetadataResponse::KEY;
    let api_version = 7;

    let expected = Bytes::from_static(&[
        0, 0, 0, 180, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 9, 108, 111, 99, 97, 108,
        104, 111, 115, 116, 0, 0, 35, 132, 255, 255, 0, 22, 53, 76, 54, 103, 51, 110, 83, 104, 84,
        45, 101, 77, 67, 116, 75, 45, 45, 88, 56, 54, 115, 119, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 4,
        116, 101, 115, 116, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0,
        0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
        0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0,
    ]);

    CodecResponse {
        expected,
        name: "metadata_response_v7_000".into(),
        api_key,
        api_version,
    }
    .into()
}

pub(super) fn metadata_request_v12_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 53, 0, 3, 0, 12, 0, 0, 0, 5, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            5, 116, 101, 115, 116, 0, 1, 0, 0,
        ]),
        name: "metadata_request_v12_000".into(),
    }
    .into()
}

pub(super) fn metadata_response_v12_000() -> Codec {
    let api_key = 3;
    let api_version = 12;

    let expected = Bytes::from_static(&[
        0, 0, 0, 92, 0, 0, 0, 5, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 13, 107, 97, 102, 107, 97, 45, 115,
        101, 114, 118, 101, 114, 0, 0, 35, 132, 0, 0, 23, 82, 118, 81, 119, 114, 89, 101, 103, 83,
        85, 67, 107, 73, 80, 107, 97, 105, 65, 90, 81, 108, 81, 0, 0, 0, 0, 2, 0, 3, 5, 116, 101,
        115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 128, 0, 0, 0, 0, 0,
    ]);

    CodecResponse {
        expected,
        name: "metadata_response_v12_000".into(),
        api_key,
        api_version,
    }
    .into()
}

pub(super) fn metadata_request_v12_001() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 31, 0, 3, 0, 12, 0, 0, 0, 1, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 1, 1, 0, 0,
        ]),
        name: "metadata_request_v12_001".into(),
    }
    .into()
}

pub(super) fn metadata_request_v12_002() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 49, 0, 3, 0, 12, 0, 0, 0, 2, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 2, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 98, 101, 110, 99, 104, 109, 97, 114,
            107, 0, 1, 0, 0,
        ]),
        name: "metadata_request_v12_002".into(),
    }
    .into()
}

pub(super) fn metadata_response_v12_002() -> Codec {
    let api_key = 3;
    let api_version = 12;

    let expected = Bytes::from_static(&[
        0, 0, 1, 20, 0, 0, 0, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 10, 108, 111, 99, 97, 108, 104, 111,
        115, 116, 0, 0, 35, 132, 0, 0, 23, 53, 76, 54, 103, 51, 110, 83, 104, 84, 45, 101, 77, 67,
        116, 75, 45, 45, 88, 56, 54, 115, 119, 0, 0, 0, 1, 2, 0, 0, 10, 98, 101, 110, 99, 104, 109,
        97, 114, 107, 177, 248, 14, 236, 65, 78, 72, 57, 179, 196, 215, 75, 145, 238, 120, 241, 0,
        8, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0,
        0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 6, 0,
        0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0,
        0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 0, 2,
        0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1,
        2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0,
        1, 1, 0, 128, 0, 0, 0, 0, 0,
    ]);
    CodecResponse {
        expected,
        name: "metadata_response_v12_002".into(),
        api_key,
        api_version,
    }
    .into()
}
