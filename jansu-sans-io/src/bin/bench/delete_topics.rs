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

use super::{Codec, CodecRequest};

pub(super) fn delete_topics_request_v6_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 52, 0, 20, 0, 6, 0, 0, 0, 4, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 2, 5, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 117, 48, 0,
        ]),
        name: "delete_topics_request_v6_000".into(),
    }
    .into()
}

pub(super) fn describe_cluster_request_v1_000() -> Codec {
    CodecRequest {
        expected: Bytes::from_static(&[
            0, 0, 0, 27, 0, 60, 0, 1, 0, 0, 0, 7, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 0, 1, 0,
        ]),
        name: "describe_cluster_request_v1_000".into(),
    }
    .into()
}
