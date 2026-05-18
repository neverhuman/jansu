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

mod composite;
mod context;
mod encoder;
mod record_batch;
mod scalar;

pub use encoder::Encoder;
pub use record_batch::RecordBatchEncoder;

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use serde::Serialize;

    use super::RecordBatchEncoder;
    use crate::{ApiKey as _, ApiVersionsRequest, Frame, Header, Result, record::Record};

    #[test]
    fn api_versions_request_encoding_is_byte_stable() -> Result<()> {
        let header = Header::Request {
            api_key: ApiVersionsRequest::KEY,
            api_version: 3,
            correlation_id: 3,
            client_id: Some("console-producer".into()),
        };

        let body = ApiVersionsRequest::default()
            .client_software_name(Some("apache-kafka-java".into()))
            .client_software_version(Some("3.6.1".into()))
            .into();

        assert_eq!(
            Bytes::from_static(&[
                0, 0, 0, 52, 0, 18, 0, 3, 0, 0, 0, 3, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45,
                112, 114, 111, 100, 117, 99, 101, 114, 0, 18, 97, 112, 97, 99, 104, 101, 45, 107,
                97, 102, 107, 97, 45, 106, 97, 118, 97, 6, 51, 46, 54, 46, 49, 0,
            ]),
            Frame::request(header, body)?,
        );

        Ok(())
    }

    #[test]
    fn record_batch_encoding_is_byte_stable() -> Result<()> {
        let record = Record::builder().value(Some(Bytes::from_static(b"def")));
        let mut encoder = RecordBatchEncoder::new(BytesMut::new());

        record.serialize(&mut encoder)?;

        assert_eq!(
            Bytes::from_static(&[18, 0, 0, 0, 1, 6, 100, 101, 102, 0]),
            Bytes::from(encoder),
        );

        Ok(())
    }
}
