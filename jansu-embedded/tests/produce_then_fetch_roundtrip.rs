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

//! Smoke test: produce a record, fetch it back via Consumer::next().

#![allow(unused_results)]

use jansu_embedded::EmbeddedBroker;

#[tokio::test]
async fn single_record_round_trip() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    broker.create_topic("smoke", 1).await.expect("create topic");

    let offset = broker
        .send("smoke", 0, Some(b"key-1"), b"hello")
        .await
        .expect("produce");
    assert!(offset >= 0);

    let mut consumer = broker.consumer("smoke", 0, 0);
    let record = consumer
        .next()
        .await
        .expect("fetch")
        .expect("at least one record");

    assert_eq!(record.topic, "smoke");
    assert_eq!(record.partition, 0);
    assert_eq!(record.offset, offset);
    assert_eq!(record.key.as_deref(), Some(b"key-1".as_slice()));
    assert_eq!(record.payload, b"hello");
}

#[tokio::test]
async fn send_without_key_works() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    broker.create_topic("nokey", 1).await.expect("create");

    broker
        .send("nokey", 0, None, b"value-only")
        .await
        .expect("produce");

    let mut consumer = broker.consumer("nokey", 0, 0);
    let record = consumer.next().await.expect("fetch").expect("one record");
    assert_eq!(record.key, None);
    assert_eq!(record.payload, b"value-only");
}
