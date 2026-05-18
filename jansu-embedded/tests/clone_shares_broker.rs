// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Cloning the broker handle yields a sibling that reads + writes the same storage.

#![allow(unused_results)]

use jansu_embedded::EmbeddedBroker;

#[tokio::test]
async fn clone_writes_visible_to_original() {
    let producer = EmbeddedBroker::new().await.expect("broker");
    let consumer = producer.clone();

    producer
        .create_topic("clone-topic", 1)
        .await
        .expect("create");
    producer
        .send("clone-topic", 0, Some(b"key"), b"shared payload")
        .await
        .expect("send via producer clone");

    let mut c = consumer.consumer("clone-topic", 0, 0);
    let record = c
        .next()
        .await
        .expect("fetch via clone")
        .expect("at least one");
    assert_eq!(record.payload, b"shared payload");
    assert_eq!(record.key.as_deref(), Some(b"key".as_slice()));
}
