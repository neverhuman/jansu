// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Each partition has an independent offset stream.

#![allow(unused_results)]

use jansu_embedded::EmbeddedBroker;

#[tokio::test]
async fn partitions_are_independent_streams() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    broker.create_topic("multipart", 3).await.expect("create");

    // produce 2 records to partition 0, 1 to partition 1, 3 to partition 2
    broker
        .send("multipart", 0, None, b"p0-a")
        .await
        .expect("p0a");
    broker
        .send("multipart", 0, None, b"p0-b")
        .await
        .expect("p0b");
    broker
        .send("multipart", 1, None, b"p1-a")
        .await
        .expect("p1a");
    broker
        .send("multipart", 2, None, b"p2-a")
        .await
        .expect("p2a");
    broker
        .send("multipart", 2, None, b"p2-b")
        .await
        .expect("p2b");
    broker
        .send("multipart", 2, None, b"p2-c")
        .await
        .expect("p2c");

    // each consumer reads its own partition only
    let mut p0_payloads = Vec::new();
    let mut c0 = broker.consumer("multipart", 0, 0);
    while let Some(r) = c0.next().await.expect("p0 fetch") {
        p0_payloads.push(r.payload);
        if p0_payloads.len() >= 2 {
            break;
        }
    }

    let mut p1_payloads = Vec::new();
    let mut c1 = broker.consumer("multipart", 1, 0);
    while let Some(r) = c1.next().await.expect("p1 fetch") {
        p1_payloads.push(r.payload);
        if !p1_payloads.is_empty() {
            break;
        }
    }

    let mut p2_payloads = Vec::new();
    let mut c2 = broker.consumer("multipart", 2, 0);
    while let Some(r) = c2.next().await.expect("p2 fetch") {
        p2_payloads.push(r.payload);
        if p2_payloads.len() >= 3 {
            break;
        }
    }

    assert_eq!(p0_payloads, vec![b"p0-a".to_vec(), b"p0-b".to_vec()]);
    assert_eq!(p1_payloads, vec![b"p1-a".to_vec()]);
    assert_eq!(
        p2_payloads,
        vec![b"p2-a".to_vec(), b"p2-b".to_vec(), b"p2-c".to_vec()]
    );
}
