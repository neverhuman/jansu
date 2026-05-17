// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Consumer::next() over many records returns them in offset order.

#![allow(unused_results)]

use jansu_embedded::EmbeddedBroker;

#[tokio::test]
async fn consumes_all_in_order() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    broker.create_topic("seq", 1).await.expect("create");

    let mut offsets = Vec::new();
    for i in 0..10_i32 {
        let payload = format!("msg-{i}");
        let off = broker
            .send("seq", 0, None, payload.as_bytes())
            .await
            .expect("produce");
        offsets.push(off);
    }
    assert_eq!(offsets.len(), 10);

    let mut consumer = broker.consumer("seq", 0, 0);
    let mut received = Vec::new();
    while let Some(record) = consumer.next().await.expect("fetch") {
        received.push((record.offset, record.payload));
        if received.len() >= 10 {
            break;
        }
    }
    assert_eq!(received.len(), 10);
    // Offsets are monotonically increasing
    for window in received.windows(2) {
        assert!(window[0].0 < window[1].0);
    }
    // Payloads match
    for (i, (_, payload)) in received.iter().enumerate() {
        assert_eq!(payload, format!("msg-{i}").as_bytes());
    }
}

#[tokio::test]
async fn next_on_empty_returns_none() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    broker.create_topic("empty", 1).await.expect("create");

    let mut consumer = broker.consumer("empty", 0, 0);
    let result = consumer.next().await.expect("fetch ok");
    assert!(result.is_none());
}

#[tokio::test]
async fn seek_skips_ahead() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    broker.create_topic("seek", 1).await.expect("create");
    for i in 0..5_i32 {
        let payload = format!("p{i}");
        broker
            .send("seek", 0, None, payload.as_bytes())
            .await
            .expect("produce");
    }

    let mut consumer = broker.consumer("seek", 0, 0);
    // Read first record
    let r0 = consumer.next().await.expect("fetch").expect("one");
    assert_eq!(r0.payload, b"p0");

    // Seek to offset 3 — should skip p1 and p2
    consumer.seek(r0.offset + 3);
    let r3 = consumer.next().await.expect("fetch").expect("one");
    assert_eq!(r3.payload, b"p3");
}
