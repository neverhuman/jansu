// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Multiple topics on one broker: no cross-topic message bleed.

#![allow(unused_results)]

use jansu_embedded::EmbeddedBroker;

#[tokio::test]
async fn three_topics_isolated() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    broker.create_topic("alpha", 1).await.expect("alpha");
    broker.create_topic("beta", 1).await.expect("beta");
    broker.create_topic("gamma", 1).await.expect("gamma");

    broker.send("alpha", 0, None, b"A1").await.expect("a1");
    broker.send("alpha", 0, None, b"A2").await.expect("a2");
    broker.send("beta", 0, None, b"B1").await.expect("b1");
    broker.send("gamma", 0, None, b"G1").await.expect("g1");
    broker.send("gamma", 0, None, b"G2").await.expect("g2");
    broker.send("gamma", 0, None, b"G3").await.expect("g3");

    let mut alpha_payloads = Vec::new();
    let mut beta_payloads = Vec::new();
    let mut gamma_payloads = Vec::new();

    let mut ac = broker.consumer("alpha", 0, 0);
    while let Some(r) = ac.next().await.expect("alpha fetch") {
        alpha_payloads.push(r.payload);
        if alpha_payloads.len() >= 2 {
            break;
        }
    }

    let mut bc = broker.consumer("beta", 0, 0);
    while let Some(r) = bc.next().await.expect("beta fetch") {
        beta_payloads.push(r.payload);
        if !beta_payloads.is_empty() {
            break;
        }
    }

    let mut gc = broker.consumer("gamma", 0, 0);
    while let Some(r) = gc.next().await.expect("gamma fetch") {
        gamma_payloads.push(r.payload);
        if gamma_payloads.len() >= 3 {
            break;
        }
    }

    assert_eq!(alpha_payloads, vec![b"A1".to_vec(), b"A2".to_vec()]);
    assert_eq!(beta_payloads, vec![b"B1".to_vec()]);
    assert_eq!(
        gamma_payloads,
        vec![b"G1".to_vec(), b"G2".to_vec(), b"G3".to_vec()]
    );
}
