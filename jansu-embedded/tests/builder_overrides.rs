// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! EmbeddedBrokerBuilder accepts custom cluster_id, broker_id, incarnation.

#![allow(unused_results)]

use jansu_embedded::EmbeddedBroker;
use uuid::Uuid;

#[tokio::test]
async fn custom_cluster_and_broker_id_apply() {
    let incarnation = Uuid::now_v7();
    let broker = EmbeddedBroker::builder()
        .cluster_id("integration-test-cluster")
        .broker_id(42)
        .incarnation_id(incarnation)
        .build()
        .await
        .expect("build");

    // Sanity: still produces + consumes after custom configuration.
    broker.create_topic("custom", 1).await.expect("create");
    broker.send("custom", 0, None, b"x").await.expect("send");
    let mut consumer = broker.consumer("custom", 0, 0);
    let record = consumer.next().await.expect("fetch").expect("one");
    assert_eq!(record.payload, b"x");
}

#[tokio::test]
async fn create_topic_zero_partitions_errors() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    let err = broker
        .create_topic("bad", 0)
        .await
        .expect_err("should reject 0 partitions");
    assert!(matches!(err, jansu_embedded::Error::Misuse(_)));
}
