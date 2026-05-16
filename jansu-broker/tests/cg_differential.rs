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

use rand::{prelude::*, rng};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

#[tokio::test]
async fn differential_cg_churn() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = common::init_tracing()?;

    let cluster_id = Uuid::now_v7().to_string();
    let node_id = rng().random_range(0..i32::MAX);
    // Use a random high port to avoid conflicts
    let port = rng().random_range(10000..60000);
    let listener = Url::parse(&format!("tcp://127.0.0.1:{}", port))?;
    let advertised_listener = Url::parse(&format!("tcp://127.0.0.1:{}", port))?;
    let storage = Url::parse("slatedb://memory")?;

    let broker = jansu_broker::broker::Broker::<
        jansu_broker::coordinator::group::administrator::Controller<jansu_storage::ArcDynStorage>,
        jansu_storage::ArcDynStorage,
    >::builder()
    .node_id(node_id)
    .cluster_id(cluster_id)
    .incarnation_id(Uuid::now_v7())
    .listener(listener)
    .advertised_listener(advertised_listener)
    .storage(storage)
    .build()
    .await?;

    let started = Instant::now();
    let _broker_task = tokio::spawn(async move {
        broker.main(started).await.unwrap();
    });

    // Wait for the broker to start
    sleep(Duration::from_millis(1000)).await;

    // Start 3 rdkafka consumers
    let mut consumers = vec![];
    for i in 0..3 {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-differential")
            .set("bootstrap.servers", format!("127.0.0.1:{}", port))
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("client.id", &format!("consumer-{}", i))
            .create()?;

        consumer.subscribe(&["test-topic"])?;
        consumers.push(consumer);
    }

    // We'll poll for a bit to let the consumer group stabilize
    // The consumer event loop will negotiate with our coordinator.
    // We expect it to successfully receive an assignment.
    for _i in 0..10 {
        for consumer in &consumers {
            let timeout = sleep(Duration::from_millis(500));
            tokio::pin!(timeout);
            tokio::select! {
                _ = &mut timeout => {}
                res = consumer.recv() => {
                    match res {
                        Err(e) => {
                            debug!("recv error: {:?}", e);
                        }
                        Ok(m) => {
                            debug!("received message: {:?}", m.payload());
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
