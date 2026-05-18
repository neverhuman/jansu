// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Embed a Jansu broker in your own binary — no TCP, no docker, no Kafka
//! deployment. In-memory storage, single-process producer + consumer.
//!
//! Run with: `cargo run --example single_binary -p jansu-embedded`

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let broker = jansu_embedded::EmbeddedBroker::new().await?;
    broker.create_topic("greetings", 1).await?;

    for body in ["hello", "world", "from", "jansu-embedded"] {
        let offset = broker.send("greetings", 0, None, body.as_bytes()).await?;
        println!("produced offset={offset} body={body}");
    }

    let mut consumer = broker.consumer("greetings", 0, 0);
    while let Some(record) = consumer.next().await? {
        println!(
            "consumed offset={} body={}",
            record.offset,
            String::from_utf8_lossy(&record.payload)
        );
        if record.offset >= 3 {
            break;
        }
    }
    Ok(())
}
