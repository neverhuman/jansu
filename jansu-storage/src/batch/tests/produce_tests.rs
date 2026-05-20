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

//! Time-window batching behaviour tests for [`ProduceRequestBatcher`].

use std::time::Duration;

use bytes::Bytes;
use jansu_sans_io::BatchAttribute;
use tokio::{task::yield_now, time::advance};
use tracing::debug;

use super::flight_recorder::FlightRecorder;
use super::into_batch;
use crate::{Result, Storage, Topition, batch::ProduceRequestBatcher};

#[tokio::test(start_paused = true)]
async fn single_produce_in_window() -> Result<()> {
    const MINIMUM_DELAY: Duration = Duration::from_secs(1);
    const ADVANCE_DELAY: Duration = Duration::from_secs(5);

    let recorder = FlightRecorder::new();
    let storage =
        ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

    let producer_id = 54345;
    let producer_epoch = 32123;
    let base_offset = 0;
    let attributes: i16 = BatchAttribute::default().into();

    let transaction_id = None;
    let abc0 = Topition::new("abc", 0);

    const A: Bytes = Bytes::from_static(b"a");
    const B: Bytes = Bytes::from_static(b"b");
    const C: Bytes = Bytes::from_static(b"c");

    let batch_a = {
        let storage = storage.clone();
        let abc0 = abc0.clone();

        tokio::spawn(async move {
            storage
                .produce(
                    transaction_id,
                    &abc0,
                    into_batch(
                        attributes,
                        producer_id,
                        producer_epoch,
                        base_offset,
                        &[A, B, C],
                    )?,
                )
                .await
        })
    };

    advance(ADVANCE_DELAY).await;
    yield_now().await;

    let response_a = batch_a
        .await
        .expect("join_handle")
        .inspect(|produce_response| debug!(?produce_response))?;
    assert_eq!(0, response_a);

    let sent = recorder.produced(&abc0)?.unwrap();
    assert_eq!(1, sent.len());
    assert_eq!(3, sent[0].records.len());
    assert_eq!(Some(A), sent[0].records[0].value());
    assert_eq!(Some(B), sent[0].records[1].value());
    assert_eq!(Some(C), sent[0].records[2].value());

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn two_produces_in_window() -> Result<()> {
    const MINIMUM_DELAY: Duration = Duration::from_secs(1);
    const ADVANCE_DELAY: Duration = Duration::from_secs(5);

    let recorder = FlightRecorder::new();
    let storage =
        ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

    let producer_id = 54345;
    let producer_epoch = 32123;
    let base_offset = 0;
    let attributes: i16 = BatchAttribute::default().into();

    let transaction_id = None;
    let abc0 = Topition::new("abc", 0);

    const A: Bytes = Bytes::from_static(b"a");
    const B: Bytes = Bytes::from_static(b"b");
    const C: Bytes = Bytes::from_static(b"c");

    let batch_a = {
        let storage = storage.clone();
        let abc0 = abc0.clone();

        tokio::spawn(async move {
            storage
                .produce(
                    transaction_id,
                    &abc0,
                    into_batch(
                        attributes,
                        producer_id,
                        producer_epoch,
                        base_offset,
                        &[A, B, C],
                    )?,
                )
                .await
        })
    };

    const D: Bytes = Bytes::from_static(b"d");
    const E: Bytes = Bytes::from_static(b"e");

    let batch_b = {
        let storage = storage.clone();
        let abc0 = abc0.clone();

        tokio::spawn(async move {
            storage
                .produce(
                    transaction_id,
                    &abc0,
                    into_batch(
                        attributes,
                        producer_id,
                        producer_epoch,
                        base_offset,
                        &[D, E],
                    )?,
                )
                .await
        })
    };

    advance(ADVANCE_DELAY).await;
    yield_now().await;

    let response_a = batch_a
        .await
        .expect("join_handle")
        .inspect(|produce_response| debug!(?produce_response))?;
    assert_eq!(0, response_a);

    let response_b = batch_b
        .await
        .expect("join_handle")
        .inspect(|produce_response| debug!(?produce_response))?;
    assert_eq!(0, response_b);

    let sent = recorder.produced(&abc0)?.unwrap();
    assert_eq!(1, sent.len());
    assert_eq!(5, sent[0].records.len());
    assert_eq!(Some(A), sent[0].records[0].value());
    assert_eq!(Some(B), sent[0].records[1].value());
    assert_eq!(Some(C), sent[0].records[2].value());
    assert_eq!(Some(D), sent[0].records[3].value());
    assert_eq!(Some(E), sent[0].records[4].value());

    Ok(())
}
