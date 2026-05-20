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

//! Produce-window queue management for [`ProduceRequestBatcher`].

use std::{collections::BTreeSet, time::SystemTime};

use jansu_sans_io::record::deflated;
use opentelemetry::KeyValue;
use tokio::time::sleep;
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use super::{
    BATCH_REQUESTS_LENGTH, BatchRequest, BatchResponse, PRODUCE_DURATION,
    PRODUCE_REQUEST_MINIMUM_SIZE_TRIGGER, PRODUCE_REQUEST_QUEUED_COUNTER,
    PRODUCE_REQUEST_TIMEOUT_EXPIRED_TRIGGER, PRODUCE_REQUEST_YOUR_TICKET_IS_READY,
    ProduceRequestBatcher, SEND_QUEUED_PRODUCED_RECORDS_COUNTER, SEND_QUEUED_WAKE_COUNTER, Ticket,
    TopitionProducerId, combine,
};
use crate::{Error, Result, Storage, Topition};

impl<G> ProduceRequestBatcher<G>
where
    G: Storage + Clone,
{
    #[instrument(skip(self, transaction_id, topition, producer_id))]
    pub(super) async fn send_queued(
        &self,
        id: &Uuid,
        transaction_id: Option<&str>,
        topition: &Topition,
        producer_id: i64,
    ) -> Result<(), Error> {
        let Some(queued) = self.requests.lock().map(|mut requests| {
            BATCH_REQUESTS_LENGTH
                .record(requests.values().map(|queue| queue.len() as u64).sum(), &[]);

            requests.remove(&TopitionProducerId {
                topition: topition.to_owned(),
                producer_id,
            })
        })?
        else {
            BATCH_REQUESTS_LENGTH.record(0, &[]);

            return Ok(());
        };

        let owners = queued
            .iter()
            .map(|batch_request| batch_request.id)
            .collect::<BTreeSet<_>>();

        debug!(owners = owners.len());

        let attributes = [KeyValue::new("topic", topition.topic.clone())];

        if let Some(queued) = combine(queued.into_iter().map(|queued| queued.batch).collect())? {
            let record_count = (queued.last_offset_delta + 1) as u64;

            let offset = self
                .storage
                .produce(transaction_id, topition, queued)
                .await
                .inspect(|offset| debug!(offset))?;

            SEND_QUEUED_PRODUCED_RECORDS_COUNTER.add(record_count, &attributes);

            self.responses.lock().map(|mut responses| {
                for owner in owners {
                    if let Some(BatchResponse::Waker(waker)) =
                        responses.insert(owner, BatchResponse::Response(offset))
                    {
                        debug!(waking = %owner);
                        SEND_QUEUED_WAKE_COUNTER.add(1, &attributes);
                        waker.wake();
                    }
                }
            })?;
        }

        Ok(())
    }

    #[instrument(skip_all, fields(transaction_id, topic = topition.topic, partition = topition.partition))]
    pub(super) async fn produce_batched(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let Some(maximum_delay) = self.maximum_delay else {
            return self
                .storage
                .produce(transaction_id, topition, deflated)
                .await;
        };

        let start = SystemTime::now();

        let attributes = [KeyValue::new("topic", topition.topic.clone())];

        let producer_id = deflated.producer_id;

        let topition_producer_id = TopitionProducerId {
            topition: topition.to_owned(),
            producer_id,
        };

        let ticket = self.requests.lock().map(|mut requests| {
            let ticket = Ticket::new(self.clone());

            let queue = requests.entry(topition_producer_id.clone()).or_default();

            queue.push(BatchRequest {
                id: ticket.id,
                batch: deflated,
            });

            PRODUCE_REQUEST_QUEUED_COUNTER.add(1, &attributes);
            debug!(queue_len = queue.len());

            ticket
        })?;

        debug!(ticket = %ticket.id);

        let mut iteration = -1;

        loop {
            self.update_metrics()?;

            let ticket = ticket.clone();
            let id = ticket.id;

            iteration += 1;

            let queued_bytes = self
                .requests
                .lock()
                .map(|requests| match requests.get(&topition_producer_id) {
                    Some(queue) => queue
                        .iter()
                        .map(|batch_request| batch_request.batch.record_data.len())
                        .sum::<usize>(),
                    None => 0,
                })
                .inspect(|queued_bytes| debug!(queued_bytes))?;

            if self
                .minimum_size
                .inspect(|minimum_size| debug!(minimum_size, queued_bytes))
                .is_some_and(|minimum_size| queued_bytes > minimum_size)
            {
                PRODUCE_REQUEST_MINIMUM_SIZE_TRIGGER.add(1, &attributes);

                self.send_queued(&id, transaction_id, topition, producer_id)
                    .await?;
            }

            let patience = sleep(maximum_delay);

            tokio::select! {
                response = ticket  => {
                    let elapsed = start.elapsed().map_or(0, |duration| duration.as_millis() as u64);
                    debug!(ready = %id, elapsed, iteration);
                    PRODUCE_REQUEST_YOUR_TICKET_IS_READY.add(1, &attributes);
                    PRODUCE_DURATION.record(elapsed, &attributes);
                    self.update_metrics()?;
                    return response;
                }

                _ = patience => {
                    if iteration > 1 {
                        warn!(ticket = %id, iteration);
                    }

                    PRODUCE_REQUEST_TIMEOUT_EXPIRED_TRIGGER.add(1, &attributes);
                    self.send_queued(&id, transaction_id, topition, producer_id).await?;
                }
            }
        }
    }
}
