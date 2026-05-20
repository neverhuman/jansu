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

//! Record produce / fetch channel calls for [`RequestChannelService`].

use jansu_sans_io::{IsolationLevel, ListOffset, record::deflated};
use rama::{Context, Service};

use crate::service::{Request, RequestChannelService, Response};
use crate::{AbortedTransactionRange, Error, ListOffsetResponse, OffsetStage, Result, Topition};
use tracing::instrument;

impl RequestChannelService {
    #[instrument(name = "produce", skip_all)]
    pub(super) async fn channel_produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let transaction_id = transaction_id.map(|s| s.to_string());
        let topition = topition.to_owned();

        self.serve(
            Context::default(),
            Request::Produce {
                transaction_id,
                topition,
                batch,
            },
        )
        .await
        .and_then(|response| {
            if let Response::Produce(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "fetch", skip_all)]
    pub(super) async fn channel_fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let topition = topition.to_owned();

        self.serve(
            Context::default(),
            Request::Fetch {
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation,
            },
        )
        .await
        .and_then(|response| {
            if let Response::Fetch(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "aborted_transaction_ranges", skip_all)]
    pub(super) async fn channel_aborted_transaction_ranges(
        &self,
        topition: &Topition,
    ) -> Result<Vec<AbortedTransactionRange>> {
        let topition = topition.to_owned();

        self.serve(
            Context::default(),
            Request::AbortedTransactionRanges(topition),
        )
        .await
        .and_then(|response| {
            if let Response::AbortedTransactionRanges(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "offset_stage", skip_all)]
    pub(super) async fn channel_offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.serve(
            Context::default(),
            Request::OffsetStage(topition.to_owned()),
        )
        .await
        .and_then(|response| {
            if let Response::OffsetStage(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "list_offsets", skip_all)]
    pub(super) async fn channel_list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let offsets = Vec::from(offsets);

        self.serve(
            Context::default(),
            Request::ListOffsets {
                isolation_level,
                offsets,
            },
        )
        .await
        .and_then(|response| {
            if let Response::ListOffsets(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "offset_for_leader_epoch", skip_all)]
    pub(super) async fn channel_offset_for_leader_epoch(
        &self,
        topition: &Topition,
        leader_epoch: i32,
    ) -> Result<Option<(i32, i64)>> {
        self.serve(
            Context::default(),
            Request::OffsetForLeaderEpoch {
                topition: topition.to_owned(),
                leader_epoch,
            },
        )
        .await
        .and_then(|response| {
            if let Response::OffsetForLeaderEpoch(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }
}
