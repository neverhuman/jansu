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

//! Producer / transaction channel calls for [`RequestChannelService`].

use jansu_sans_io::{ErrorCode, txn_offset_commit_response::TxnOffsetCommitResponseTopic};
use rama::{Context, Service};

use tracing::instrument;

use crate::service::{Request, RequestChannelService, Response};
use crate::{
    Error, ProducerIdResponse, Result, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest,
};

impl RequestChannelService {
    #[instrument(name = "init_producer", skip_all)]
    pub(super) async fn channel_init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let transaction_id = transaction_id.map(|transaction_id| transaction_id.to_owned());

        self.serve(
            Context::default(),
            Request::InitProducer {
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            },
        )
        .await
        .and_then(|response| {
            if let Response::InitProducer(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "txn_add_offsets", skip_all)]
    pub(super) async fn channel_txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let transaction_id = transaction_id.to_string();
        let group_id = group_id.to_string();

        self.serve(
            Context::default(),
            Request::TxnAddOffsets {
                transaction_id,
                producer_id,
                producer_epoch,
                group_id,
            },
        )
        .await
        .and_then(|response| {
            if let Response::TxnAddOffsets(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "txn_add_partitions", skip_all)]
    pub(super) async fn channel_txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.serve(Context::default(), Request::TxnAddPartitions(partitions))
            .await
            .and_then(|response| {
                if let Response::TxnAddPartitions(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "txn_offset_commit", skip_all)]
    pub(super) async fn channel_txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.serve(Context::default(), Request::TxnOffsetCommit(offsets))
            .await
            .and_then(|response| {
                if let Response::TxnOffsetCommit(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "txn_end", skip_all)]
    pub(super) async fn channel_txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let transaction_id = transaction_id.to_string();

        self.serve(
            Context::default(),
            Request::TxnEnd {
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
            },
        )
        .await
        .and_then(|response| {
            if let Response::TxnEnd(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }
}
