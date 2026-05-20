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

//! Consumer-offset channel calls for [`RequestChannelService`].

use std::{collections::BTreeMap, time::Duration};

use jansu_sans_io::ErrorCode;
use rama::{Context, Service};

use crate::service::{Request, RequestChannelService, Response};
use crate::{Error, OffsetCommitRequest, OffsetFetchRecord, Result, Topition};
use tracing::instrument;

impl RequestChannelService {
    #[instrument(name = "offset_commit", skip_all)]
    pub(super) async fn channel_offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let group_id = group_id.to_string();
        let offsets = Vec::from(offsets);

        self.serve(
            Context::default(),
            Request::OffsetCommit {
                group_id,
                retention_time_ms,
                offsets,
            },
        )
        .await
        .and_then(|response| {
            if let Response::OffsetCommit(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "committed_offset_topitions", skip_all)]
    pub(super) async fn channel_committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        let group_id = group_id.to_string();

        self.serve(
            Context::default(),
            Request::CommittedOffsetTopitions(group_id),
        )
        .await
        .and_then(|response| {
            if let Response::CommittedOffsetTopitions(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "offset_fetch_records", skip_all)]
    pub(super) async fn channel_offset_fetch_records(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, OffsetFetchRecord>> {
        let group_id = group_id.map(|s| s.to_string());
        let topics = Vec::from(topics);

        self.serve(
            Context::default(),
            Request::OffsetFetch {
                group_id,
                topics,
                require_stable,
            },
        )
        .await
        .and_then(|response| {
            if let Response::OffsetFetch(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }
}
