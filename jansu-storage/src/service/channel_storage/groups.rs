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

//! Consumer-group channel calls for [`RequestChannelService`].

use jansu_sans_io::{
    delete_groups_response::DeletableGroupResult, list_groups_response::ListedGroup,
};
use rama::{Context, Service};

use crate::service::{Request, RequestChannelService, Response};
use crate::{Error, GroupDetail, NamedGroupDetail, Result, UpdateError, Version};
use tracing::instrument;

impl RequestChannelService {
    #[instrument(name = "list_groups", skip_all)]
    pub(super) async fn channel_list_groups(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        let states_filter = states_filter.map(Vec::from);

        self.serve(Context::default(), Request::ListGroups(states_filter))
            .await
            .and_then(|response| {
                if let Response::ListGroups(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "delete_groups", skip_all)]
    pub(super) async fn channel_delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let group_ids = group_ids.map(Vec::from);

        self.serve(Context::default(), Request::DeleteGroups(group_ids))
            .await
            .and_then(|response| {
                if let Response::DeleteGroups(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "describe_groups", skip_all)]
    pub(super) async fn channel_describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let group_ids = group_ids.map(Vec::from);

        self.serve(
            Context::default(),
            Request::DescribeGroups {
                group_ids,
                include_authorized_operations,
            },
        )
        .await
        .and_then(|response| {
            if let Response::DescribeGroups(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "update_group", skip_all)]
    pub(super) async fn channel_update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let group_id = group_id.to_string();

        self.serve(
            Context::default(),
            Request::UpdateGroup {
                group_id,
                detail,
                version,
            },
        )
        .await
        .and_then(|response| {
            if let Response::UpdateGroup(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }
}
