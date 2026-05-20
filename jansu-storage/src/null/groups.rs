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

//! Consumer-group operations for the `null://` [`Engine`].

use jansu_sans_io::{ErrorCode, delete_groups_response::DeletableGroupResult};
use uuid::Uuid;

use super::{Engine, Group};
use crate::{GroupDetail, GroupDetailResponse, NamedGroupDetail, Result, UpdateError, Version};

impl Engine {
    pub(super) fn null_delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        Ok(group_ids
            .unwrap_or(&[])
            .iter()
            .map(|group_id| {
                DeletableGroupResult::default()
                    .error_code(ErrorCode::None.into())
                    .group_id(group_id.to_owned())
            })
            .collect())
    }

    pub(super) fn null_describe_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<NamedGroupDetail>> {
        Ok(group_ids
            .unwrap_or(&[])
            .iter()
            .map(|name| NamedGroupDetail {
                name: name.to_owned(),
                response: GroupDetailResponse::ErrorCode(ErrorCode::GroupIdNotFound),
            })
            .collect())
    }

    pub(super) fn null_update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.groups
            .lock()
            .map_err(|err| UpdateError::Error(err.into()))
            .and_then(|mut groups| {
                let group = groups.entry(group_id.to_string()).or_insert(Group {
                    detail: detail.clone(),
                    version: version.clone(),
                });

                if group.version == version {
                    let id = Uuid::now_v7();
                    let version = Version {
                        e_tag: Some(id.to_string()),
                        version: Some(id.to_string()),
                    };

                    group.detail = detail;
                    group.version = Some(version.clone());

                    Ok(version)
                } else {
                    let known_version = group.version.clone().unwrap_or(Version {
                        e_tag: None,
                        version: None,
                    });
                    Err(UpdateError::Outdated {
                        current: Box::new(group.detail.clone()),
                        version: known_version,
                    })
                }
            })
    }
}
