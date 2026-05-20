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

//! Consumer group lifecycle and maintenance operations for the SlateDB engine.

use std::sync::Arc;
use std::time::SystemTime;

use jansu_sans_io::{
    ErrorCode, delete_groups_response::DeletableGroupResult, list_groups_response::ListedGroup,
};
use tracing::debug;
use uuid::Uuid;

use crate::{Error, GroupDetail, NamedGroupDetail, Result, UpdateError, Version};

use super::super::engine::Engine;
use super::super::types::{
    GroupDetailVersion, GroupKey, GroupKeyPrefix, OffsetCommitKeyPrefix, OffsetCommitValue,
};

impl Engine {
    pub(in crate::slate) async fn list_groups_op(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        let include_unknown = states_filter.is_none_or(|states| {
            states
                .iter()
                .any(|state| state.eq_ignore_ascii_case("unknown"))
        });
        let prefix = postcard::to_stdvec(&GroupKeyPrefix::new())?;
        let mut groups = vec![];

        let mut scan = self.db.scan(prefix.clone()..).await?;

        while let Some(kv) = scan.next().await? {
            if !kv.key.starts_with(&prefix) {
                break;
            }

            if include_unknown && let Ok(key) = postcard::from_bytes::<GroupKey>(&kv.key) {
                groups.push(
                    ListedGroup::default()
                        .group_id(key.group_id)
                        .protocol_type("consumer".into())
                        .group_state(Some("Unknown".into()))
                        .group_type(Some("classic".into())),
                );
            }
        }

        Ok(groups)
    }

    pub(in crate::slate) async fn delete_groups_op(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            for group_id in group_ids {
                // Delete group state
                let group_key = postcard::to_stdvec(&GroupKey::new(group_id))?;
                let had_group = tx.get(&group_key).await?.is_some();

                if had_group {
                    tx.delete(&group_key)?;
                }

                // Delete committed offsets for this group
                let offset_prefix = postcard::to_stdvec(&OffsetCommitKeyPrefix::new(group_id))?;
                let mut deleted_offsets = false;

                // Note: SlateDB doesn't support range deletes directly,
                // so we scan and delete individually
                let mut scan = self.db.scan(offset_prefix.clone()..).await?;
                while let Some(kv) = scan.next().await? {
                    if !kv.key.starts_with(&offset_prefix) {
                        break;
                    }
                    tx.delete(&kv.key)?;
                    deleted_offsets = true;
                }

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if had_group || deleted_offsets {
                                ErrorCode::None
                            } else {
                                ErrorCode::GroupIdNotFound
                            }
                            .into(),
                        ),
                );
            }

            tx.commit().await.map_err(Error::from)?;
        }

        Ok(results)
    }

    pub(in crate::slate) async fn describe_groups_op(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let key = postcard::to_stdvec(&GroupKey::new(group_id))?;

                match self.db.get(&key).await {
                    Ok(Some(encoded)) => {
                        match postcard::from_bytes::<GroupDetailVersion>(&encoded) {
                            Ok(gdv) => {
                                results.push(NamedGroupDetail::found(group_id.into(), gdv.detail));
                            }
                            Err(_) => {
                                results.push(NamedGroupDetail::found(
                                    group_id.into(),
                                    GroupDetail::default(),
                                ));
                            }
                        }
                    }
                    Ok(None) => {
                        results.push(NamedGroupDetail::found(
                            group_id.into(),
                            GroupDetail::default(),
                        ));
                    }
                    Err(_) => {
                        results.push(NamedGroupDetail::error_code(
                            group_id.into(),
                            ErrorCode::UnknownServerError,
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    pub(in crate::slate) async fn update_group_op(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(|err| UpdateError::Error(Error::Slate(Arc::new(err))))?;

        let key = postcard::to_stdvec(&GroupKey::new(group_id))
            .map_err(|err| UpdateError::Error(Error::Postcard(err)))?;

        // Try to load existing group
        let current_group: Option<GroupDetailVersion> = self
            .load_metadata(&tx, &key)
            .await
            .map(Some)
            .or_else(|_| Ok::<_, Error>(None))
            .map_err(UpdateError::Error)?;

        if let Some(current) = current_group {
            // Check version if provided
            if version.is_some_and(|v| v != current.version) {
                tx.rollback();
                return Err(UpdateError::Outdated {
                    current: Box::new(current.detail),
                    version: current.version,
                });
            }
        }

        let updated_version = Version::from(&Uuid::now_v7());
        let new_group = GroupDetailVersion::default()
            .detail(detail)
            .version(updated_version.clone());

        self.save_metadata(&tx, &key, &new_group)
            .map_err(UpdateError::Error)?;

        tx.commit()
            .await
            .map_err(|err| UpdateError::Error(Error::Slate(Arc::new(err))))
            .and(Ok(updated_version))
    }

    pub(in crate::slate) async fn maintain_op(&self, now: SystemTime) -> Result<()> {
        use jansu_schema::lake::LakeHouse as _;

        if let Some(ref lake) = self.lake {
            return lake.maintain().await.map_err(Into::into);
        }

        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let group_prefix = postcard::to_stdvec(&GroupKeyPrefix::new())?;
        let mut groups = self.db.scan(group_prefix.clone()..).await?;
        let mut expired = 0usize;

        while let Some(kv) = groups.next().await? {
            if !kv.key.starts_with(&group_prefix) {
                break;
            }

            let Ok(group_key) = postcard::from_bytes::<GroupKey>(&kv.key) else {
                continue;
            };

            let offset_prefix =
                postcard::to_stdvec(&OffsetCommitKeyPrefix::new(group_key.group_id))?;
            let mut offsets = self.db.scan(offset_prefix.clone()..).await?;

            while let Some(offset_kv) = offsets.next().await? {
                if !offset_kv.key.starts_with(&offset_prefix) {
                    break;
                }

                let Ok(value) = postcard::from_bytes::<OffsetCommitValue>(&offset_kv.value) else {
                    continue;
                };

                if value.expires_at.is_some_and(|expires_at| now >= expires_at) {
                    tx.delete(&offset_kv.key)?;
                    expired += 1;
                }
            }
        }

        tx.commit().await.map_err(Error::from)?;
        debug!(expired);

        Ok(())
    }
}
