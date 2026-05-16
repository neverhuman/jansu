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

//! Group, cluster, maintain, SCRAM, and ping Storage impl helpers

use std::time::SystemTime;

use jansu_schema::lake::LakeHouse as _;
use jansu_sans_io::{
    ErrorCode, ScramMechanism,
    delete_groups_response::DeletableGroupResult,
    list_groups_response::ListedGroup,
};
use tracing::debug;

use crate::{Error, GroupDetail, NamedGroupDetail, Result, ScramCredential};

use super::engine::Engine;
use super::types::{
    GroupDetailVersion, GroupKey, GroupKeyPrefix, OffsetCommitKeyPrefix, OffsetCommitValue,
};

impl Engine {
    pub(super) async fn impl_list_groups(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        let _ = states_filter;
        let prefix = postcard::to_stdvec(&GroupKeyPrefix::new())?;
        let mut groups = vec![];

        let mut scan = self.db.scan(prefix.clone()..).await?;

        while let Some(kv) = scan.next().await? {
            if !kv.key.starts_with(&prefix) {
                break;
            }

            if let Ok(key) = postcard::from_bytes::<GroupKey>(&kv.key) {
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

    pub(super) async fn impl_delete_groups(
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

    pub(super) async fn impl_describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        if include_authorized_operations {
            tracing::warn!(
                "describe_groups authorized_operations is not implemented, returning empty"
            );
        }
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

    /// Maintenance callback for periodic cleanup operations.
    ///
    /// Runs lake maintenance if configured. This aligns with PG's maintain
    /// implementation.
    pub(super) async fn impl_maintain(&self, now: SystemTime) -> Result<()> {
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

    pub(super) async fn impl_cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    pub(super) async fn impl_node(&self) -> Result<i32> {
        Ok(self.node)
    }

    pub(super) async fn impl_advertised_listener(&self) -> Result<url::Url> {
        Ok(self.advertised_listener.clone())
    }

    pub(super) async fn impl_delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Ok(())
    }

    pub(super) async fn impl_upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Ok(())
    }

    pub(super) async fn impl_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Ok(None)
    }

    pub(super) async fn impl_ping(&self) -> Result<()> {
        // Verify connectivity by attempting a simple read operation
        let _ = self.db.get(Self::BROKERS).await?;
        Ok(())
    }
}
