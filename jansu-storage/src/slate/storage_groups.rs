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

//! Group management Storage impl helpers: update_group

use std::sync::Arc;

use tracing::debug;
use uuid::Uuid;

use crate::{Error, GroupDetail, Result, UpdateError, Version};

use super::engine::Engine;
use super::types::{GroupDetailVersion, GroupKey};

impl Engine {
    pub(super) async fn impl_update_group(
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
            .or::<Error>(Ok(None))
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
}
