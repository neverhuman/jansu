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

use super::*;

impl Delegate {
    pub(super) async fn delegate_update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        _ = pc
            .execute(
                "consumer_group_insert.sql",
                (self.cluster.as_str(), group_id),
            )
            .await?;

        let existing_e_tag = version
            .as_ref()
            .map_or(Ok(Uuid::from_u128(0)), |version| {
                version
                    .e_tag
                    .as_ref()
                    .map_or(Err(UpdateError::MissingEtag::<GroupDetail>), |e_tag| {
                        Uuid::from_str(e_tag.as_str()).map_err(Into::into)
                    })
            })
            .inspect_err(|err| error!(?err))
            .inspect(|existing_e_tag| debug!(?existing_e_tag))?;

        let new_e_tag = default_hash(&detail);
        debug!(?new_e_tag);

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(%detail))?;

        let outcome = if let Some(row) = pc
            .query_opt(
                "consumer_group_detail_insert.sql",
                (
                    self.cluster.as_str(),
                    group_id,
                    existing_e_tag.to_string().as_str(),
                    new_e_tag.to_string().as_str(),
                    detail.to_string().as_str(),
                ),
            )
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| error!(?err))?
        {
            row.get_str(2)
                .map_err(Error::from)
                .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                .inspect_err(|err| error!(?err))
                .map_err(Into::into)
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))
        } else {
            let row = pc
                .query_one(
                    "consumer_group_detail.sql",
                    (group_id, self.cluster.as_str()),
                )
                .await
                .inspect(|row| debug!(?row))
                .inspect_err(|err| error!(?err))?;

            let version = row
                .get_str(0)
                .map_err(Error::from)
                .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                .inspect_err(|err| error!(?err))
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))?;

            let value = row
                .get_str(1)
                .map_err(Error::from)
                .inspect(|value| debug!(%value))
                .and_then(|value| serde_json::from_str(value).map_err(Into::into))
                .inspect(|value| debug!(%value))?;

            let current = serde_json::from_value::<GroupDetail>(value)
                .inspect(|current| debug!(?current))
                .inspect_err(|err| error!(?err))
                .map(Box::new)?;

            Err(UpdateError::Outdated { current, version })
        };

        pc.commit(tx).await?;

        debug!(?outcome);

        outcome.inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "update_group")],
            )
        })
    }

}
