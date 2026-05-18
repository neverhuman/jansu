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

        let mut pc = self.connection().await?;
        pc.begin(BeginMode::Immediate).map_err(Error::from)?;

        _ = pc
            .execute(
                &sql("consumer_group_insert.sql").map_err(Error::from)?,
                (self.cluster.as_str(), group_id),
            )
            .map_err(Error::from)?;

        let existing_e_tag = version
            .as_ref()
            .map_or(Ok(Uuid::from_u128(0)), |version| {
                version
                    .e_tag
                    .as_ref()
                    .map_or(Err(UpdateError::MissingEtag::<GroupDetail>), |e_tag| {
                        Uuid::from_str(e_tag.as_str()).map_err(UpdateError::Uuid)
                    })
            })
            .inspect_err(|err| error!(?err))
            .inspect(|existing_e_tag| debug!(?existing_e_tag))?;

        let new_e_tag = default_hash(&detail);
        debug!(?new_e_tag);

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(%detail))?;

        let outcome_e_tag = {
            let s = sql("consumer_group_detail_insert.sql").map_err(Error::from)?;
            let mut rows = pc
                .query(
                    &s,
                    (
                        self.cluster.as_str(),
                        group_id,
                        existing_e_tag.to_string().as_str(),
                        new_e_tag.to_string().as_str(),
                        detail.to_string().as_str(),
                    ),
                )
                .map_err(Error::from)
                .inspect_err(|err| error!(?err))?;
            match rows.step().map_err(Error::from)? {
                Step::Row(row) => Some(row.get::<String>(2).map_err(Error::from)?),
                Step::Done => None,
            }
        };

        let outcome = if let Some(e_tag_str) = outcome_e_tag {
            Uuid::parse_str(e_tag_str.as_str())
                .map_err(UpdateError::Uuid)
                .inspect_err(|err| error!(?err))
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))
        } else {
            let s = sql("consumer_group_detail.sql").map_err(Error::from)?;
            let mut rows = pc
                .query(&s, (group_id, self.cluster.as_str()))
                .map_err(Error::from)
                .inspect_err(|err| error!(?err))?;

            let Step::Row(row) = rows.step().map_err(Error::from)? else {
                return Err(UpdateError::MissingEtag);
            };

            let version_str = row
                .get::<String>(0)
                .map_err(Error::from)
                .inspect_err(|err| error!(?err))?;
            let value_str = row
                .get::<String>(1)
                .map_err(Error::from)
                .inspect(|value| debug!(%value))
                .inspect_err(|err| error!(?err))?;
            drop(rows);

            let version = Uuid::parse_str(version_str.as_str())
                .map_err(Error::from)
                .inspect_err(|err: &Error| error!(?err))
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))?;

            let value: serde_json::Value = serde_json::from_str(value_str.as_str())
                .map_err(Error::from)
                .inspect(|value| debug!(%value))?;

            let current = serde_json::from_value::<GroupDetail>(value)
                .inspect(|current| debug!(?current))
                .inspect_err(|err| error!(?err))
                .map(Box::new)?;

            Err(UpdateError::Outdated { current, version })
        };

        let _ = pc.commit().map_err(Error::from)?;

        debug!(?outcome);

        outcome.inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "update_group")],
            )
        })
    }
}
