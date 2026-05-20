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

//! Consumer-group dispatch.
//!
//! Inherent `Postgres` methods backing the `Storage` trait implementation.

use super::*;

impl Postgres {
    pub(crate) async fn list_groups_dispatch(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        debug!(?states_filter);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut listed_groups = vec![];

        for row in self
            .prepare_query(&c, "consumer_group_select.sql", &[&self.cluster])
            .await
            .inspect_err(|err| error!(?err))?
        {
            let group_id = row.try_get::<_, String>(0)?;

            listed_groups.push(
                ListedGroup::default()
                    .group_id(group_id)
                    .protocol_type("consumer".into())
                    .group_state(Some("unknown".into()))
                    .group_type(Some("classic".into())),
            );
        }

        Ok(listed_groups)
    }

    pub(crate) async fn delete_groups_dispatch(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        debug!(?group_ids);

        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

            let consumer_offset = c
                .prepare_cached(self.sql_lookup("consumer_offset_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let group_detail = c
                .prepare_cached(self.sql_lookup("consumer_group_detail_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let group = c
                .prepare_cached(self.sql_lookup("consumer_group_delete.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            for group_id in group_ids {
                _ = c
                    .execute(&consumer_offset, &[&self.cluster, &group_id])
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(&group_detail, &[&self.cluster, &group_id])
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = c
                    .execute(&group, &[&self.cluster, &group_id])
                    .await
                    .inspect_err(|err| error!(?err))?;

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if rows == 0 {
                                ErrorCode::GroupIdNotFound
                            } else {
                                ErrorCode::None
                            }
                            .into(),
                        ),
                );
            }
        }

        Ok(results)
    }

    pub(crate) async fn describe_groups_dispatch(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        debug!(?group_ids, include_authorized_operations);

        let mut results = vec![];
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                if let Some(row) = self
                    .prepare_query_opt(
                        &c,
                        "consumer_group_select_by_name.sql",
                        &[&self.cluster, group_id],
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?
                {
                    let value = row
                        .try_get::<_, Value>(1)
                        .inspect_err(|err| error!(?err, group_id))?;

                    let current = serde_json::from_value::<GroupDetail>(value)
                        .inspect(|current| debug!(?current))?;

                    results.push(NamedGroupDetail::found(group_id.into(), current));
                } else {
                    results.push(NamedGroupDetail::error_code(
                        group_id.into(),
                        ErrorCode::GroupIdNotFound,
                    ));
                }
            }
        }

        Ok(results)
    }

    pub(crate) async fn update_group_dispatch(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        _ = self
            .tx_prepare_execute(
                &tx,
                "consumer_group_insert.sql",
                &[&self.cluster, &group_id],
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

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(?detail))?;

        let outcome = if let Some(row) = self
            .tx_prepare_query_opt(
                &tx,
                "consumer_group_detail_insert.sql",
                &[
                    &self.cluster,
                    &group_id,
                    &existing_e_tag,
                    &new_e_tag,
                    &detail,
                ],
            )
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| error!(?err))?
        {
            row.try_get::<_, Uuid>(2)
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
            let row = self
                .tx_prepare_query_one(
                    &tx,
                    "consumer_group_detail.sql",
                    &[&group_id, &self.cluster.as_str()],
                )
                .await
                .inspect(|row| debug!(?row))
                .inspect_err(|err| error!(?err))?;

            let version = row
                .try_get::<_, Uuid>(0)
                .inspect_err(|err| error!(?err))
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))?;

            let value = row.try_get::<_, Value>(1)?;
            let current = serde_json::from_value::<GroupDetail>(value)
                .inspect(|current| debug!(?current))
                .map(Box::new)?;

            Err(UpdateError::Outdated { current, version })
        };

        tx.commit().await.inspect_err(|err| error!(?err))?;

        debug!(?outcome);

        outcome
    }
}
