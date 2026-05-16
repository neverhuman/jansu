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

pub(super) async fn list_groups(
    this: &Engine,
    states_filter: Option<&[String]>,
) -> Result<Vec<ListedGroup>> {
    debug!(?states_filter);
    let c = this.connection().await.inspect_err(|err| error!(?err))?;

    let mut listed_groups = vec![];

    let mut rows = c
        .query(
            &sql_lookup("consumer_group_select.sql")?,
            &[this.cluster.as_str()],
        )
        .await?;

    while let Some(row) = rows.next().await? {
        let group_id = row.get_value(0).map_err(Into::into).and_then(|value| {
            value
                .as_text()
                .cloned()
                .ok_or(Error::UnexpectedValue(value.clone()))
        })?;

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

pub(super) async fn delete_groups(
    this: &Engine,
    group_ids: Option<&[String]>,
) -> Result<Vec<DeletableGroupResult>> {
    debug!(?group_ids);
    let mut results = vec![];

    if let Some(group_ids) = group_ids {
        let c = this.connection().await?;

        let mut consumer_offset = c
            .prepare(&sql_lookup("consumer_offset_delete_by_cg.sql")?)
            .await
            .inspect_err(|err| error!(?err))?;

        let mut group_detail = c
            .prepare(&sql_lookup("consumer_group_detail_delete_by_cg.sql")?)
            .await
            .inspect_err(|err| error!(?err))?;

        let mut group = c
            .prepare(&sql_lookup("consumer_group_delete.sql")?)
            .await
            .inspect_err(|err| error!(?err))?;

        for group_id in group_ids {
            _ = consumer_offset
                .execute((this.cluster.as_str(), group_id.as_str()))
                .await
                .inspect_err(|err| error!(?err))?;

            _ = group_detail
                .execute((this.cluster.as_str(), group_id.as_str()))
                .await
                .inspect_err(|err| error!(?err))?;

            let rows = group
                .execute((this.cluster.as_str(), group_id.as_str()))
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

pub(super) async fn describe_groups(
    this: &Engine,
    group_ids: Option<&[String]>,
    include_authorized_operations: bool,
) -> Result<Vec<NamedGroupDetail>> {
    debug!(?group_ids, include_authorized_operations);

    let mut results = vec![];
    let c = this.connection().await.inspect_err(|err| error!(?err))?;

    if let Some(group_ids) = group_ids {
        for group_id in group_ids {
            if let Some(row) = this
                .prepare_query_opt(
                    &c,
                    &sql_lookup("consumer_group_select_by_name.sql")?,
                    (this.cluster.as_str(), group_id.as_str()),
                )
                .await
                .inspect_err(|err| error!(?err, group_id))?
            {
                let current = row
                    .get_value(1)
                    .map_err(Error::from)
                    .and_then(|value| {
                        value
                            .as_text()
                            .cloned()
                            .ok_or(Error::UnexpectedValue(value.clone()))
                    })
                    .and_then(|s| serde_json::from_str::<GroupDetail>(&s).map_err(Into::into))
                    .inspect(|current| debug!(?current))
                    .inspect_err(|err| error!(?err, group_id))?;

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

pub(super) async fn update_group(
    this: &Engine,
    group_id: &str,
    detail: GroupDetail,
    version: Option<Version>,
) -> Result<Version, UpdateError<GroupDetail>> {
    debug!(cluster = this.cluster, group_id, ?detail, ?version);
    debug!(cluster = this.cluster, group_id, ?detail, ?version);

    let mut c = this.connection().await?;
    let tx = c.transaction().await?;

    _ = this
        .prepare_execute(
            &tx,
            &sql_lookup("consumer_group_insert.sql")?,
            (this.cluster.as_str(), group_id),
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

    let outcome = if let Some(row) = this
        .prepare_query_opt(
            &tx,
            &sql_lookup("consumer_group_detail_insert.sql")?,
            (
                this.cluster.as_str(),
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
        row.get_value(2)
            .map_err(Error::from)
            .and_then(|value| {
                value
                    .as_text()
                    .ok_or(Error::UnexpectedValue(value.clone()))
                    .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
            })
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
        let row = this
            .prepare_query_one(
                &tx,
                &sql_lookup("consumer_group_detail.sql")?,
                (group_id, this.cluster.as_str()),
            )
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| error!(?err))?;

        let version = row
            .get_value(0)
            .map_err(Error::from)
            .and_then(|value| {
                value
                    .as_text()
                    .ok_or(Error::UnexpectedValue(value.clone()))
                    .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
            })
            .inspect_err(|err| error!(?err))
            .map(|uuid| uuid.to_string())
            .map(Some)
            .map(|e_tag| Version {
                e_tag,
                version: None,
            })
            .inspect(|version| debug!(?version))?;

        let current = row
            .get_value(1)
            .map_err(Error::from)
            .and_then(|value| {
                value
                    .as_text()
                    .map(|v| v.as_str())
                    .ok_or(Error::UnexpectedValue(value.clone()))
                    .and_then(|s| serde_json::from_str::<GroupDetail>(s).map_err(Into::into))
            })
            .inspect(|current| debug!(?current))
            .map(Box::new)?;

        Err(UpdateError::Outdated { current, version })
    };

    tx.commit().await.inspect_err(|err| error!(?err))?;

    debug!(?outcome);

    outcome
}
