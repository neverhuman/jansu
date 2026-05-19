use super::sql::sql_lookup;
use super::*;

impl Engine {
    pub(super) async fn list_groups_impl(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        debug!(?states_filter);
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut listed_groups = vec![];

        let mut rows = c
            .query(
                &sql_lookup("consumer_group_select.sql")?,
                &[self.cluster.as_str()],
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

    pub(super) async fn delete_groups_impl(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        debug!(?group_ids);
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

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
                    .execute((self.cluster.as_str(), group_id.as_str()))
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = group_detail
                    .execute((self.cluster.as_str(), group_id.as_str()))
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = group
                    .execute((self.cluster.as_str(), group_id.as_str()))
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

    pub(super) async fn describe_groups_impl(
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
                        &sql_lookup("consumer_group_select_by_name.sql")?,
                        (self.cluster.as_str(), group_id.as_str()),
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

    pub(super) async fn update_group_impl(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        debug!(cluster = self.cluster, group_id, ?detail, ?version);
        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("consumer_group_insert.sql")?,
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

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(?detail))?;

        let outcome = if let Some(row) = self
            .prepare_query_opt(
                &tx,
                &sql_lookup("consumer_group_detail_insert.sql")?,
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
            let row = self
                .prepare_query_one(
                    &tx,
                    &sql_lookup("consumer_group_detail.sql")?,
                    (group_id, self.cluster.as_str()),
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
}
