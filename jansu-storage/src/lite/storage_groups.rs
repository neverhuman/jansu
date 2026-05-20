//! `Storage` consumer-group operations for the libSQL `Delegate`.

use super::*;

impl Delegate {
    pub(super) async fn list_groups_inner(
        &self,
        states_filter: Option<&[String]>,
    ) -> Result<Vec<ListedGroup>> {
        let start = SystemTime::now();

        debug!(?states_filter);

        let c = self.connection().await?;

        let mut listed_groups = vec![];

        let mut rows = c
            .query("consumer_group_select.sql", &[self.cluster.as_str()])
            .await?;

        while let Some(row) = rows.next().await? {
            let group_id = row.get_str(0)?;

            listed_groups.push(
                ListedGroup::default()
                    .group_id(group_id.to_owned())
                    .protocol_type("consumer".into())
                    .group_state(Some("unknown".into()))
                    .group_type(Some("classic".into())),
            );
        }

        Ok(listed_groups).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "list_groups")],
            )
        })
    }

    pub(super) async fn delete_groups_inner(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let start = SystemTime::now();

        debug!(?group_ids);

        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

            for group_id in group_ids {
                _ = c
                    .execute(
                        "consumer_offset_delete_by_cg.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(
                        "consumer_group_detail_delete_by_cg.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = c
                    .execute(
                        "consumer_group_delete.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
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

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_groups")],
            )
        })
    }

    pub(super) async fn describe_groups_inner(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();

        debug!(?group_ids, include_authorized_operations);

        let mut results = vec![];
        let c = self.connection().await?;

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                if let Some(row) = c
                    .query_opt(
                        "consumer_group_select_by_name.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?
                {
                    let current = row
                        .get_str(1)
                        .map_err(Error::from)
                        .and_then(|s| serde_json::from_str::<GroupDetail>(s).map_err(Into::into))
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

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "describe_groups")],
            )
        })
    }

    pub(super) async fn update_group_inner(
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
