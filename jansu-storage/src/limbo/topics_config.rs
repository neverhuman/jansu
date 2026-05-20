//! Topic configuration `Storage` operations for the Turso `Engine`.

use super::sql::sql_lookup;
use super::*;

impl Engine {
    pub(super) async fn incremental_alter_resource_impl(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        debug!(?resource);
        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Group => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::ClientMetric => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::BrokerLogger => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Broker => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Topic => {
                let mut error_code = ErrorCode::None;

                for config in resource
                    .configs
                    .map_or_else(Vec::new, std::convert::identity)
                {
                    let operation = OpType::try_from(config.config_operation)?;
                    match operation {
                        OpType::Set => {
                            let c = self.connection().await?;

                            if c.query(
                                &sql_lookup("topic_configuration_upsert.sql")?,
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                    config.value.as_deref(),
                                ),
                            )
                            .await
                            .inspect_err(|err| error!(?err))
                            .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Delete => {
                            let c = self.connection().await?;

                            if c.query(
                                &sql_lookup("topic_configuration_delete.sql")?,
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                ),
                            )
                            .await
                            .inspect_err(|err| error!(?err))
                            .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Append | OpType::Subtract => {
                            let c = self.connection().await?;
                            let mut statement = c
                                .prepare(&sql_lookup("topic_configuration_select.sql")?)
                                .await?;
                            let mut rows = statement
                                .query((self.cluster.as_str(), resource.resource_name.as_str()))
                                .await?;

                            let mut current = None;
                            while let Some(row) = rows.next().await? {
                                let name = row.get_value(0)?;
                                if name
                                    .as_text()
                                    .is_some_and(|name| name.as_str() == config.name)
                                {
                                    let value = row.get_value(1)?;
                                    current =
                                        value.as_text().map(|value| value.as_str().to_owned());
                                    break;
                                }
                            }

                            let updated = match operation {
                                OpType::Append => crate::append_config_tokens(
                                    current.as_deref(),
                                    config.value.as_deref(),
                                ),
                                OpType::Subtract => crate::subtract_config_tokens(
                                    current.as_deref(),
                                    config.value.as_deref(),
                                ),
                                OpType::Set | OpType::Delete => None,
                            };

                            let outcome = if let Some(updated) = updated {
                                let updated = Some(updated);
                                c.query(
                                    &sql_lookup("topic_configuration_upsert.sql")?,
                                    (
                                        self.cluster.as_str(),
                                        resource.resource_name.as_str(),
                                        config.name.as_str(),
                                        updated.as_deref(),
                                    ),
                                )
                                .await
                                .map(|_| ())
                            } else {
                                c.query(
                                    &sql_lookup("topic_configuration_delete.sql")?,
                                    (
                                        self.cluster.as_str(),
                                        resource.resource_name.as_str(),
                                        config.name.as_str(),
                                    ),
                                )
                                .await
                                .map(|_| ())
                            };

                            if outcome.inspect_err(|err| error!(?err)).is_err() {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                    }
                }

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(error_code.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    pub(super) async fn describe_config_impl(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut rows = c
            .query(
                &sql_lookup("topic_select.sql")?,
                (self.cluster.as_str(), name),
            )
            .await?;

        if rows.next().await?.is_some() {
            let mut rows = c
                .query(
                    &sql_lookup("topic_configuration_select.sql")?,
                    (self.cluster.as_str(), name),
                )
                .await?;

            let mut configs = vec![];

            while let Some(row) = rows.next().await? {
                let name = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_text()
                            .cloned()
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect_err(|err| error!(?err))?;

                let value = row
                    .get_value(1)
                    .map(|value| value.as_text().cloned())
                    .inspect_err(|err| error!(?err))?;

                configs.push(
                    DescribeConfigsResourceResult::default()
                        .name(name)
                        .value(value)
                        .read_only(false)
                        .is_default(None)
                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                        .is_sensitive(false)
                        .synonyms(Some([].into()))
                        .config_type(Some(ConfigType::String.into()))
                        .documentation(Some("".into())),
                );
            }

            let error_code = ErrorCode::None;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(configs)))
        } else {
            let error_code = ErrorCode::UnknownTopicOrPartition;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some([].into())))
        }
    }
}
