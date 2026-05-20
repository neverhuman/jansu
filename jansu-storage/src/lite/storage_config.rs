//! `Storage` topic-configuration operations for the libSQL `Delegate`.

use super::*;

impl Delegate {
    pub(super) async fn incremental_alter_resource_inner(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let start = SystemTime::now();
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

                let configs = match resource.configs {
                    Some(configs) => configs,
                    None => Vec::new(),
                };
                for config in configs {
                    let operation = OpType::try_from(config.config_operation)?;
                    match operation {
                        OpType::Set => {
                            let c = self.connection().await?;

                            if c.query(
                                "topic_configuration_upsert.sql",
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
                                "topic_configuration_delete.sql",
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
                            let mut rows = c
                                .query(
                                    "topic_configuration_select.sql",
                                    (self.cluster.as_str(), resource.resource_name.as_str()),
                                )
                                .await?;
                            let mut current = None;
                            while let Some(row) = rows.next().await? {
                                if row.get::<String>(0)? == config.name {
                                    current = row.get::<Option<String>>(1)?;
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
                                    "topic_configuration_upsert.sql",
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
                                    "topic_configuration_delete.sql",
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
                .inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "incremental_alter_resource")],
                    )
                })
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "incremental_alter_resource")],
                )
            }),
        }
    }

    pub(super) async fn describe_config_inner(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await?;

        let mut rows = c
            .query("topic_select.sql", (self.cluster.as_str(), name))
            .await?;

        if rows.next().await?.is_some() {
            let mut rows = c
                .query(
                    "topic_configuration_select.sql",
                    (self.cluster.as_str(), name),
                )
                .await?;

            let mut configs = vec![];

            while let Some(row) = rows.next().await? {
                let name = row.get_str(0).inspect_err(|err| error!(?err))?;
                let value = row
                    .get::<Option<String>>(1)
                    .map(|value| match value {
                        Some(value) => value,
                        None => String::new(),
                    })
                    .map(Some)
                    .inspect_err(|err| error!(?err))?;

                configs.push(
                    DescribeConfigsResourceResult::default()
                        .name(name.to_owned())
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
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_config")],
                )
            })
        } else {
            let error_code = ErrorCode::UnknownTopicOrPartition;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some([].into())))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_config")],
                )
            })
        }
    }
}
