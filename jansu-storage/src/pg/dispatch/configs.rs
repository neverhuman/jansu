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

//! Config alter/describe dispatch.
//!
//! Inherent `Postgres` methods backing the `Storage` trait implementation.

use super::*;

impl Postgres {
    pub(crate) async fn incremental_alter_resource_dispatch(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
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

                let configs = resource
                    .configs
                    .map_or_else(Vec::new, std::convert::identity);
                for config in configs {
                    let operation = OpType::try_from(config.config_operation)?;
                    match operation {
                        OpType::Set => {
                            let c = self.connection().await?;

                            if self
                                .prepare_query(
                                    &c,
                                    "topic_configuration_upsert.sql",
                                    &[
                                        &self.cluster,
                                        &resource.resource_name,
                                        &config.name,
                                        &config.value,
                                    ],
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

                            if self
                                .prepare_query(
                                    &c,
                                    "topic_configuration_delete.sql",
                                    &[&self.cluster, &resource.resource_name, &config.name],
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
                            let rows = self
                                .prepare_query(
                                    &c,
                                    "topic_configuration_select.sql",
                                    &[&self.cluster, &resource.resource_name],
                                )
                                .await?;
                            let current = rows.iter().find_map(|row| {
                                row.try_get::<_, String>(0).ok().and_then(|name| {
                                    if name == config.name {
                                        row.try_get::<_, Option<String>>(1).ok().flatten()
                                    } else {
                                        None
                                    }
                                })
                            });

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
                                self.prepare_query(
                                    &c,
                                    "topic_configuration_upsert.sql",
                                    &[
                                        &self.cluster,
                                        &resource.resource_name,
                                        &config.name,
                                        &updated,
                                    ],
                                )
                                .await
                                .map(|_| ())
                            } else {
                                self.prepare_query(
                                    &c,
                                    "topic_configuration_delete.sql",
                                    &[&self.cluster, &resource.resource_name, &config.name],
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

    pub(crate) async fn describe_config_dispatch(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let prepared = c
            .prepare_cached(self.sql_lookup("topic_select.sql")?)
            .await
            .inspect_err(|err| error!(?err))?;

        if c.query_opt(&prepared, &[&self.cluster.as_str(), &name])
            .await
            .inspect_err(|err| error!(?err))?
            .is_some()
        {
            let prepared = c
                .prepare_cached(self.sql_lookup("topic_configuration_select.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let rows = c
                .query(&prepared, &[&self.cluster.as_str(), &name])
                .await
                .inspect_err(|err| error!(?err))?;

            let mut configs = vec![];

            for row in rows {
                let name = row
                    .try_get::<_, String>(0)
                    .inspect_err(|err| error!(?err))?;
                let value = row
                    .try_get::<_, Option<String>>(1)
                    .map(|value| value.unwrap_or_else(String::new))
                    .map(Some)
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
