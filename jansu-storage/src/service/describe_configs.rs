// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::collections::BTreeSet;

use jansu_sans_io::{
    ApiKey, ConfigResource, DescribeConfigsRequest, DescribeConfigsResponse, ErrorCode,
};
use rama::{Context, Service};
use tracing::{error, instrument};

use super::topic_config_defaults;
use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`DescribeConfigsRequest`] returning [`DescribeConfigsResponse`].
/// ```
/// use rama::{Context, Layer, Service as _, layer::MapStateLayer};
/// use jansu_sans_io::{ConfigResource, DescribeConfigsRequest,
///     EndpointType, ErrorCode, describe_configs_request::DescribeConfigsResource};
/// use jansu_storage::{DescribeConfigsService, Error, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("jansu")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://jansu/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(DescribeConfigsService);
///
/// let response = service
///     .serve(
///         Context::default(),
///         DescribeConfigsRequest::default()
///             .include_documentation(Some(false))
///             .include_synonyms(Some(false))
///             .resources(Some(
///                 [DescribeConfigsResource::default()
///                     .resource_name("abcba".into())
///                     .resource_type(ConfigResource::Topic.into())
///                     .configuration_keys(Some([].into()))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// let results = response.results.unwrap_or(Vec::new());
/// assert_eq!(1, results.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);
/// assert!(results[0].configs.as_deref().unwrap_or(&[]).is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeConfigsService;

impl ApiKey for DescribeConfigsService {
    const KEY: i16 = DescribeConfigsRequest::KEY;
}

impl<G> Service<G, DescribeConfigsRequest> for DescribeConfigsService
where
    G: Storage,
{
    type Response = DescribeConfigsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeConfigsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let include_synonyms = req.include_synonyms.unwrap_or(false);
        let mut results = vec![];

        for resource in req.resources.unwrap_or(Vec::new()) {
            let resource_type = ConfigResource::from(resource.resource_type);
            let mut result = ctx
                .state()
                .describe_config(
                    resource.resource_name.as_str(),
                    resource_type,
                    resource.configuration_keys.as_deref(),
                )
                .await
                .inspect_err(|err| error!(?err))?;

            // For topic resources, overlay storage results on top of
            // Kafka-standard defaults so every backend returns a complete
            // config set. Only do this for successful responses (topic exists).
            if resource_type == ConfigResource::Topic
                && matches!(ErrorCode::try_from(result.error_code), Ok(ErrorCode::None))
            {
                if let Some(configs) = result.configs.as_mut() {
                    // Build a fresh default map.
                    let mut merged = topic_config_defaults::build_default_configs();

                    // Overlay explicitly-stored configs from the storage
                    // backend, keeping the storage value and marking
                    // is_default = None.
                    for config in configs.drain(..) {
                        _ = merged.insert(config.name.clone(), config);
                    }

                    // Apply key filtering if the request asked for
                    // specific keys.
                    if let Some(keys) = resource
                        .configuration_keys
                        .as_ref()
                        .filter(|keys| !keys.is_empty())
                    {
                        let requested: BTreeSet<_> =
                            keys.iter().map(|key| key.as_str()).collect();
                        merged.retain(|name, _| requested.contains(name.as_str()));
                    }

                    // Apply synonym expansion.
                    if include_synonyms {
                        for config in merged.values_mut() {
                            if let Some(synonyms) = topic_config_defaults::synonyms_for(
                                config.name.as_str(),
                                config.value.as_deref(),
                            ) {
                                *config = config.clone().synonyms(Some(synonyms));
                            }
                        }
                    }

                    *configs = merged.into_values().collect();
                }
            }

            results.push(result);
        }

        Ok(DescribeConfigsResponse::default().results(Some(results)))
    }
}
