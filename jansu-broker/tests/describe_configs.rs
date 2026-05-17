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

use common::{alphanumeric_string, register_broker};
use jansu_broker::Result;
use jansu_sans_io::{
    ConfigResource, ConfigSource, DescribeConfigsRequest, ErrorCode,
    IncrementalAlterConfigsRequest, OpType,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    describe_configs_request::DescribeConfigsResource,
    describe_configs_response::DescribeConfigsResourceResult,
    describe_configs_response::DescribeConfigsSynonym,
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
};
use jansu_storage::{DescribeConfigsService, IncrementalAlterConfigsService, Storage};
use rama::{Context, Service};
use rand::{prelude::*, rng};
use tracing::debug;
use uuid::Uuid;
pub mod common;

/// Helper: assert that a config entry with the given name exists in the
/// config list and return its value.
fn find_config<'a>(
    configs: &'a [DescribeConfigsResourceResult],
    name: &str,
) -> &'a DescribeConfigsResourceResult {
    configs
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("expected config {name}"))
}

/// The full default config set has 22 entries matching our centralized
/// topic_config_defaults module.
const EXPECTED_DEFAULT_CONFIG_COUNT: usize = 22;

pub async fn single_topic<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let cleanup_policy = "cleanup.policy";
    let retention_ms = "retention.ms";
    let compact = "compact";

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some(
        [CreatableTopicConfig::default()
            .name(cleanup_policy.into())
            .value(Some(compact.into()))]
        .into(),
    );

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;

    let resources = [DescribeConfigsResource::default()
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic_name.clone())
        .configuration_keys(None)];

    let include_synonyms = Some(false);
    let include_documentation = Some(false);

    let ctx = Context::with_state(sc);

    // --- Full describe (no key filter, no synonyms) ---
    let results = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.clone().into())),
        )
        .await?;

    let result_list = results.results.unwrap_or_default();
    assert_eq!(1, result_list.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(result_list[0].error_code)?
    );
    let full_configs = result_list[0].configs.as_deref().unwrap_or_default();
    assert_eq!(
        EXPECTED_DEFAULT_CONFIG_COUNT,
        full_configs.len(),
        "expected {} default configs, got {}",
        EXPECTED_DEFAULT_CONFIG_COUNT,
        full_configs.len()
    );

    // The explicitly-set cleanup.policy should have is_default = None and
    // value = "compact".
    let cp = find_config(full_configs, cleanup_policy);
    assert_eq!(cp.value.as_deref(), Some(compact));
    assert_eq!(cp.is_default, None);

    // retention.ms should have is_default = Some(true) and the default value.
    let rm = find_config(full_configs, retention_ms);
    assert_eq!(rm.value.as_deref(), Some("604800000"));
    assert_eq!(rm.is_default, Some(true));

    // Verify some other defaults exist
    let sb = find_config(full_configs, "segment.bytes");
    assert_eq!(sb.value.as_deref(), Some("1073741824"));
    assert_eq!(sb.is_default, Some(true));

    let mir = find_config(full_configs, "min.insync.replicas");
    assert_eq!(mir.value.as_deref(), Some("1"));
    assert_eq!(mir.is_default, Some(true));

    // --- Key-filtered describe ---
    let filtered = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_name(topic_name.clone())
                        .resource_type(ConfigResource::Topic.into())
                        .configuration_keys(Some([cleanup_policy.into()].into()))]
                    .into(),
                )),
        )
        .await?;

    let filtered_results = filtered.results.unwrap_or_default();
    assert_eq!(1, filtered_results.len());
    let filtered_configs = filtered_results[0].configs.as_deref().unwrap_or_default();
    assert_eq!(1, filtered_configs.len());
    assert_eq!(filtered_configs[0].name, cleanup_policy);
    assert_eq!(filtered_configs[0].value.as_deref(), Some(compact));

    // --- Describe with include_synonyms=true ---
    let with_synonyms = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(Some(true))
                .resources(Some(resources.clone().into())),
        )
        .await?;

    let results = with_synonyms.results.unwrap_or_default();
    assert_eq!(1, results.len());
    let configs = results[0].configs.as_deref().unwrap_or_default();

    // cleanup.policy synonym
    let cleanup = find_config(configs, cleanup_policy);
    assert_eq!(
        Some(
            [DescribeConfigsSynonym::default()
                .name("log.cleanup.policy".into())
                .value(Some(compact.into()))
                .source(ConfigSource::DefaultConfig.into())]
            .into()
        ),
        cleanup.synonyms
    );

    // retention.ms synonym
    let retention = find_config(configs, retention_ms);
    assert_eq!(
        Some(
            [DescribeConfigsSynonym::default()
                .name("log.retention.ms".into())
                .value(Some("604800000".into()))
                .source(ConfigSource::DefaultConfig.into())]
            .into()
        ),
        retention.synonyms
    );

    // segment.bytes synonym
    let segment = find_config(configs, "segment.bytes");
    assert_eq!(
        Some(
            [DescribeConfigsSynonym::default()
                .name("log.segment.bytes".into())
                .value(Some("1073741824".into()))
                .source(ConfigSource::DefaultConfig.into())]
            .into()
        ),
        segment.synonyms
    );

    debug!(?topic_id);
    Ok(())
}

pub async fn alter_single_topic<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let cleanup_policy = "cleanup.policy";
    let retention_ms = "retention.ms";
    let compact = "compact";
    let delete = "delete";

    let num_partitions = 6;
    let replication_factor = 0;

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let resources = [DescribeConfigsResource::default()
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic_name.clone())
        .configuration_keys(None)];

    let include_synonyms = Some(false);
    let include_documentation = Some(false);

    let ctx = Context::with_state(sc);

    // --- Describe before any alter ---
    let results = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.clone().into())),
        )
        .await?;

    let none = ErrorCode::None;

    let result_list = results.results.unwrap_or_default();
    assert_eq!(1, result_list.len());
    assert_eq!(none, ErrorCode::try_from(result_list[0].error_code)?);
    let full_configs = result_list[0].configs.as_deref().unwrap_or_default();
    assert_eq!(EXPECTED_DEFAULT_CONFIG_COUNT, full_configs.len());

    // Before alter: cleanup.policy should be default "delete"
    let cp = find_config(full_configs, cleanup_policy);
    assert_eq!(cp.value.as_deref(), Some(delete));
    assert_eq!(cp.is_default, Some(true));

    // --- Alter: set cleanup.policy to compact ---
    let response = IncrementalAlterConfigsService
        .serve(
            ctx.clone(),
            IncrementalAlterConfigsRequest::default().resources(Some(
                [AlterConfigsResource::default()
                    .resource_type(ConfigResource::Topic.into())
                    .resource_name(topic_name.clone())
                    .configs(Some(vec![
                        AlterableConfig::default()
                            .name(cleanup_policy.into())
                            .config_operation(OpType::Set.into())
                            .value(Some(compact.into())),
                    ]))]
                .into(),
            )),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    assert_eq!(i16::from(none), responses[0].error_code);
    assert_eq!(i8::from(ConfigResource::Topic), responses[0].resource_type);
    assert_eq!(topic_name, responses[0].resource_name);

    // --- Describe after alter ---
    let results = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.clone().into())),
        )
        .await?;

    let result_list = results.results.unwrap_or_default();
    let full_configs = result_list[0].configs.as_deref().unwrap_or_default();
    assert_eq!(EXPECTED_DEFAULT_CONFIG_COUNT, full_configs.len());

    let cp = find_config(full_configs, cleanup_policy);
    assert_eq!(cp.value.as_deref(), Some(compact));
    assert_eq!(cp.is_default, None);

    // retention.ms still default
    let rm = find_config(full_configs, retention_ms);
    assert_eq!(rm.value.as_deref(), Some("604800000"));
    assert_eq!(rm.is_default, Some(true));

    // --- Key-filtered describe ---
    let filtered = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_type(ConfigResource::Topic.into())
                        .resource_name(topic_name.clone())
                        .configuration_keys(Some([cleanup_policy.into()].into()))]
                    .into(),
                )),
        )
        .await?;

    let filtered_results = filtered.results.unwrap_or_default();
    let filtered_configs = filtered_results[0].configs.as_deref().unwrap_or_default();
    assert_eq!(1, filtered_configs.len());
    assert_eq!(filtered_configs[0].name, cleanup_policy);
    assert_eq!(filtered_configs[0].value.as_deref(), Some(compact));

    // --- Alter: set cleanup.policy back to delete ---
    let response = IncrementalAlterConfigsService
        .serve(
            ctx.clone(),
            IncrementalAlterConfigsRequest::default().resources(Some(
                [AlterConfigsResource::default()
                    .resource_type(ConfigResource::Topic.into())
                    .resource_name(topic_name.clone())
                    .configs(Some(vec![
                        AlterableConfig::default()
                            .name(cleanup_policy.into())
                            .config_operation(OpType::Set.into())
                            .value(Some(delete.into())),
                    ]))]
                .into(),
            )),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    assert_eq!(i16::from(none), responses[0].error_code);

    let results = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.clone().into())),
        )
        .await?;

    let result_list = results.results.unwrap_or_default();
    let full_configs = result_list[0].configs.as_deref().unwrap_or_default();
    // After setting cleanup.policy to "delete" explicitly, it's still an
    // explicit override (is_default = None), not a revert to default.
    let cp = find_config(full_configs, cleanup_policy);
    assert_eq!(cp.value.as_deref(), Some(delete));
    assert_eq!(cp.is_default, None);

    // --- Alter: delete cleanup.policy (revert to default) ---
    let response = IncrementalAlterConfigsService
        .serve(
            ctx.clone(),
            IncrementalAlterConfigsRequest::default().resources(Some(
                [AlterConfigsResource::default()
                    .resource_type(ConfigResource::Topic.into())
                    .resource_name(topic_name.clone())
                    .configs(Some(vec![
                        AlterableConfig::default()
                            .name(cleanup_policy.into())
                            .config_operation(OpType::Delete.into())
                            .value(None),
                    ]))]
                .into(),
            )),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    assert_eq!(i16::from(none), responses[0].error_code);

    let results = DescribeConfigsService
        .serve(
            ctx,
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.into())),
        )
        .await?;

    let result_list = results.results.unwrap_or_default();
    let full_configs = result_list[0].configs.as_deref().unwrap_or_default();
    assert_eq!(EXPECTED_DEFAULT_CONFIG_COUNT, full_configs.len());

    // After deleting the explicit override, cleanup.policy should revert to
    // the default value "delete" with is_default = Some(true).
    let cp = find_config(full_configs, cleanup_policy);
    assert_eq!(cp.value.as_deref(), Some(delete));
    assert_eq!(cp.is_default, Some(true));

    debug!(?topic_id);
    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use std::sync::Arc;

    use common::{StorageType, init_tracing};
    use url::Url;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn alter_single_topic() -> Result<()> {
        let _guard = init_tracing()?;
        if std::env::var("POSTGRES_URL").is_err() {
            return Ok(());
        }

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::alter_single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;
        if std::env::var("POSTGRES_URL").is_err() {
            return Ok(());
        }

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use std::sync::Arc;

    use common::{StorageType, init_tracing};
    use url::Url;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn alter_single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::alter_single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "redlinedb")]
mod redlinedb {
    use std::sync::Arc;

    use common::{StorageType, init_tracing};
    use url::Url;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::RedlineDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn alter_single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::alter_single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use std::sync::Arc;

    use common::{StorageType, init_tracing};
    use url::Url;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::SlateDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn alter_single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::alter_single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
