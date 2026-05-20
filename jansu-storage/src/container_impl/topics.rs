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

//! Topic and configuration dispatchers for [`StorageContainer`].

use jansu_sans_io::{
    ConfigResource, ErrorCode, create_topics_request::CreatableTopic,
    delete_records_request::DeleteRecordsTopic, delete_records_response::DeleteRecordsTopicResult,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};
use opentelemetry::KeyValue;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    MetadataResponse, Result, STORAGE_CONTAINER_ERRORS, STORAGE_CONTAINER_REQUESTS, Storage,
    StorageContainer, TopicId, Topition,
};

#[instrument(skip_all)]
pub(super) async fn incremental_alter_resource(
    container: &StorageContainer,
    resource: AlterConfigsResource,
) -> Result<AlterConfigsResourceResponse> {
    let attributes = [KeyValue::new("method", "incremental_alter_resource")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.incremental_alter_resource(resource),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.incremental_alter_resource(resource),

        StorageContainer::Null(engine) => engine.incremental_alter_resource(resource),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.incremental_alter_resource(resource),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.incremental_alter_resource(resource),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.incremental_alter_resource(resource),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn create_topic(
    container: &StorageContainer,
    topic: CreatableTopic,
    validate_only: bool,
) -> Result<Uuid> {
    let attributes = [KeyValue::new("method", "create_topic")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.create_topic(topic, validate_only),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.create_topic(topic, validate_only),

        StorageContainer::Null(engine) => engine.create_topic(topic, validate_only),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.create_topic(topic, validate_only),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.create_topic(topic, validate_only),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.create_topic(topic, validate_only),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn delete_records(
    container: &StorageContainer,
    topics: &[DeleteRecordsTopic],
) -> Result<Vec<DeleteRecordsTopicResult>> {
    let attributes = [KeyValue::new("method", "delete_records")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.delete_records(topics),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.delete_records(topics),

        StorageContainer::Null(engine) => engine.delete_records(topics),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.delete_records(topics),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.delete_records(topics),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.delete_records(topics),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn delete_topic(
    container: &StorageContainer,
    topic: &TopicId,
) -> Result<ErrorCode> {
    let attributes = [KeyValue::new("method", "delete_topic")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.delete_topic(topic),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.delete_topic(topic),

        StorageContainer::Null(engine) => engine.delete_topic(topic),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.delete_topic(topic),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.delete_topic(topic),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.delete_topic(topic),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn metadata(
    container: &StorageContainer,
    topics: Option<&[TopicId]>,
) -> Result<MetadataResponse> {
    let attributes = [KeyValue::new("method", "metadata")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.metadata(topics),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.metadata(topics),

        StorageContainer::Null(engine) => engine.metadata(topics),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.metadata(topics),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.metadata(topics),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.metadata(topics),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn describe_config(
    container: &StorageContainer,
    name: &str,
    resource: ConfigResource,
    keys: Option<&[String]>,
) -> Result<DescribeConfigsResult> {
    let attributes = [KeyValue::new("method", "describe_config")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => engine.describe_config(name, resource, keys),

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => engine.describe_config(name, resource, keys),

        StorageContainer::Null(engine) => engine.describe_config(name, resource, keys),

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => engine.describe_config(name, resource, keys),

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => engine.describe_config(name, resource, keys),

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => engine.describe_config(name, resource, keys),
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}

#[instrument(skip_all)]
pub(super) async fn describe_topic_partitions(
    container: &StorageContainer,
    topics: Option<&[TopicId]>,
    partition_limit: i32,
    cursor: Option<Topition>,
) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
    let attributes = [KeyValue::new("method", "describe_topic_partitions")];

    match container {
        #[cfg(feature = "dynostore")]
        StorageContainer::DynoStore(engine) => {
            engine.describe_topic_partitions(topics, partition_limit, cursor)
        }

        #[cfg(feature = "libsql")]
        StorageContainer::Lite(engine) => {
            engine.describe_topic_partitions(topics, partition_limit, cursor)
        }

        StorageContainer::Null(engine) => {
            engine.describe_topic_partitions(topics, partition_limit, cursor)
        }

        #[cfg(feature = "postgres")]
        StorageContainer::Postgres(engine) => {
            engine.describe_topic_partitions(topics, partition_limit, cursor)
        }

        #[cfg(feature = "slatedb")]
        StorageContainer::Slate(engine) => {
            engine.describe_topic_partitions(topics, partition_limit, cursor)
        }

        #[cfg(feature = "turso")]
        StorageContainer::Turso(engine) => {
            engine.describe_topic_partitions(topics, partition_limit, cursor)
        }
    }
    .await
    .inspect(|_| {
        STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
    })
    .inspect_err(|_| {
        STORAGE_CONTAINER_ERRORS.add(1, &attributes);
    })
}
