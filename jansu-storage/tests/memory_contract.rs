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

#![cfg(feature = "dynostore")]

mod common;

use crate::common::{Error, build_storage, create_topic, init_tracing, register_broker};
use jansu_sans_io::ErrorCode;
use jansu_storage::TopicId;
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn memory_storage_is_empty_after_recreate() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7().to_string();
    let node_id = rng().random_range(0..i32::MAX);
    let topic = format!("memory-contract-{}", Uuid::now_v7());
    let storage_url = Url::parse(&format!("memory://{cluster_id}/"))?;

    let topic_id = {
        let storage = build_storage(&cluster_id, node_id, storage_url.clone()).await?;
        register_broker(&*storage, &cluster_id, node_id).await?;
        create_topic(&*storage, &topic, 1).await?
    };

    let storage = build_storage(&cluster_id, node_id, storage_url).await?;
    register_broker(&*storage, &cluster_id, node_id).await?;

    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        storage.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}
