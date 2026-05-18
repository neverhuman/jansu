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

mod common;

use std::{slice::from_ref, time::Duration};

use crate::common::{Error, build_storage, create_topic, init_tracing, register_broker};
use jansu_sans_io::offset_commit_request::OffsetCommitRequestPartition;
use jansu_storage::{OffsetCommitRequest, Storage, Topition};
use rand::{RngExt as _, rng};
use tokio::time::sleep;
use url::Url;
use uuid::Uuid;

async fn offset_retention_cleanup_round_trip<S>(
    storage: S,
    cluster_id: &str,
    node_id: i32,
) -> Result<(), Error>
where
    S: Storage + Clone,
{
    let group_id = format!("group-{}", Uuid::now_v7());
    let expired_topic = format!("expired-{}", Uuid::now_v7());
    let live_topic = format!("live-{}", Uuid::now_v7());

    register_broker(&storage, cluster_id, node_id).await?;
    _ = create_topic(&storage, &expired_topic, 1).await?;
    _ = create_topic(&storage, &live_topic, 1).await?;

    let expired_commit = OffsetCommitRequest::try_from(
        &OffsetCommitRequestPartition::default()
            .partition_index(0)
            .committed_offset(11)
            .committed_leader_epoch(Some(1))
            .committed_metadata(Some("expired".into())),
    )?;
    let live_commit = OffsetCommitRequest::try_from(
        &OffsetCommitRequestPartition::default()
            .partition_index(0)
            .committed_offset(22)
            .committed_leader_epoch(Some(2))
            .committed_metadata(Some("live".into())),
    )?;

    let expired_topition = Topition::new(expired_topic.clone(), 0);
    let live_topition = Topition::new(live_topic.clone(), 0);

    _ = storage
        .offset_commit(
            &group_id,
            Some(Duration::from_secs(5)),
            &[(expired_topition.clone(), expired_commit)],
        )
        .await?;
    _ = storage
        .offset_commit(&group_id, None, &[(live_topition.clone(), live_commit)])
        .await?;

    let pre_maintain = storage
        .offset_fetch_records(
            Some(&group_id),
            &[expired_topition.clone(), live_topition.clone()],
            None,
        )
        .await?;
    assert_eq!(11, pre_maintain[&expired_topition].committed_offset());
    assert_eq!(22, pre_maintain[&live_topition].committed_offset());

    sleep(Duration::from_secs(6)).await;
    storage.maintain(std::time::SystemTime::now()).await?;

    let post_maintain = storage.committed_offset_topitions(&group_id).await?;
    assert_eq!(Some(&22), post_maintain.get(&live_topition));
    assert!(!post_maintain.contains_key(&expired_topition));

    let post_fetch = storage
        .offset_fetch_records(
            Some(&group_id),
            &[expired_topition.clone(), live_topition.clone()],
            None,
        )
        .await?;
    assert_eq!(-1, post_fetch[&expired_topition].committed_offset());
    assert_eq!(22, post_fetch[&live_topition].committed_offset());

    Ok(())
}

#[cfg(feature = "redlinedb")]
#[tokio::test]
async fn redlinedb_offset_commit_fetch_round_trip() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7().to_string();
    let node_id = rng().random_range(0..i32::MAX);
    let group_id = format!("group-{}", Uuid::now_v7());
    let topic = format!("topic-{}", Uuid::now_v7());
    let storage_url = common::redlinedb_storage_url("consumer-offsets")?;

    let storage = build_storage(&cluster_id, node_id, storage_url).await?;
    register_broker(&*storage, &cluster_id, node_id).await?;
    _ = create_topic(&*storage, &topic, 1).await?;

    let commit = OffsetCommitRequest::try_from(
        &OffsetCommitRequestPartition::default()
            .partition_index(0)
            .committed_offset(42)
            .committed_leader_epoch(Some(7))
            .committed_metadata(Some("meta".into())),
    )?;

    let topition = Topition::new(topic.clone(), 0);
    let response = storage
        .offset_commit(&group_id, None, &[(topition.clone(), commit.clone())])
        .await?;

    assert_eq!(1, response.len());
    assert_eq!(jansu_sans_io::ErrorCode::None, response[0].1);

    let fetched = storage
        .offset_fetch_records(Some(&group_id), from_ref(&topition), None)
        .await?;

    let record = fetched
        .get(&topition)
        .expect("offset fetch must return committed record");
    assert_eq!(42, record.committed_offset());
    assert_eq!(Some(7), record.leader_epoch());
    assert_eq!(Some("meta"), record.metadata());
    assert!(record.commit_timestamp().is_some());
    assert!(record.expires_at().is_some());

    let offsets = storage
        .offset_fetch(Some(&group_id), from_ref(&topition), None)
        .await?;
    assert_eq!(Some(&42), offsets.get(&topition));

    let committed = storage.committed_offset_topitions(&group_id).await?;
    assert_eq!(Some(&42), committed.get(&topition));

    Ok(())
}

#[cfg(feature = "redlinedb")]
#[tokio::test]
async fn redlinedb_offset_commit_expires_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7().to_string();
    let node_id = rng().random_range(0..i32::MAX);
    let group_id = format!("group-{}", Uuid::now_v7());
    let topic = format!("topic-{}", Uuid::now_v7());
    let storage_url = common::redlinedb_storage_url("consumer-offsets")?;

    let storage = build_storage(&cluster_id, node_id, storage_url).await?;
    register_broker(&*storage, &cluster_id, node_id).await?;
    _ = create_topic(&*storage, &topic, 1).await?;

    let commit = OffsetCommitRequest::try_from(
        &OffsetCommitRequestPartition::default()
            .partition_index(0)
            .committed_offset(99)
            .committed_leader_epoch(Some(1))
            .committed_metadata(Some("soon-expired".into())),
    )?;

    let topition = Topition::new(topic.clone(), 0);
    _ = storage
        .offset_commit(
            &group_id,
            Some(Duration::from_secs(1)),
            &[(topition.clone(), commit)],
        )
        .await?;

    sleep(Duration::from_secs(2)).await;

    let fetched = storage
        .offset_fetch_records(Some(&group_id), from_ref(&topition), None)
        .await?;
    let record = fetched
        .get(&topition)
        .expect("offset fetch must return placeholder record");
    assert_eq!(-1, record.committed_offset());

    let offsets = storage
        .offset_fetch(Some(&group_id), from_ref(&topition), None)
        .await?;
    assert_eq!(Some(&-1), offsets.get(&topition));

    Ok(())
}

#[cfg(feature = "dynostore")]
#[tokio::test]
async fn dynostore_offset_commit_maintain_deletes_expired_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7().to_string();
    let node_id = rng().random_range(0..i32::MAX);
    let storage = build_storage(
        &cluster_id,
        node_id,
        Url::parse("memory://phase10-retention/")?,
    )
    .await?;

    offset_retention_cleanup_round_trip(storage, &cluster_id, node_id).await
}

#[cfg(feature = "slatedb")]
#[tokio::test]
async fn slatedb_offset_commit_maintain_clears_expired_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7().to_string();
    let node_id = rng().random_range(0..i32::MAX);
    let storage = build_storage(&cluster_id, node_id, Url::parse("slatedb://memory")?).await?;

    offset_retention_cleanup_round_trip(storage, &cluster_id, node_id).await
}
