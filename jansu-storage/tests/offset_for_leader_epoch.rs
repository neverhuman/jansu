use bytes::Bytes;
use jansu_sans_io::{
    ErrorCode, OffsetForLeaderEpochRequest,
    offset_for_leader_epoch_request::{OffsetForLeaderPartition, OffsetForLeaderTopic},
    record::{Record, deflated, inflated},
};
use jansu_storage::{OffsetForLeaderEpochService, Storage, Topition};
use rama::{Context, Service};
use url::Url;

mod common;
use crate::common::{Error, build_storage, create_topic, init_tracing, register_broker};

fn single_record_batch_at_epoch(
    value: &'static [u8],
    epoch: i32,
) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .partition_leader_epoch(epoch)
        .record(Record::builder().value(Some(Bytes::from_static(value))))
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

async fn assert_storage_driven_epoch_boundaries(
    storage: &dyn Storage,
    topic: &str,
) -> Result<(), Error> {
    _ = create_topic(storage, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    let history = storage.leader_epoch_history(&tp).await?;
    assert_eq!(vec![(0, 0)], epoch_tuples(&history));

    let offset = storage
        .produce(None, &tp, single_record_batch_at_epoch(b"epoch-0", 0)?)
        .await?;
    assert_eq!(0, offset);

    let offset = storage
        .produce(None, &tp, single_record_batch_at_epoch(b"epoch-1", 1)?)
        .await?;
    assert_eq!(1, offset);

    let offset = storage
        .produce(
            None,
            &tp,
            single_record_batch_at_epoch(b"epoch-1-repeat", 1)?,
        )
        .await?;
    assert_eq!(2, offset);

    let offset = storage
        .produce(
            None,
            &tp,
            single_record_batch_at_epoch(b"epoch-0-lower", 0)?,
        )
        .await?;
    assert_eq!(3, offset);

    let history = storage.leader_epoch_history(&tp).await?;
    assert_eq!(vec![(0, 0), (1, 1)], epoch_tuples(&history));
    assert_eq!(Some((1, 1)), storage.offset_for_leader_epoch(&tp, 0).await?);
    assert_eq!(None, storage.offset_for_leader_epoch(&tp, 1).await?);

    Ok(())
}

fn epoch_tuples(history: &[jansu_storage::LeaderEpochRecord]) -> Vec<(i32, i64)> {
    history
        .iter()
        .map(|record| (record.epoch, record.start_offset))
        .collect()
}

#[cfg(feature = "redlinedb")]
fn redlinedb_path(storage_url: &Url) -> Result<std::path::PathBuf, Error> {
    let mut path = std::env::current_dir()?;

    if let Some(domain) = storage_url.domain() {
        path.push(domain);
    }

    if let Some(relative) = storage_url.path().strip_prefix("/") {
        path.push(relative);
    } else {
        path.push(storage_url.path());
    }

    Ok(path)
}

#[cfg(feature = "redlinedb")]
async fn insert_redlinedb_epoch_history(
    storage_url: &Url,
    cluster_id: &str,
    topic: &str,
    partition: i32,
    epochs: &[(i32, i64)],
) -> i64 {
    let db = redlinedb::Database::open_with_options(
        redlinedb_path(storage_url).unwrap(),
        redlinedb::OpenOptions {
            create: true,
            ..redlinedb::OpenOptions::default()
        },
    )
    .unwrap();
    let mut conn = db.connect().unwrap();

    let mut rows = conn
        .query(
            "
        select tp.id
        from topition tp
        join topic t on tp.topic = t.id
        join cluster c on t.cluster = c.id
        where c.name = ?1 and t.name = ?2 and tp.partition = ?3
    ",
            redlinedb::params![cluster_id, topic, i64::from(partition)],
        )
        .unwrap();
    let topition_id: i64 = match rows.step().unwrap() {
        redlinedb::Step::Row(row) => row.get(0).unwrap(),
        redlinedb::Step::Done => panic!("topition id must exist"),
    };

    for (epoch, start_offset) in epochs {
        _ = conn.execute(
            "insert into leader_epoch_history (topition, epoch, start_offset) values (?1, ?2, ?3)",
            redlinedb::params![topition_id, i64::from(*epoch), *start_offset],
        )
        .unwrap();
    }

    topition_id
}

#[cfg(feature = "redlinedb")]
#[tokio::test]
async fn redlinedb_epoch_boundaries_are_recorded_on_produce() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage_url = common::redlinedb_storage_url("phase08-redlinedb-produce-leader-epoch")?;

    let cluster_id = "phase08-redlinedb-produce-leader-epoch";
    let node_id = 111;
    let topic = "redlinedb-produce-leader-epoch-topic";

    let storage = build_storage(cluster_id, node_id, storage_url.clone()).await?;
    register_broker(&*storage, cluster_id, node_id).await?;
    assert_storage_driven_epoch_boundaries(&**storage, topic).await
}

#[cfg(feature = "redlinedb")]
#[tokio::test]
async fn offset_for_leader_epoch_service_returns_exact_epoch_errors() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage_url = common::redlinedb_storage_url("phase08-offset-for-leader-epoch-service")?;

    let cluster_id = "phase08-leader-epoch-service-test";
    let node_id = 111;
    let topic = "leader-epoch-service-topic";

    let storage = build_storage(cluster_id, node_id, storage_url.clone()).await?;
    register_broker(&*storage, cluster_id, node_id).await?;
    _ = create_topic(&*storage, topic, 1).await?;

    _ = insert_redlinedb_epoch_history(&storage_url, cluster_id, topic, 0, &[(1, 5)]).await;

    let partition = |leader_epoch, current_leader_epoch| {
        OffsetForLeaderPartition::default()
            .partition(0)
            .current_leader_epoch(Some(current_leader_epoch))
            .leader_epoch(leader_epoch)
    };

    let request = |partition| {
        OffsetForLeaderEpochRequest::default().topics(Some(vec![
            OffsetForLeaderTopic::default()
                .topic(topic.into())
                .partitions(Some(vec![partition])),
        ]))
    };

    let response = OffsetForLeaderEpochService
        .serve(
            Context::with_state(storage.clone()),
            request(partition(0, -1)),
        )
        .await?;
    let partitions = response.topics.as_deref().unwrap()[0]
        .partitions
        .as_deref()
        .unwrap();
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(1), partitions[0].leader_epoch);
    assert_eq!(5, partitions[0].end_offset);

    let response = OffsetForLeaderEpochService
        .serve(
            Context::with_state(storage.clone()),
            request(partition(1, -1)),
        )
        .await?;
    let partitions = response.topics.as_deref().unwrap()[0]
        .partitions
        .as_deref()
        .unwrap();
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(1), partitions[0].leader_epoch);
    assert_eq!(0, partitions[0].end_offset);

    let response = OffsetForLeaderEpochService
        .serve(
            Context::with_state(storage.clone()),
            request(partition(2, -1)),
        )
        .await?;
    let partitions = response.topics.as_deref().unwrap()[0]
        .partitions
        .as_deref()
        .unwrap();
    assert_eq!(
        i16::from(ErrorCode::UnknownLeaderEpoch),
        partitions[0].error_code
    );

    let response = OffsetForLeaderEpochService
        .serve(
            Context::with_state(storage.clone()),
            request(partition(0, 0)),
        )
        .await?;
    let partitions = response.topics.as_deref().unwrap()[0]
        .partitions
        .as_deref()
        .unwrap();
    assert_eq!(
        i16::from(ErrorCode::FencedLeaderEpoch),
        partitions[0].error_code
    );

    let response = OffsetForLeaderEpochService
        .serve(
            Context::with_state(storage.clone()),
            request(partition(0, 2)),
        )
        .await?;
    let partitions = response.topics.as_deref().unwrap()[0]
        .partitions
        .as_deref()
        .unwrap();
    assert_eq!(
        i16::from(ErrorCode::UnknownLeaderEpoch),
        partitions[0].error_code
    );

    let response = OffsetForLeaderEpochService
        .serve(
            Context::with_state(storage.clone()),
            OffsetForLeaderEpochRequest::default().topics(Some(vec![
                OffsetForLeaderTopic::default()
                    .topic("unknown-leader-epoch-topic".into())
                    .partitions(Some(vec![partition(0, -1)])),
            ])),
        )
        .await?;
    let partitions = response.topics.as_deref().unwrap()[0]
        .partitions
        .as_deref()
        .unwrap();
    assert_eq!(
        i16::from(ErrorCode::UnknownTopicOrPartition),
        partitions[0].error_code
    );

    Ok(())
}

#[cfg(feature = "redlinedb")]
#[tokio::test]
async fn redlinedb_leader_epoch_history_unknown_topition_returns_unknown_topic_or_partition()
-> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage_url = common::redlinedb_storage_url("phase08-leader-epoch-history-unknown")?;

    let storage = build_storage("phase08-leader-epoch-history-unknown", 111, storage_url).await?;

    let err = storage
        .leader_epoch_history(&Topition::new("missing-topic", 0))
        .await
        .expect_err("unknown topition should be rejected");
    assert!(matches!(
        err,
        jansu_storage::Error::Api(ErrorCode::UnknownTopicOrPartition)
    ));

    Ok(())
}

#[cfg(feature = "slatedb")]
#[tokio::test]
async fn slatedb_epoch_boundaries_are_recorded_on_produce() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = "slatedb-produce-epoch-test";
    let node_id = 1;
    let topic = "slatedb-produce-epoch-topic";

    let storage = build_storage(cluster_id, node_id, Url::parse("slatedb://memory")?).await?;
    register_broker(&*storage, cluster_id, node_id).await?;
    assert_storage_driven_epoch_boundaries(&**storage, topic).await
}

#[cfg(feature = "slatedb")]
#[tokio::test]
async fn slatedb_epoch_unknown_topition_returns_unknown_topic_or_partition() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage =
        build_storage("slatedb-epoch-unknown", 1, Url::parse("slatedb://memory")?).await?;
    register_broker(&*storage, "slatedb-epoch-unknown", 1).await?;

    let err = storage
        .leader_epoch_history(&Topition::new("nonexistent", 0))
        .await
        .expect_err("unknown topition should be rejected");
    assert!(matches!(
        err,
        jansu_storage::Error::Api(ErrorCode::UnknownTopicOrPartition)
    ));

    let err = storage
        .offset_for_leader_epoch(&Topition::new("nonexistent", 0), 0)
        .await
        .expect_err("unknown topition should be rejected");
    assert!(matches!(
        err,
        jansu_storage::Error::Api(ErrorCode::UnknownTopicOrPartition)
    ));

    Ok(())
}

/// Verify DynoStore initializes epoch 0 at offset 0 on topic creation and
/// that offset_for_leader_epoch correctly returns None for the current epoch
/// and returns the next epoch boundary when queried.
#[cfg(feature = "dynostore")]
#[tokio::test]
async fn dynostore_epoch_boundaries_are_recorded_on_produce() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = "dyno-produce-epoch-test";
    let node_id = 1;
    let topic = "dyno-produce-epoch-topic";

    let storage_url = Url::parse("memory://dyno-produce-epoch-test/")?;
    let storage = build_storage(cluster_id, node_id, storage_url).await?;
    register_broker(&*storage, cluster_id, node_id).await?;
    assert_storage_driven_epoch_boundaries(&**storage, topic).await
}

/// Verify that requesting history from a non-existent topition is a partition error.
#[cfg(feature = "dynostore")]
#[tokio::test]
async fn dynostore_epoch_unknown_topition_returns_unknown_topic_or_partition() -> Result<(), Error>
{
    let _guard = init_tracing()?;

    let cluster_id = "dyno-epoch-unknown";
    let node_id = 1;

    let storage_url = Url::parse("memory://dyno-epoch-unknown/")?;
    let storage = build_storage(cluster_id, node_id, storage_url).await?;
    register_broker(&*storage, cluster_id, node_id).await?;

    let tp = Topition::new("nonexistent", 0);
    let err = storage
        .leader_epoch_history(&tp)
        .await
        .expect_err("unknown topition should be rejected");
    assert!(matches!(
        err,
        jansu_storage::Error::Api(ErrorCode::UnknownTopicOrPartition)
    ));

    Ok(())
}
