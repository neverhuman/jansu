//! `EmbeddedBroker::fetch_one` hydrates a persisted topic/partition/offset
//! without advancing any shared consumer cursor.

use jansu_embedded::EmbeddedBroker;

#[tokio::test]
async fn fetch_one_returns_exact_offset_only() {
    let broker = EmbeddedBroker::new().await.expect("broker");
    broker.create_topic("artifacts", 1).await.expect("topic");

    let off0 = broker
        .send("artifacts", 0, Some(b"k0"), b"zero")
        .await
        .expect("send zero");
    let off1 = broker
        .send("artifacts", 0, Some(b"k1"), b"one")
        .await
        .expect("send one");

    let record = broker
        .fetch_one("artifacts", 0, off1)
        .await
        .expect("fetch")
        .expect("record");
    assert_eq!(record.offset, off1);
    assert_eq!(record.key.as_deref(), Some(b"k1".as_slice()));
    assert_eq!(record.payload, b"one");

    assert!(
        broker
            .fetch_one("artifacts", 0, off1 + 1)
            .await
            .expect("fetch missing")
            .is_none()
    );
    assert_eq!(off0 + 1, off1);
}
