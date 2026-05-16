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

use super::*;

pub(super) async fn txn_offset_commit(
    this: &Engine,
    offsets: TxnOffsetCommitRequest,
) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
    debug!(cluster = this.cluster, ?offsets);

    let mut c = this.connection().await.inspect_err(|err| error!(?err))?;
    let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

    let (producer_id, producer_epoch) = if let Some(row) = this
        .prepare_query_opt(
            &tx,
            &sql_lookup("producer_epoch_for_current_txn.sql")?,
            (this.cluster.as_str(), offsets.transaction_id.as_str()),
        )
        .await
        .inspect_err(|err| error!(?err))?
    {
        let producer_id = row
            .get_value(0)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?err))?;

        let epoch = row
            .get_value(1)
            .map(|value| value.as_integer().map(|i| *i as i16))
            .inspect_err(|err| error!(?err))?;

        (producer_id, epoch)
    } else {
        (None, None)
    };

    _ = this
        .prepare_execute(
            &tx,
            &sql_lookup("consumer_group_insert.sql")?,
            (this.cluster.as_str(), offsets.group_id.as_str()),
        )
        .await?;

    debug!(?producer_id, ?producer_epoch);

    _ = this
        .prepare_execute(
            &tx,
            &sql_lookup("txn_offset_commit_insert.sql")?,
            (
                this.cluster.as_str(),
                offsets.transaction_id.as_str(),
                offsets.group_id.as_str(),
                offsets.producer_id,
                offsets.producer_epoch,
                offsets.generation_id,
                offsets.member_id,
            ),
        )
        .await
        .inspect_err(|err| error!(?err))?;

    let mut topics = vec![];

    for topic in offsets.topics {
        let mut partitions = vec![];

        for partition in topic.partitions.unwrap_or(vec![]) {
            if producer_id.is_some_and(|producer_id| producer_id == offsets.producer_id) {
                if producer_epoch
                    .is_some_and(|producer_epoch| producer_epoch == offsets.producer_epoch)
                {
                    _ = this
                        .prepare_execute(
                            &tx,
                            &sql_lookup("txn_offset_commit_tp_insert.sql")?,
                            (
                                this.cluster.as_str(),
                                offsets.transaction_id.as_str(),
                                offsets.group_id.as_str(),
                                offsets.producer_id,
                                offsets.producer_epoch,
                                topic.name.as_str(),
                                partition.partition_index,
                                partition.committed_offset,
                                partition.committed_leader_epoch,
                                partition.committed_metadata,
                            ),
                        )
                        .await
                        .inspect_err(|err| error!(?err))?;

                    partitions.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(i16::from(ErrorCode::None)),
                    );
                } else {
                    partitions.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(i16::from(ErrorCode::InvalidProducerEpoch)),
                    );
                }
            } else {
                partitions.push(
                    TxnOffsetCommitResponsePartition::default()
                        .partition_index(partition.partition_index)
                        .error_code(i16::from(ErrorCode::UnknownProducerId)),
                );
            }
        }

        topics.push(
            TxnOffsetCommitResponseTopic::default()
                .name(topic.name)
                .partitions(Some(partitions)),
        );
    }

    tx.commit().await?;

    Ok(topics)
}

pub(super) async fn txn_end(
    this: &Engine,
    transaction_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> Result<ErrorCode> {
    debug!(cluster = ?this.cluster, transaction_id, producer_id, producer_epoch, committed);

    let mut c = this.connection().await.inspect_err(|err| error!(?err))?;
    let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

    let error_code = this
        .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &tx)
        .await?;

    tx.commit().await?;

    Ok(error_code)
}

pub(super) async fn maintain(_this: &Engine, _now: SystemTime) -> Result<()> {
    Ok(())
}

pub(super) async fn cluster_id(this: &Engine) -> Result<String> {
    Ok(this.cluster.clone())
}

pub(super) async fn node(this: &Engine) -> Result<i32> {
    Ok(this.node)
}

pub(super) async fn advertised_listener(this: &Engine) -> Result<Url> {
    Ok(this.advertised_listener.clone())
}

pub(super) async fn delete_user_scram_credential(
    _this: &Engine,
    _user: &str,
    _mechanism: ScramMechanism,
) -> Result<()> {
    todo!()
}

pub(super) async fn upsert_user_scram_credential(
    _this: &Engine,
    _user: &str,
    _mechanism: ScramMechanism,
    _credential: ScramCredential,
) -> Result<()> {
    todo!()
}

pub(super) async fn user_scram_credential(
    _this: &Engine,
    _user: &str,
    _mechanism: ScramMechanism,
) -> Result<Option<ScramCredential>> {
    todo!()
}

pub(super) async fn ping(this: &Engine) -> Result<()> {
    let c = this.connection().await?;
    let _ = c.query("ping.sql", ()).await?;
    Ok(())
}
