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

//! Transactional offset commit operation for the SlateDB engine.

use std::time::SystemTime;

use jansu_sans_io::{
    ErrorCode,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use tracing::debug;

use crate::{DEFAULT_OFFSET_RETENTION, Error, Result, TxnOffsetCommitRequest};

use super::super::engine::Engine;
use super::super::types::{Transactions, TxnCommitOffset};

impl Engine {
    pub(in crate::slate) async fn txn_offset_commit_op(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut transactions: Transactions = self.load_metadata(&tx, Self::TRANSACTIONS).await?;

        let error_response = |error_code: ErrorCode| -> Vec<TxnOffsetCommitResponseTopic> {
            offsets
                .topics
                .iter()
                .map(|topic| {
                    TxnOffsetCommitResponseTopic::default()
                        .name(topic.name.clone())
                        .partitions(Some(
                            topic
                                .partitions
                                .as_deref()
                                .unwrap_or(&[])
                                .iter()
                                .map(|p| {
                                    TxnOffsetCommitResponsePartition::default()
                                        .partition_index(p.partition_index)
                                        .error_code(error_code.into())
                                })
                                .collect(),
                        ))
                })
                .collect()
        };

        let Some(transaction) = transactions.get_mut(&offsets.transaction_id) else {
            return Ok(error_response(ErrorCode::TransactionalIdNotFound));
        };

        if transaction.producer != offsets.producer_id {
            return Ok(error_response(ErrorCode::UnknownProducerId));
        }

        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
            return Ok(error_response(ErrorCode::ProducerFenced));
        };

        if &offsets.producer_epoch != current_epoch.key() {
            return Ok(error_response(ErrorCode::ProducerFenced));
        }

        let txn_detail = current_epoch.get_mut();
        let now = SystemTime::now();
        let expires_at = now.checked_add(DEFAULT_OFFSET_RETENTION);
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    _ = txn_detail
                        .offsets
                        .entry(offsets.group_id.clone())
                        .or_default()
                        .entry(topic.name.clone())
                        .or_default()
                        .insert(
                            partition.partition_index,
                            TxnCommitOffset {
                                committed_offset: partition.committed_offset,
                                leader_epoch: partition.committed_leader_epoch,
                                metadata: partition.committed_metadata.clone(),
                                commit_timestamp: Some(now),
                                expires_at,
                            },
                        );

                    partition_responses.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(ErrorCode::None.into()),
                    );
                }
            }

            responses.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        postcard::to_stdvec(&transactions)
            .map_err(Error::from)
            .and_then(|encoded| tx.put(Self::TRANSACTIONS, encoded).map_err(Into::into))?;

        tx.commit().await.map_err(Error::from)?;

        Ok(responses)
    }
}
