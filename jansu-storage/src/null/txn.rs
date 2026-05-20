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

//! Transaction operations for the `null://` [`Engine`].

use jansu_sans_io::{
    add_partitions_to_txn_response::{AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};

use super::Engine;
use crate::{Result, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest};

impl Engine {
    pub(super) fn null_txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        Ok(match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree { topics, .. } => {
                TxnAddPartitionsResponse::VersionZeroToThree(
                    topics
                        .iter()
                        .map(|topic| {
                            AddPartitionsToTxnTopicResult::default()
                                .name(topic.name.clone())
                                .results_by_partition(Some([].into()))
                        })
                        .collect(),
                )
            }
            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                TxnAddPartitionsResponse::VersionFourPlus(
                    transactions
                        .iter()
                        .map(|_| AddPartitionsToTxnResult::default())
                        .collect(),
                )
            }
        })
    }

    pub(super) fn null_txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        Ok(offsets
            .topics
            .iter()
            .map(|topic| TxnOffsetCommitResponseTopic::default().name(topic.name.clone()))
            .collect())
    }
}
