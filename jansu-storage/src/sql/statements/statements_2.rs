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

//! Embedded SQL statement table, partition 2.

pub(super) fn chunk_2() -> Vec<(&'static str, String)> {
    vec![
        (
            "scram_credential_delete.sql",
            include_sql!("../../sql/scram_credential_delete.sql"),
        ),
        (
            "scram_credential_insert.sql",
            include_sql!("../../sql/scram_credential_insert.sql"),
        ),
        (
            "scram_credential_select.sql",
            include_sql!("../../sql/scram_credential_select.sql"),
        ),
        (
            "topic_by_cluster.sql",
            include_sql!("../../sql/topic_by_cluster.sql"),
        ),
        (
            "topic_by_uuid.sql",
            include_sql!("../../sql/topic_by_uuid.sql"),
        ),
        (
            "topic_configuration_delete_by_topic.sql",
            include_sql!("../../sql/topic_configuration_delete_by_topic.sql"),
        ),
        (
            "topic_configuration_delete.sql",
            include_sql!("../../sql/topic_configuration_delete.sql"),
        ),
        (
            "topic_configuration_select.sql",
            include_sql!("../../sql/topic_configuration_select.sql"),
        ),
        (
            "topic_configuration_upsert.sql",
            include_sql!("../../sql/topic_configuration_upsert.sql"),
        ),
        (
            "topic_delete_by.sql",
            include_sql!("../../sql/topic_delete_by.sql"),
        ),
        (
            "topic_insert.sql",
            include_sql!("../../sql/topic_insert.sql"),
        ),
        (
            "topic_select.sql",
            include_sql!("../../sql/topic_select.sql"),
        ),
        (
            "topic_select_name.sql",
            include_sql!("../../sql/topic_select_name.sql"),
        ),
        (
            "topic_select_uuid.sql",
            include_sql!("../../sql/topic_select_uuid.sql"),
        ),
        (
            "pg/topic_select_uuid.sql",
            include_sql!("../../pg/topic_select_uuid.sql"),
        ),
        (
            "topition_delete_by_topic.sql",
            include_sql!("../../sql/topition_delete_by_topic.sql"),
        ),
        (
            "topition_insert.sql",
            include_sql!("../../sql/topition_insert.sql"),
        ),
        (
            "topition_select.sql",
            include_sql!("../../sql/topition_select.sql"),
        ),
        (
            "topition_select_id.sql",
            include_sql!("../../sql/topition_select_id.sql"),
        ),
        (
            "txn_detail_insert.sql",
            include_sql!("../../sql/txn_detail_insert.sql"),
        ),
        (
            "txn_detail_select_current.sql",
            include_sql!("../../sql/txn_detail_select_current.sql"),
        ),
        (
            "txn_detail_select_for_update.sql",
            include_sql!("../../sql/txn_detail_select_for_update.sql"),
        ),
        (
            "txn_detail_select.sql",
            include_sql!("../../sql/txn_detail_select.sql"),
        ),
        (
            "txn_detail_update_sequence.sql",
            include_sql!("../../sql/txn_detail_update_sequence.sql"),
        ),
        (
            "txn_detail_update_started_at.sql",
            include_sql!("../../sql/txn_detail_update_started_at.sql"),
        ),
        ("txn_insert.sql", include_sql!("../../sql/txn_insert.sql")),
        (
            "txn_offset_commit_delete_by_txn.sql",
            include_sql!("../../sql/txn_offset_commit_delete_by_txn.sql"),
        ),
    ]
}
