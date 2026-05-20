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

//! Embedded SQL statement table, partition 0.

pub(super) fn chunk_0() -> Vec<(&'static str, String)> {
    vec![
        (
            "maintain-vacuum.sql",
            include_sql!("../../sql/maintain-vacuum.sql"),
        ),
        (
            "consumer_group_delete.sql",
            include_sql!("../../sql/consumer_group_delete.sql"),
        ),
        (
            "consumer_group_detail_delete_by_cg.sql",
            include_sql!("../../sql/consumer_group_detail_delete_by_cg.sql"),
        ),
        (
            "consumer_group_detail_insert.sql",
            include_sql!("../../sql/consumer_group_detail_insert.sql"),
        ),
        (
            "consumer_group_detail.sql",
            include_sql!("../../sql/consumer_group_detail.sql"),
        ),
        (
            "consumer_group_insert.sql",
            include_sql!("../../sql/consumer_group_insert.sql"),
        ),
        (
            "consumer_group_select_by_name.sql",
            include_sql!("../../sql/consumer_group_select_by_name.sql"),
        ),
        (
            "consumer_group_select.sql",
            include_sql!("../../sql/consumer_group_select.sql"),
        ),
        (
            "consumer_offset_delete_by_cg.sql",
            include_sql!("../../sql/consumer_offset_delete_by_cg.sql"),
        ),
        (
            "consumer_offset_delete_by_topic.sql",
            include_sql!("../../sql/consumer_offset_delete_by_topic.sql"),
        ),
        (
            "consumer_offset_delete_expired.sql",
            include_sql!("../../sql/consumer_offset_delete_expired.sql"),
        ),
        (
            "consumer_offset_insert_from_txn.sql",
            include_sql!("../../sql/consumer_offset_insert_from_txn.sql"),
        ),
        (
            "consumer_offset_insert.sql",
            include_sql!("../../sql/consumer_offset_insert.sql"),
        ),
        (
            "consumer_offset_select_by_group.sql",
            include_sql!("../../sql/consumer_offset_select_by_group.sql"),
        ),
        (
            "consumer_offset_select.sql",
            include_sql!("../../sql/consumer_offset_select.sql"),
        ),
        ("header_copy.sql", include_sql!("../../sql/header_copy.sql")),
        (
            "header_delete_by_topic.sql",
            include_sql!("../../sql/header_delete_by_topic.sql"),
        ),
        (
            "header_fetch.sql",
            include_sql!("../../sql/header_fetch.sql"),
        ),
        (
            "header_insert.sql",
            include_sql!("../../sql/header_insert.sql"),
        ),
        (
            "list_earliest_offset.sql",
            include_sql!("../../sql/list_earliest_offset.sql"),
        ),
        (
            "list_latest_offset_committed.sql",
            include_sql!("../../sql/list_latest_offset_committed.sql"),
        ),
        (
            "list_latest_offset_timestamp.sql",
            include_sql!("../../sql/list_latest_offset_timestamp.sql"),
        ),
        (
            "list_latest_offset_uncommitted.sql",
            include_sql!("../../sql/list_latest_offset_uncommitted.sql"),
        ),
        (
            "leader_epoch_history.sql",
            include_sql!("../../sql/leader_epoch_history.sql"),
        ),
        (
            "leader_epoch_history_insert.sql",
            include_sql!("../../sql/leader_epoch_history_insert.sql"),
        ),
        (
            "lite/policy_compact_compaction.sql",
            include_sql!("../../lite/policy_compact_compaction.sql"),
        ),
        (
            "lite/policy_compact_delete.sql",
            include_sql!("../../lite/policy_compact_delete.sql"),
        ),
    ]
}
