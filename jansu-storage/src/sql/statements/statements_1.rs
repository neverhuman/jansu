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

//! Embedded SQL statement table, partition 1.

pub(super) fn chunk_1() -> Vec<(&'static str, String)> {
    vec![
        (
            "lite/policy_compact_distinct_k.sql",
            include_sql!("../../lite/policy_compact_distinct_k.sql"),
        ),
        (
            "lite/policy_compact_max_offset_id.sql",
            include_sql!("../../lite/policy_compact_max_offset_id.sql"),
        ),
        (
            "lite/policy_compact_topitions.sql",
            include_sql!("../../lite/policy_compact_topitions.sql"),
        ),
        (
            "lite/policy_delete.sql",
            include_sql!("../../lite/policy_delete.sql"),
        ),
        (
            "lite/vacuum_into.sql",
            include_sql!("../../lite/vacuum_into.sql"),
        ),
        (
            "policy_compact.sql",
            include_sql!("../../sql/policy_compact.sql"),
        ),
        (
            "policy_delete.sql",
            include_sql!("../../sql/policy_delete.sql"),
        ),
        ("ping.sql", "select 1".to_string()),
        (
            "producer_detail_delete_by_topic.sql",
            include_sql!("../../sql/producer_detail_delete_by_topic.sql"),
        ),
        (
            "producer_detail_insert.sql",
            include_sql!("../../sql/producer_detail_insert.sql"),
        ),
        (
            "producer_epoch_current_for_producer.sql",
            include_sql!("../../sql/producer_epoch_current_for_producer.sql"),
        ),
        (
            "producer_epoch_for_current_txn.sql",
            include_sql!("../../sql/producer_epoch_for_current_txn.sql"),
        ),
        (
            "producer_epoch_insert.sql",
            include_sql!("../../sql/producer_epoch_insert.sql"),
        ),
        (
            "producer_insert.sql",
            include_sql!("../../sql/producer_insert.sql"),
        ),
        (
            "producer_select_for_update.sql",
            include_sql!("../../sql/producer_select_for_update.sql"),
        ),
        (
            "producer_update_epoch_with_txn.sql",
            include_sql!("../../sql/producer_update_epoch_with_txn.sql"),
        ),
        (
            "producer_update_sequence.sql",
            include_sql!("../../sql/producer_update_sequence.sql"),
        ),
        ("record_copy.sql", include_sql!("../../sql/record_copy.sql")),
        (
            "record_delete_before_offset.sql",
            include_sql!("../../sql/record_delete_before_offset.sql"),
        ),
        (
            "record_delete_by_topic.sql",
            include_sql!("../../sql/record_delete_by_topic.sql"),
        ),
        (
            "record_fetch.sql",
            include_sql!("../../sql/record_fetch.sql"),
        ),
        (
            "record_fetch_keyed.sql",
            include_sql!("../../sql/record_fetch_keyed.sql"),
        ),
        (
            "offset_for_leader_epoch.sql",
            include_sql!("../../sql/offset_for_leader_epoch.sql"),
        ),
        (
            "record_fetch_pg.sql",
            include_sql!("../../pg/record_fetch.sql"),
        ),
        (
            "record_fetch_pg_keyed.sql",
            include_sql!("../../pg/record_fetch_keyed.sql"),
        ),
        (
            "record_insert.sql",
            include_sql!("../../sql/record_insert.sql"),
        ),
        (
            "register_broker.sql",
            include_sql!("../../sql/register_broker.sql"),
        ),
    ]
}
