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

//! Embedded SQL statement table, partition 3.

pub(super) fn chunk_3() -> Vec<(&'static str, String)> {
    vec![
        (
            "txn_offset_commit_insert.sql",
            include_sql!("../../sql/txn_offset_commit_insert.sql"),
        ),
        (
            "txn_offset_commit_tp_delete_by_topic.sql",
            include_sql!("../../sql/txn_offset_commit_tp_delete_by_topic.sql"),
        ),
        (
            "txn_offset_commit_tp_delete_by_txn.sql",
            include_sql!("../../sql/txn_offset_commit_tp_delete_by_txn.sql"),
        ),
        (
            "txn_offset_commit_tp_insert.sql",
            include_sql!("../../sql/txn_offset_commit_tp_insert.sql"),
        ),
        (
            "txn_produce_offset_delete_by_topic.sql",
            include_sql!("../../sql/txn_produce_offset_delete_by_topic.sql"),
        ),
        (
            "txn_produce_offset_delete_by_txn.sql",
            include_sql!("../../sql/txn_produce_offset_delete_by_txn.sql"),
        ),
        (
            "txn_produce_offset_insert.sql",
            include_sql!("../../sql/txn_produce_offset_insert.sql"),
        ),
        (
            "txn_produce_offset_select_offset_range.sql",
            include_sql!("../../sql/txn_produce_offset_select_offset_range.sql"),
        ),
        (
            "txn_produce_offset_select_overlapping_txn.sql",
            include_sql!("../../sql/txn_produce_offset_select_overlapping_txn.sql"),
        ),
        (
            "txn_select_name.sql",
            include_sql!("../../sql/txn_select_name.sql"),
        ),
        (
            "txn_select_produced_topitions.sql",
            include_sql!("../../sql/txn_select_produced_topitions.sql"),
        ),
        (
            "txn_select_producer_epoch.sql",
            include_sql!("../../sql/txn_select_producer_epoch.sql"),
        ),
        (
            "txn_status_update.sql",
            include_sql!("../../sql/txn_status_update.sql"),
        ),
        (
            "txn_topition_delete_by_topic.sql",
            include_sql!("../../sql/txn_topition_delete_by_topic.sql"),
        ),
        (
            "txn_topition_delete_by_txn.sql",
            include_sql!("../../sql/txn_topition_delete_by_txn.sql"),
        ),
        (
            "txn_topition_insert.sql",
            include_sql!("../../sql/txn_topition_insert.sql"),
        ),
        (
            "txn_topition_select_txns.sql",
            include_sql!("../../sql/txn_topition_select_txns.sql"),
        ),
        (
            "txn_topition_select.sql",
            include_sql!("../../sql/txn_topition_select.sql"),
        ),
        (
            "virtual_topic_upsert.sql",
            include_sql!("../../sql/virtual_topic_upsert.sql"),
        ),
        (
            "watermark_delete_by_topic.sql",
            include_sql!("../../sql/watermark_delete_by_topic.sql"),
        ),
        (
            "watermark_insert_from_txn.sql",
            include_sql!("../../sql/watermark_insert_from_txn.sql"),
        ),
        (
            "watermark_insert.sql",
            include_sql!("../../sql/watermark_insert.sql"),
        ),
        (
            "watermark_select_for_update.sql",
            include_sql!("../../sql/watermark_select_for_update.sql"),
        ),
        (
            "watermark_select_no_update.sql",
            include_sql!("../../sql/watermark_select_no_update.sql"),
        ),
        (
            "watermark_select_stable.sql",
            include_sql!("../../sql/watermark_select_stable.sql"),
        ),
        (
            "watermark_select.sql",
            include_sql!("../../sql/watermark_select.sql"),
        ),
        (
            "watermark_update.sql",
            include_sql!("../../sql/watermark_update.sql"),
        ),
    ]
}
