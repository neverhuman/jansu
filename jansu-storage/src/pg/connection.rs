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

//! Connection pool + SQL lookup helpers for the PostgreSQL backend.

use super::*;

impl Postgres {
    pub(super) async fn connection(&self) -> Result<Object> {
        self.pool.get().await.map_err(Into::into)
    }

    pub(super) fn sql_lookup(&self, key: &str) -> Result<&str> {
        crate::sql::SQL.get(key)
    }

    pub(super) fn attributes_for_error(
        &self,
        nickname: &str,
        error: &tokio_postgres::error::Error,
    ) -> Vec<KeyValue> {
        let mut attributes = vec![
            KeyValue::new("sql", nickname.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        if let Some(db_error) = error.as_db_error() {
            if let Some(schema) = db_error.schema() {
                attributes.push(KeyValue::new("schema", schema.to_owned()));
            }

            if let Some(table) = db_error.table() {
                attributes.push(KeyValue::new("table", table.to_owned()));
            }

            if let Some(constraint) = db_error.constraint() {
                attributes.push(KeyValue::new("constraint", constraint.to_owned()));
            }
        }

        if let Some(code) = error.code() {
            attributes.push(KeyValue::new("code", format!("{code:?}")));
        }

        attributes
    }
}
