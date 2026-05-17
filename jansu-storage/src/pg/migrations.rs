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

//! Maintenance, SCRAM credentials, cluster metadata

use super::*;

impl Postgres {
    #[instrument(skip_all)]
    pub(super) async fn maintain_storage(&self, now: SystemTime) -> Result<()> {
        let deleted = self.policy_delete(now).await?;
        debug!(deleted);

        let c = self.connection().await?;
        let expired = self
            .prepare_execute(
                &c,
                "consumer_offset_delete_expired.sql",
                &[&self.cluster, &now],
            )
            .await?;
        debug!(expired);

        let compacted = self.policy_compact().await?;
        debug!(compacted);

        if let Some(ref lake) = self.lake {
            return lake.maintain().await.map_err(Into::into);
        }

        Ok(())
    }

    pub(super) async fn delete_user_scram_credential_storage(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let c = self.connection().await?;

        self.prepare_execute(
            &c,
            "scram_credential_delete.sql",
            &[&self.cluster, &user, &i32::from(mechanism)],
        )
        .await
        .inspect_err(|err| error!(?err, ?user, ?mechanism,))
        .and(Ok(()))
    }

    pub(super) async fn upsert_user_scram_credential_storage(
        &self,
        username: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let c = self.connection().await?;

        self.prepare_execute(
            &c,
            "scram_credential_insert.sql",
            &[
                &self.cluster,
                &username,
                &i32::from(mechanism),
                &&credential.salt[..],
                &credential.iterations,
                &&credential.stored_key[..],
                &&credential.server_key[..],
            ],
        )
        .await
        .inspect_err(|err| error!(?err, ?username, ?mechanism,))
        .and(Ok(()))
    }

    pub(super) async fn user_scram_credential_storage(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let c = self.connection().await?;

        self.prepare_query_opt(
            &c,
            "scram_credential_select.sql",
            &[&self.cluster, &user, &i32::from(mechanism)],
        )
        .await
        .and_then(|maybe| {
            if let Some(row) = maybe {
                let salt = row.try_get::<_, &[u8]>(0).map(Bytes::copy_from_slice)?;
                let iterations = row.try_get::<_, i32>(1)?;
                let stored_key = row.try_get::<_, &[u8]>(2).map(Bytes::copy_from_slice)?;
                let server_key = row.try_get::<_, &[u8]>(3).map(Bytes::copy_from_slice)?;

                Ok(Some(ScramCredential {
                    salt,
                    iterations,
                    stored_key,
                    server_key,
                }))
            } else {
                Ok(None)
            }
        })
        .inspect_err(|err| error!(?err, ?user, ?mechanism,))
    }

    pub(super) async fn cluster_id_storage(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    pub(super) async fn node_storage(&self) -> Result<i32> {
        Ok(self.node)
    }

    pub(super) async fn advertised_listener_storage(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    #[instrument(skip_all)]
    pub(super) async fn ping_storage(&self) -> Result<()> {
        let c = self.pool.get().await?;
        let _ = self.prepare_query(&c, "ping.sql", &[]).await?;
        Ok(())
    }
}
