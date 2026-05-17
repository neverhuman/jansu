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

impl Delegate {
    pub(super) async fn delegate_maintain(&self, now: SystemTime) -> Result<()> {
        self.vacuum_into().await?;

        let Ok(_permit) = self.maintenance.try_acquire() else {
            return Ok(());
        };

        let start = SystemTime::now();

        let deleted = self.policy_delete(now).await?;
        debug!(deleted);

        let connection = self.pool.get().await?;
        let expired = connection
            .execute(
                "consumer_offset_delete_expired.sql",
                (self.cluster.as_str(), RedlineTimestamp::from(now)),
            )
            .await?;
        debug!(expired);

        let compacted = self.policy_compact().await?;
        debug!(compacted);

        Ok(()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "maintain")],
            )
        })
    }

    pub(super) async fn delegate_cluster_id(&self) -> Result<String> {
        let start = SystemTime::now();

        Ok(self.cluster.clone()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "cluster_id")],
            )
        })
    }

    pub(super) async fn delegate_node(&self) -> Result<i32> {
        let start = SystemTime::now();

        Ok(self.node).inspect(|_| {
            DELEGATE_REQUEST_DURATION
                .record(elapsed_millis(start), &[KeyValue::new("operation", "node")])
        })
    }

    pub(super) async fn delegate_advertised_listener(&self) -> Result<Url> {
        let start = SystemTime::now();

        Ok(self.advertised_listener.clone()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "advertised_listener")],
            )
        })
    }

    pub(super) async fn delegate_delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let start = SystemTime::now();
        let pc = self.connection().await?;

        pc.execute(
            "scram_credential_delete.sql",
            (self.cluster.as_str(), user, i32::from(mechanism)),
        )
        .await
        .inspect_err(|err| error!(?err))
        .map_err(Into::into)
        .and(Ok(()))
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_user_scram_credential")],
            )
        })
    }

    #[instrument(skip_all)]
    pub(super) async fn delegate_upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let start = SystemTime::now();
        let pc = self.connection().await?;

        pc.execute(
            "scram_credential_insert.sql",
            (
                self.cluster.as_str(),
                user,
                i32::from(mechanism),
                &credential.salt[..],
                credential.iterations,
                &credential.stored_key[..],
                &credential.server_key[..],
            ),
        )
        .await
        .inspect_err(|err| error!(?err))
        .map_err(Into::into)
        .and(Ok(()))
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "upsert_user_scram_credential")],
            )
        })
    }

    #[instrument(skip_all)]
    pub(super) async fn delegate_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let start = SystemTime::now();
        let pc = self.connection().await?;

        pc.query_opt(
            "scram_credential_select.sql",
            (self.cluster.as_str(), user, i32::from(mechanism)),
        )
        .await
        .inspect_err(|err| error!(?err))
        .map_err(Into::into)
        .and_then(|row| {
            if let Some(row) = row {
                let salt = row.get::<Vec<u8>>(0).map(Bytes::from)?;
                let iterations = row.get::<i32>(1)?;
                let stored_key = row.get::<Vec<u8>>(2).map(Bytes::from)?;
                let server_key = row.get::<Vec<u8>>(3).map(Bytes::from)?;

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
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "upsert_user_scram_credential")],
            )
        })
    }

    #[instrument(skip_all)]
    pub(super) async fn delegate_ping(&self) -> Result<()> {
        let start = SystemTime::now();
        let c = self.pool.get().await?;
        let _ = c.query("ping.sql", ()).await?;
        DELEGATE_REQUEST_DURATION
            .record(elapsed_millis(start), &[KeyValue::new("operation", "ping")]);
        Ok(())
    }
}
