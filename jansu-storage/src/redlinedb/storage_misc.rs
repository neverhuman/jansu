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

        let mut connection = self.connection().await?;
        let s = sql("consumer_offset_delete_expired.sql").map_err(Error::from)?;
        let _ = connection
            .execute(&s, (self.cluster.as_str(), Value::from(now)))
            .map_err(Error::from)?;

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
        let mut pc = self.connection().await?;

        let s = sql("scram_credential_delete.sql").map_err(Error::from)?;
        let _ = pc.execute(&s, (self.cluster.as_str(), user, i32::from(mechanism)))
            .inspect_err(|err| error!(?err))
            .map_err(Error::from)?;

        Ok(()).inspect(|_| {
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
        let mut pc = self.connection().await?;

        let s = sql("scram_credential_insert.sql").map_err(Error::from)?;
        let _ = pc.execute(
            &s,
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
        .inspect_err(|err| error!(?err))
        .map_err(Error::from)?;

        Ok(()).inspect(|_| {
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
        let mut pc = self.connection().await?;

        let s = sql("scram_credential_select.sql").map_err(Error::from)?;
        let mut rows = pc
            .query(&s, (self.cluster.as_str(), user, i32::from(mechanism)))
            .map_err(Error::from)
            .inspect_err(|err| error!(?err))?;

        match rows.step().map_err(Error::from)? {
            Step::Row(row) => {
                let salt = row.get::<Vec<u8>>(0).map_err(Error::from).map(Bytes::from)?;
                let iterations = row.get::<i32>(1).map_err(Error::from)?;
                let stored_key = row.get::<Vec<u8>>(2).map_err(Error::from).map(Bytes::from)?;
                let server_key = row.get::<Vec<u8>>(3).map_err(Error::from).map(Bytes::from)?;

                Ok(Some(ScramCredential {
                    salt,
                    iterations,
                    stored_key,
                    server_key,
                }))
            }
            Step::Done => Ok(None),
        }
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
        let mut c = self.connection().await?;
        let s = sql("ping.sql").map_err(Error::from)?;
        let _ = c.query(&s, ()).map_err(Error::from)?;
        DELEGATE_REQUEST_DURATION
            .record(elapsed_millis(start), &[KeyValue::new("operation", "ping")]);
        Ok(())
    }
}
