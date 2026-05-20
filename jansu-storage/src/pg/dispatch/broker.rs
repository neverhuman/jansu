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

//! Broker registration, cluster identity, maintenance and SCRAM dispatch.
//!
//! Inherent `Postgres` methods backing the `Storage` trait implementation.

use super::*;

impl Postgres {
    pub(crate) async fn register_broker_dispatch(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        debug!(cluster = self.cluster, ?broker_registration);

        let c = self.connection().await?;

        _ = self
            .prepare_execute(
                &c,
                "register_broker.sql",
                &[&broker_registration.cluster_id],
            )
            .await
            .inspect(|n| debug!(cluster = self.cluster, n))?;

        Ok(())
    }

    pub(crate) async fn brokers_dispatch(&self) -> Result<Vec<DescribeClusterBroker>> {
        debug!(cluster = self.cluster);

        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    pub(crate) async fn maintain_dispatch(&self, now: SystemTime) -> Result<()> {
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

    pub(crate) async fn delete_user_scram_credential_dispatch(
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

    pub(crate) async fn upsert_user_scram_credential_dispatch(
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

    pub(crate) async fn user_scram_credential_dispatch(
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

    pub(crate) async fn cluster_id_dispatch(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    pub(crate) async fn node_dispatch(&self) -> Result<i32> {
        Ok(self.node)
    }

    pub(crate) async fn advertised_listener_dispatch(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    pub(crate) async fn ping_dispatch(&self) -> Result<()> {
        let c = self.pool.get().await?;
        let _ = self.prepare_query(&c, "ping.sql", &[]).await?;
        Ok(())
    }
}
