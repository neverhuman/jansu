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

//! Broker registration and cluster identity operations for the SlateDB engine.

use jansu_sans_io::{ScramMechanism, describe_cluster_response::DescribeClusterBroker};
use tracing::debug;

use crate::{BrokerRegistrationRequest, Error, Result, ScramCredential};

use super::super::engine::Engine;
use super::super::types::{BrokerInfo, Brokers};

impl Engine {
    pub(in crate::slate) async fn register_broker_op(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        debug!(?broker_registration);

        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut brokers: Brokers = self.load_metadata(&tx, Self::BROKERS).await?;

        // Persist broker info to storage
        // NOTE: This is stored permanently - no cleanup mechanism exists yet
        let broker_info = BrokerInfo {
            broker_id: self.node,
            host: self
                .advertised_listener
                .host_str()
                .unwrap_or("0.0.0.0")
                .into(),
            port: self.advertised_listener.port().unwrap_or(9092).into(),
            rack: broker_registration.rack,
        };

        _ = brokers.insert(self.node, broker_info);
        self.save_metadata(&tx, Self::BROKERS, &brokers)?;

        tx.commit().await.map_err(Error::from)?;

        Ok(())
    }

    pub(in crate::slate) async fn brokers_op(&self) -> Result<Vec<DescribeClusterBroker>> {
        let stored_brokers = self
            .db
            .get(Self::BROKERS)
            .await
            .map_err(Error::from)
            .and_then(|brokers| {
                brokers.map_or(Ok(Brokers::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        if stored_brokers.is_empty() {
            // Return self as the only broker if no registrations yet
            let broker_id = self.node;
            let host = self
                .advertised_listener
                .host_str()
                .unwrap_or("0.0.0.0")
                .into();
            let port = self.advertised_listener.port().unwrap_or(9092).into();

            Ok(vec![
                DescribeClusterBroker::default()
                    .broker_id(broker_id)
                    .host(host)
                    .port(port)
                    .rack(None),
            ])
        } else {
            Ok(stored_brokers
                .values()
                .map(|info| {
                    DescribeClusterBroker::default()
                        .broker_id(info.broker_id)
                        .host(info.host.clone())
                        .port(info.port)
                        .rack(info.rack.clone())
                })
                .collect())
        }
    }

    pub(in crate::slate) async fn cluster_id_op(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    pub(in crate::slate) async fn node_op(&self) -> Result<i32> {
        Ok(self.node)
    }

    pub(in crate::slate) async fn advertised_listener_op(&self) -> Result<url::Url> {
        Ok(self.advertised_listener.clone())
    }

    pub(in crate::slate) async fn delete_user_scram_credential_op(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Ok(())
    }

    pub(in crate::slate) async fn upsert_user_scram_credential_op(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Ok(())
    }

    pub(in crate::slate) async fn user_scram_credential_op(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Ok(None)
    }

    pub(in crate::slate) async fn ping_op(&self) -> Result<()> {
        // Verify connectivity by attempting a simple read operation
        let _ = self.db.get(Self::BROKERS).await?;
        Ok(())
    }
}
