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
//! Inherent `DynoStore` methods backing the `Storage` trait implementation.

use super::*;

impl DynoStore {
    pub(crate) async fn register_broker_dispatch(
        &self,
        _broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn brokers_dispatch(&self) -> Result<Vec<DescribeClusterBroker>> {
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
        if let Some(ref lake) = self.lake {
            return lake
                .maintain()
                .await
                .inspect(|maintain| debug!(?maintain))
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into);
        }

        let prefix = Path::from(format!("clusters/{}/groups/consumers/", self.cluster));
        let mut list_stream = self.object_store.list(Some(&prefix));

        while let Some(meta) = list_stream.next().await.transpose()? {
            let location = meta.location;
            let location_str = location.to_string();

            if !location_str.contains("/offsets/") {
                continue;
            }

            let record = match self.object_store.get(&location).await {
                Ok(get_result) => {
                    get_result
                        .bytes()
                        .await
                        .map_err(Error::from)
                        .and_then(|encoded| {
                            serde_json::from_slice::<OffsetFetchRecord>(&encoded[..])
                                .map_err(Error::from)
                        })
                }

                Err(object_store::Error::NotFound { .. }) => continue,

                Err(error) => {
                    debug!(?error, ?location);
                    continue;
                }
            }?;

            if record.expired(now) {
                self.object_store
                    .delete(&location)
                    .await
                    .inspect(|outcome| {
                        debug!(?location, ?outcome);
                    })?;
            }
        }

        Ok(())
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

    pub(crate) async fn delete_user_scram_credential_dispatch(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn upsert_user_scram_credential_dispatch(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn user_scram_credential_dispatch(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Ok(None)
    }

    pub(crate) async fn ping_dispatch(&self) -> Result<()> {
        // Verify connectivity by listing objects at the root
        let _ = self.object_store.list(Some(&Path::from("/"))).next().await;
        Ok(())
    }
}
