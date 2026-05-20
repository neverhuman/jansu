//! `Storage` broker, cluster-identity, and SCRAM operations for the libSQL `Delegate`.

use super::*;

impl Delegate {
    pub(super) async fn register_broker_inner(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        let start = SystemTime::now();

        debug!(?broker_registration);

        let connection = self.connection().await?;

        connection
            .execute(
                "register_broker.sql",
                &[broker_registration.cluster_id.as_str()],
            )
            .await
            .map_err(Into::into)
            .and(Ok(()))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "register_broker")],
                )
            })
    }

    pub(super) async fn brokers_inner(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();

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
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "brokers")],
            )
        })
    }

    pub(super) async fn cluster_id_inner(&self) -> Result<String> {
        let start = SystemTime::now();

        Ok(self.cluster.clone()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "cluster_id")],
            )
        })
    }

    pub(super) async fn node_inner(&self) -> Result<i32> {
        let start = SystemTime::now();

        Ok(self.node).inspect(|_| {
            DELEGATE_REQUEST_DURATION
                .record(elapsed_millis(start), &[KeyValue::new("operation", "node")])
        })
    }

    pub(super) async fn advertised_listener_inner(&self) -> Result<Url> {
        let start = SystemTime::now();

        Ok(self.advertised_listener.clone()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "advertised_listener")],
            )
        })
    }

    pub(super) async fn delete_user_scram_credential_inner(
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
    pub(super) async fn upsert_user_scram_credential_inner(
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
    pub(super) async fn user_scram_credential_inner(
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
    pub(super) async fn ping_inner(&self) -> Result<()> {
        let start = SystemTime::now();
        let c = self.pool.get().await?;
        let _ = c.query("ping.sql", ()).await?;
        DELEGATE_REQUEST_DURATION
            .record(elapsed_millis(start), &[KeyValue::new("operation", "ping")]);
        Ok(())
    }
}
