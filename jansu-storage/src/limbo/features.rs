use super::*;

impl Engine {
    pub(super) async fn impl_register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        debug!(?broker_registration);

        let connection = self.connection().await?;

        self.prepare_execute(
            &connection,
            &sql_lookup("register_broker.sql")?,
            &[broker_registration.cluster_id],
        )
        .await
        .map_err(Into::into)
        .and(Ok(()))
    }

    pub(super) async fn impl_brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
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
    pub(super) async fn impl_maintain(&self, _now: SystemTime) -> Result<()> {
        Ok(())
    }

    pub(super) async fn impl_cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    pub(super) async fn impl_node(&self) -> Result<i32> {
        Ok(self.node)
    }

    pub(super) async fn impl_advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    pub(super) async fn impl_delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        todo!()
    }

    pub(super) async fn impl_upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        todo!()
    }

    pub(super) async fn impl_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        todo!()
    }

    pub(super) async fn impl_ping(&self) -> Result<()> {
        let c = self.connection().await?;
        let _ = c.query("ping.sql", ()).await?;
        Ok(())
    }
}
