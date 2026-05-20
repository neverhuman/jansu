//! Virtual-topic name resolution helpers for the libSQL `Delegate`.

use super::*;

impl Delegate {
    pub(super) async fn topic_with_key<'a>(
        &self,
        topic: &'a str,
    ) -> Result<(&'a str, Option<&'a str>)> {
        if let Some((base, key)) = topic.split_once('/')
            && self
                .describe_config(base, ConfigResource::Topic, None)
                .await
                .map(|configs| {
                    configs
                        .configs
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .find_map(|config| {
                            if config.name == "jansu.virtual" {
                                config
                                    .value
                                    .as_deref()
                                    .and_then(|config| bool::from_str(config).ok())
                            } else {
                                None
                            }
                        })
                        .unwrap_or(false)
                })?
        {
            Ok((base, Some(key)))
        } else {
            Ok((topic, None))
        }
    }

    #[instrument(skip(self), ret)]
    pub(super) async fn base_topic<'a>(&self, topic: &'a str) -> Result<&'a str> {
        self.topic_with_key(topic).await.map(|(topic, _key)| topic)
    }

    #[instrument(skip(self), ret)]
    pub(super) async fn virtual_topic_id(&self, topic: &str, key: &str) -> Result<Uuid> {
        let uuid = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("tag:jansu.io,2026-04:virtual:{topic}:{key}",).as_bytes(),
        );

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let row = c
            .query_one(
                "virtual_topic_upsert.sql",
                (self.cluster.as_str(), topic, key, uuid.to_string()),
            )
            .await?;

        row.get_str(0)
            .map_err(Error::from)
            .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
            .inspect(|vt| debug!(%vt))
    }
}
