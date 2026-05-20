use super::sql::sql_lookup;
use super::*;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct Txn {
    pub(super) name: String,
    pub(super) producer_id: i64,
    pub(super) producer_epoch: i16,
    pub(super) status: TxnState,
}

impl TryFrom<Row> for Txn {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let name = row
            .get_value(0)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let producer_id = row
            .get_value(1)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let producer_epoch = row
            .get_value(2)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .map(|value| value as i16)
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let status = row
            .get_value(3)
            .map(|value| value.as_text().cloned())
            .map_err(Into::into)
            .and_then(|status| status.map_or(Ok(TxnState::Begin), TxnState::try_from))
            .inspect_err(|err| error!(?err))?;

        Ok(Self {
            name,
            producer_id,
            producer_epoch,
            status,
        })
    }
}

impl Engine {
    pub(super) async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &Connection,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        let mut rows = connection
            .query(
                &sql_lookup("producer_epoch_current_for_producer.sql")?,
                (self.cluster.as_str(), deflated.producer_id),
            )
            .await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            let current_epoch = row
                .get_value(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|value| value as i16)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

            let row = self
                .prepare_query_one(
                    connection,
                    &sql_lookup("producer_select_for_update.sql")?,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                    ),
                )
                .await
                .inspect_err(|err| {
                    error!(
                        self.cluster,
                        ?topition,
                        deflated.producer_id,
                        deflated.producer_epoch,
                        ?err
                    )
                })?;

            let sequence = row
                .get_value(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|value| value as i32)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

            debug!(
                self.cluster,
                ?topition,
                deflated.producer_id,
                deflated.producer_epoch,
                current_epoch,
                sequence,
            );

            let increment = idempotent_sequence_check(&current_epoch, &sequence, deflated)?;

            debug!(increment);

            assert_eq!(
                1,
                self.prepare_execute(
                    connection,
                    &sql_lookup("producer_detail_insert.sql")?,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                        increment,
                    ),
                )
                .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    pub(super) async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        tx: &Connection,
    ) -> Result<(Option<i64>, Option<i64>)> {
        debug!(?topition, ?tx);

        let mut rows = tx
            .query(
                &sql_lookup("watermark_select_no_update.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        if let Some(row) = rows
            .next()
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Ok((
                row.get_value(0)
                    .map(|value| value.as_integer().copied())
                    .inspect_err(|err| error!(?err))?,
                row.get_value(1)
                    .map(|value| value.as_integer().copied())
                    .inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    pub(super) async fn maybe_record_leader_epoch_boundary<'conn>(
        &self,
        topition: &Topition,
        epoch: i32,
        start_offset: i64,
        tx: &Transaction<'conn>,
    ) -> Result<()> {
        let mut rows = tx
            .query(
                &sql_lookup("leader_epoch_history.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        let mut current_epoch: Option<i32> = None;
        while let Some(row) = rows.next().await? {
            let row_epoch = row.get::<Option<i32>>(0)?.unwrap_or(0);
            current_epoch = Some(current_epoch.map_or(row_epoch, |current| current.max(row_epoch)));
        }

        if current_epoch.is_none_or(|current| epoch > current) {
            _ = self
                .prepare_execute(
                    tx,
                    &sql_lookup("leader_epoch_history_insert.sql")?,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        epoch,
                        start_offset,
                    ),
                )
                .await
                .inspect_err(|err| error!(?err, ?topition, epoch, start_offset))?;
        }

        Ok(())
    }
}
