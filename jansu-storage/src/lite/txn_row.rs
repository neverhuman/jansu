//! Row-decoded transaction record used by libSQL transaction completion logic.

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
        let name = row.get::<String>(0).inspect_err(|err| error!(?err))?;
        let producer_id = row.get::<i64>(1).inspect_err(|err| error!(?err))?;
        let producer_epoch = row.get::<i32>(2).inspect_err(|err| error!(?err))? as i16;
        let status = row
            .get::<Option<String>>(3)
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
