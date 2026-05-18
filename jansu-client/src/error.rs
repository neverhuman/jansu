use std::{error, fmt, io, sync::Arc};

use deadpool::managed::{BuildError, PoolError};
use tokio::task::JoinError;
use tracing_subscriber::filter::ParseError;
use url::Url;

/// Client Errors
#[derive(thiserror::Error, Clone, Debug)]
pub enum Error {
    DeadPoolBuild(#[from] BuildError),
    Io(Arc<io::Error>),
    Join(Arc<JoinError>),
    Message(String),
    ParseFilter(Arc<ParseError>),
    ParseUrl(#[from] url::ParseError),
    Pool(Arc<Box<dyn error::Error + Send + Sync>>),
    Protocol(#[from] jansu_sans_io::Error),
    Service(#[from] jansu_service::Error),
    UnknownApiKey(i16),
    UnknownHost(Url),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Join(Arc::new(value))
    }
}

impl<E> From<PoolError<E>> for Error
where
    E: error::Error + Send + Sync + 'static,
{
    fn from(value: PoolError<E>) -> Self {
        Self::Pool(Arc::new(Box::new(value)))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}
