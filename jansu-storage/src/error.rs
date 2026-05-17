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

//! Storage error type and conversions.

use bytes::{Bytes, TryGetError};
#[cfg(feature = "postgres")]
use deadpool::managed::PoolError;
use glob::{GlobError, PatternError};
use governor::InsufficientCapacity;
use jansu_sans_io::{Body, ErrorCode, add_partitions_to_txn_request::AddPartitionsToTxnRequest};
#[cfg(feature = "postgres")]
use std::error;
use std::{
    array::TryFromSliceError,
    ffi::OsString,
    fmt::{self, Display, Formatter},
    io,
    num::{ParseIntError, TryFromIntError},
    result,
    sync::{Arc, PoisonError},
    time::SystemTimeError,
};
use tokio::sync::AcquireError;
use tracing_subscriber::filter::ParseError;
use url::Url;

use crate::{Response, Topition};

/// Storage Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    Acquire(Arc<AcquireError>),
    Cancelled,

    Api(ErrorCode),

    ChronoParse(#[from] chrono::ParseError),

    #[cfg(feature = "postgres")]
    DeadPoolBuild(#[from] deadpool::managed::BuildError),

    Decode(Bytes),

    FeatureNotEnabled {
        feature: String,
        message: String,
    },

    FeatureUnsupported {
        backend: &'static str,
        feature: String,
    },

    Glob(Arc<GlobError>),
    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),

    LessThanBaseOffset {
        offset: i64,
        base_offset: i64,
    },
    LessThanLastOffset {
        offset: i64,
        last_offset: Option<i64>,
    },

    LessThanMaxTime {
        time: i64,
        max_time: Option<i64>,
    },
    LessThanMinTime {
        time: i64,
        min_time: Option<i64>,
    },
    Message(String),
    NoSuchEntry {
        nth: u32,
    },
    NoSuchOffset(i64),
    OsString(OsString),

    #[cfg(any(feature = "dynostore", feature = "slatedb"))]
    ObjectStore(Arc<object_store::Error>),

    ParseFilter(Arc<ParseError>),
    Pattern(Arc<PatternError>),
    ParseInt(#[from] ParseIntError),
    PhantomCached(),
    Poison,

    #[cfg(feature = "postgres")]
    Pool(Arc<Box<dyn error::Error + Send + Sync>>),

    #[cfg(feature = "slatedb")]
    Postcard(#[from] postcard::Error),

    Regex(#[from] regex::Error),

    #[cfg(feature = "redlinedb")]
    RedlineDb(Arc<::redlinedb::Error>),

    SansIo(#[from] jansu_sans_io::Error),

    Schema(Arc<jansu_schema::Error>),

    Rustls(#[from] rustls::Error),

    SegmentEmpty(Topition),

    SegmentMissing {
        topition: Topition,
        offset: Option<i64>,
    },

    SerdeJson(Arc<serde_json::Error>),

    #[cfg(feature = "slatedb")]
    Slate(Arc<slatedb::Error>),

    SystemTime(#[from] SystemTimeError),

    #[cfg(feature = "postgres")]
    TokioPostgres(Arc<tokio_postgres::error::Error>),
    TryFromInt(#[from] TryFromIntError),
    TryFromSlice(#[from] TryFromSliceError),

    TryGet(Arc<TryGetError>),

    UnexpectedBody(Box<Body>),

    UnexpectedServiceResponse(Box<Response>),

    UnknownCacheKey(String),

    UnsupportedStorageUrl(Url),
    UnexpectedAddPartitionsToTxnRequest(Box<AddPartitionsToTxnRequest>),
    Url(#[from] url::ParseError),
    UnknownTxnState(String),

    Uuid(#[from] uuid::Error),

    UnableToSend,
    OneshotRecv,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<TryGetError> for Error {
    fn from(value: TryGetError) -> Self {
        Self::TryGet(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<AcquireError> for Error {
    fn from(value: AcquireError) -> Self {
        Self::Acquire(Arc::new(value))
    }
}

#[cfg(feature = "postgres")]
impl<E> From<PoolError<E>> for Error
where
    E: error::Error + Send + Sync + 'static,
{
    fn from(value: PoolError<E>) -> Self {
        Self::Pool(Arc::new(Box::new(value)))
    }
}

#[cfg(feature = "redlinedb")]
impl From<::redlinedb::Error> for Error {
    fn from(value: ::redlinedb::Error) -> Self {
        Self::RedlineDb(Arc::new(value))
    }
}

#[cfg(feature = "slatedb")]
impl From<slatedb::Error> for Error {
    fn from(value: slatedb::Error) -> Self {
        Self::Slate(Arc::new(value))
    }
}

impl From<GlobError> for Error {
    fn from(value: GlobError) -> Self {
        Self::Glob(Arc::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl From<Arc<object_store::Error>> for Error {
    fn from(value: Arc<object_store::Error>) -> Self {
        Self::ObjectStore(value)
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl From<object_store::Error> for Error {
    fn from(value: object_store::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<PatternError> for Error {
    fn from(value: PatternError) -> Self {
        Self::Pattern(Arc::new(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<Arc<serde_json::Error>> for Error {
    fn from(value: Arc<serde_json::Error>) -> Self {
        Self::SerdeJson(value)
    }
}

#[cfg(feature = "postgres")]
impl From<tokio_postgres::error::Error> for Error {
    fn from(value: tokio_postgres::error::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

#[cfg(feature = "postgres")]
impl From<Arc<tokio_postgres::error::Error>> for Error {
    fn from(value: Arc<tokio_postgres::error::Error>) -> Self {
        Self::TokioPostgres(value)
    }
}

impl From<jansu_schema::Error> for Error {
    fn from(value: jansu_schema::Error) -> Self {
        if let jansu_schema::Error::Api(error_code) = value {
            Self::Api(error_code)
        } else {
            Self::Schema(Arc::new(value))
        }
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;
