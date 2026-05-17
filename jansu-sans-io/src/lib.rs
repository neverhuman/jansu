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
//
//! A Kafka protocol implementation that performs no I/O (it operates only on bytes)
//!
//! ## Design
//!
//! Apache Kafka defines each API message with a JSON message descriptor. Each descriptor
//! contains a list of fields together with their associated type. Each field can
//! include a range of versions for which it is valid, its encoding and whether it
//! includes tagged fields. Further background on the protocol and implementation
//! used are in the
//! [Apache Kafka protocol with serde, quote, syn and proc_macro2](https://blog.jansu.io/articles/serde-kafka-protocol)
//! article.
//!
//! Some useful starting points:
//!
//! - **Data Structures** - [`Frame`], [`Request`], [`Response`], [`Header`] and [`Body`].
//! - **Producing or fetching messages** - [`record`], [`ProduceRequest`] and [`FetchRequest`]
//!
//! ## Examples
//!
//! Encoding a [`CreateTopicsRequest`] request:
//!
//! ```
//! # use jansu_sans_io::Error;
//! # fn main() -> Result<(), Error> {
//! use jansu_sans_io::{
//!     ApiKey as _, CreateTopicsRequest, Frame, Header,
//!     create_topics_request::{CreatableTopic, CreatableTopicConfig},
//! };
//!
//! let header = Header::Request {
//!     api_key: CreateTopicsRequest::KEY,
//!     api_version: 7,
//!     correlation_id: 298,
//!     client_id: Some("adminclient-1".into()),
//! };
//!
//! let body = CreateTopicsRequest::default()
//!     .topics(Some(
//!         [CreatableTopic::default()
//!             .name("balances".into())
//!             .num_partitions(-1)
//!             .replication_factor(-1)
//!             .assignments(Some([].into()))
//!             .configs(Some(
//!                 [CreatableTopicConfig::default()
//!                     .name("cleanup.policy".into())
//!                     .value(Some("compact".into()))]
//!                 .into(),
//!             ))]
//!         .into(),
//!     ))
//!     .timeout_ms(30_000)
//!     .validate_only(Some(false))
//!     .into();
//!
//! let encoded = Frame::request(header, body)?;
//! # Ok(())
//! # }
//! ```
//!
//! Decoding a [`FindCoordinatorRequest`]:
//!
//! ```
//! # use jansu_sans_io::Error;
//! # fn main() -> Result<(), Error> {
//! use jansu_sans_io::{ApiKey as _, FindCoordinatorRequest, Frame, Header};
//!
//! let encoded = vec![
//!     0, 0, 0, 50, 0, 10, 0, 4, 0, 0, 0, 0, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99, 111,
//!     110, 115, 117, 109, 101, 114, 0, 0, 2, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
//!     109, 101, 114, 45, 103, 114, 111, 117, 112, 0,
//! ];
//!
//! assert_eq!(
//!     Frame {
//!         size: 50,
//!         header: Header::Request {
//!             api_key: FindCoordinatorRequest::KEY,
//!             api_version: 4,
//!             correlation_id: 0,
//!             client_id: Some("console-consumer".into())
//!         },
//!         body: FindCoordinatorRequest::default()
//!             .key(None)
//!             .key_type(Some(0))
//!             .coordinator_keys(Some(["test-consumer-group".into()].into()))
//!             .into()
//!     },
//!     Frame::request_from_bytes(&encoded[..])?
//! );
//! # Ok(())
//! # }
//! ```
//!
//! This crate includes a build time proc macro that generates simple Rust structures
//! containing all the fields present in the the Kafka message descriptor. Each generated
//! type implements [`serde::Serialize`] and [`serde::Deserialize`] traits. As part of
//! the generation phase [`MESSAGE_META`] is created, which is used by the actual message serializers.
//!
//! The Kafka protocol is implemented by [`ser::Encoder`] and [`de::Decoder`],
//! using [`MESSAGE_META`] to determine which fields are present, their serialization type
//! and whether any tagged fields can be present for a particular message version. Serializers
//! map from the [Serde Data Model](https://serde.rs/data-model.html) to the Kafka protocol or vice versa.

pub mod acl;
pub mod de;
pub mod primitive;
pub mod record;
pub mod resource;
pub mod ser;

use bytes::{Buf, BufMut, Bytes, BytesMut, TryGetError};
pub use de::Decoder;
use jansu_model::{MessageKind, MessageMeta};
use primitive::tagged::TagBuffer;
use record::deflated::Frame as RecordBatch;
pub use ser::Encoder;
use serde::{Deserialize, Serialize};
use std::{
    array::TryFromSliceError,
    collections::HashMap,
    env::VarError,
    fmt::{self, Display, Formatter},
    io, num, str, string,
    sync::{Arc, OnceLock},
    time::{SystemTime, SystemTimeError},
};
use tracing::{debug, instrument, warn};
use tracing_subscriber::filter::ParseError;

/// The null topic identifier.
pub const NULL_TOPIC_ID: [u8; 16] = [0; 16];

pub trait ByteSize {
    fn size_in_bytes(&self) -> Result<usize>;
}

pub trait MaximumAllocationSize {
    fn maximum_allocation_size(&self) -> Result<usize>;
}

impl<T> MaximumAllocationSize for T
where
    T: ByteSize,
{
    fn maximum_allocation_size(&self) -> Result<usize> {
        self.size_in_bytes()
    }
}

impl MaximumAllocationSize for Bytes {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i32>() + self.len())
    }
}

impl MaximumAllocationSize for f64 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<f64>())
    }
}

impl MaximumAllocationSize for i16 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i16>())
    }
}

impl MaximumAllocationSize for i32 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i32>())
    }
}

impl MaximumAllocationSize for i64 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i64>())
    }
}

impl MaximumAllocationSize for bool {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<bool>())
    }
}

impl MaximumAllocationSize for i8 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i8>())
    }
}

impl MaximumAllocationSize for String {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i16>() + self.len())
    }
}

impl MaximumAllocationSize for u16 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<u16>())
    }
}

impl MaximumAllocationSize for [u8; 16] {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<u8>() * 16)
    }
}

impl<T> MaximumAllocationSize for Vec<T>
where
    T: MaximumAllocationSize,
{
    fn maximum_allocation_size(&self) -> Result<usize> {
        self.iter()
            .map(MaximumAllocationSize::maximum_allocation_size)
            .collect::<Result<Vec<_>>>()
            .map(|elements| size_of::<i32>() + elements.iter().sum::<usize>())
    }
}

impl<T> MaximumAllocationSize for Option<T>
where
    T: MaximumAllocationSize,
{
    fn maximum_allocation_size(&self) -> Result<usize> {
        self.as_ref().map_or(
            Ok(size_of::<i32>()),
            MaximumAllocationSize::maximum_allocation_size,
        )
    }
}

#[derive(Debug)]
pub struct RootMessageMeta {
    pub(crate) requests: HashMap<i16, &'static MessageMeta>,
    pub(crate) responses: HashMap<i16, &'static MessageMeta>,
}

impl RootMessageMeta {
    fn new() -> Self {
        let (requests, responses) = MESSAGE_META.iter().fold(
            (HashMap::new(), HashMap::new()),
            |(mut requests, mut responses), (_, meta)| {
                match meta.message_kind {
                    MessageKind::Request => {
                        _ = requests.insert(meta.api_key, *meta);
                    }

                    MessageKind::Response => {
                        _ = responses.insert(meta.api_key, *meta);
                    }
                }

                (requests, responses)
            },
        );

        Self {
            requests,
            responses,
        }
    }

    pub fn messages() -> &'static RootMessageMeta {
        static MAPPING: OnceLock<RootMessageMeta> = OnceLock::new();
        MAPPING.get_or_init(RootMessageMeta::new)
    }

    #[must_use]
    pub const fn requests(&self) -> &HashMap<i16, &'static MessageMeta> {
        &self.requests
    }

    #[must_use]
    pub const fn responses(&self) -> &HashMap<i16, &'static MessageMeta> {
        &self.responses
    }
}

pub trait ApiKey {
    const KEY: i16;
}

pub trait ApiName {
    const NAME: &'static str;
}

/// All Kafka API requests implement this trait
pub trait Request:
    ApiKey + ApiName + fmt::Debug + Default + Into<Body> + Send + Sync + TryFrom<Body> + 'static
{
    type Response: Response;
}

/// All Kafka API responses implement this trait
pub trait Response:
    ApiKey + ApiName + fmt::Debug + Default + Into<Body> + Send + Sync + TryFrom<Body> + 'static
{
    type Request: Request;
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    ApiError(ErrorCode),
    EnvVar(VarError),
    FromUtf8(string::FromUtf8Error),
    InvalidAckValue(i16),
    InvalidCoordinatorType(i8),
    InvalidIsolationLevel(i8),
    InvalidOpType(i8),
    InvalidScramMechanism(i8),
    Io(Arc<io::Error>),
    Message(String),
    MessageMaxSizeExceeded(usize),
    NoSuchField(&'static str),
    NoSuchMessage(&'static str),
    NoSuchRequest(i16),
    NotAuthenticated,
    Overflow,
    ParseFilter(Arc<ParseError>),
    ParseScram(String),
    ResponseFrame,
    Snap(#[from] snap::Error),
    StringWithoutApiVersion,
    StringWithoutLength,
    SystemTime(SystemTimeError),
    JansuModel(jansu_model::Error),
    TryFromInt(#[from] num::TryFromIntError),
    TryFromSlice(#[from] TryFromSliceError),
    TryGet(Arc<TryGetError>),
    UnexpectedType(String),
    UnknownApiErrorCode(i16),
    UnknownCompressionType(i16),
    UnknownScramMechanism(i8),
    UnknownContainer,
    Utf8(str::Utf8Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::Message(e) => f.write_str(e),
            e => write!(f, "{e:?}"),
        }
    }
}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<TryGetError> for Error {
    fn from(value: TryGetError) -> Self {
        Self::TryGet(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<str::Utf8Error> for Error {
    fn from(value: str::Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(value: string::FromUtf8Error) -> Self {
        Self::FromUtf8(value)
    }
}

impl From<jansu_model::Error> for Error {
    fn from(value: jansu_model::Error) -> Self {
        Self::JansuModel(value)
    }
}

impl From<VarError> for Error {
    fn from(value: VarError) -> Self {
        Self::EnvVar(value)
    }
}

impl From<SystemTimeError> for Error {
    fn from(value: SystemTimeError) -> Self {
        Self::SystemTime(value)
    }
}

/// A Kafka API frame prefixed with its length, followed by a header and the message body.
///
/// # Examples
///
/// ## Encoding
///
/// Encoding a [`CreateTopicsRequest`] request:
///
/// ```
/// use jansu_sans_io::{
///     ApiKey as _, CreateTopicsRequest, Frame, Header,
///     create_topics_request::{CreatableTopic, CreatableTopicConfig},
/// };
///
/// let header = Header::Request {
///     api_key: CreateTopicsRequest::KEY,
///     api_version: 7,
///     correlation_id: 298,
///     client_id: Some("adminclient-1".into()),
/// };
///
/// let body = CreateTopicsRequest::default()
///     .topics(Some(
///         [CreatableTopic::default()
///             .name("balances".into())
///             .num_partitions(-1)
///             .replication_factor(-1)
///             .assignments(Some([].into()))
///             .configs(Some(
///                 [CreatableTopicConfig::default()
///                     .name("cleanup.policy".into())
///                     .value(Some("compact".into()))]
///                 .into(),
///             ))]
///         .into(),
///     ))
///     .timeout_ms(30_000)
///     .validate_only(Some(false))
///     .into();
///
/// let encoded = Frame::request(header, body).unwrap();
/// ```
///
/// ## Decoding
///
/// Decoding a [`FindCoordinatorRequest`]:
///
/// ```
/// use jansu_sans_io::{ApiKey as _, FindCoordinatorRequest, Frame, Header};
///
/// let encoded = vec![
///     0, 0, 0, 50, 0, 10, 0, 4, 0, 0, 0, 0, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99, 111,
///     110, 115, 117, 109, 101, 114, 0, 0, 2, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
///     109, 101, 114, 45, 103, 114, 111, 117, 112, 0,
/// ];
///
/// assert_eq!(
///     Frame {
///         size: 50,
///         header: Header::Request {
///             api_key: FindCoordinatorRequest::KEY,
///             api_version: 4,
///             correlation_id: 0,
///             client_id: Some("console-consumer".into())
///         },
///         body: FindCoordinatorRequest::default()
///             .key(None)
///             .key_type(Some(0))
///             .coordinator_keys(Some(["test-consumer-group".into()].into()))
///             .into()
///     },
///     Frame::request_from_bytes(&encoded[..]).unwrap()
/// );
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    /// The size of this frame.
    pub size: i32,

    /// The frame header.
    pub header: Header,

    /// The frame body.
    pub body: Body,
}

impl MaximumAllocationSize for Frame {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(self.size.maximum_allocation_size()?
            + self.header.maximum_allocation_size()?
            + self.body.maximum_allocation_size()?)
    }
}

fn fix_length(mut encoded: BytesMut) -> Result<Bytes> {
    let mut sz = encoded.split_to(size_of::<i32>());
    sz.clear();
    sz.put_i32(i32::try_from(encoded.len())?);
    sz.unsplit(encoded);
    sz.truncate(sz.len());
    Ok(sz.freeze())
}

impl Frame {
    fn elapsed_millis(start: SystemTime) -> u64 {
        start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64)
    }

    /// serialize an API request into a frame of bytes
    #[instrument(skip_all)]
    pub fn request(header: Header, body: Body) -> Result<Bytes> {
        let frame = Frame {
            size: 0,
            header,
            body,
        };

        let mut serializer = Encoder::request(
            frame
                .maximum_allocation_size()
                .map(BytesMut::with_capacity)?,
        );
        frame.serialize(&mut serializer)?;
        fix_length(BytesMut::from(serializer))
    }

    /// deserialize bytes into an API request frame
    #[instrument(skip_all)]
    pub fn request_from_bytes(encoded: impl Buf) -> Result<Frame> {
        let start = SystemTime::now();

        let mut reader = encoded.reader();
        let mut deserializer = Decoder::request(&mut reader);
        Frame::deserialize(&mut deserializer)
            .inspect(|frame| debug!(?frame, elapsed_millis = Self::elapsed_millis(start)))
    }

    /// serialize an API response into a frame of bytes
    #[instrument(skip(header, body))]
    pub fn response(header: Header, body: Body, api_key: i16, api_version: i16) -> Result<Bytes> {
        let frame = Frame {
            size: 0,
            header,
            body,
        };

        let mut encoder = Encoder::response(
            frame
                .maximum_allocation_size()
                .map(BytesMut::with_capacity)?,
            api_key,
            api_version,
        );
        frame.serialize(&mut encoder)?;
        fix_length(BytesMut::from(encoder))
    }

    /// deserialize bytes into an API response frame
    #[instrument(skip_all)]
    pub fn response_from_bytes(bytes: impl Buf, api_key: i16, api_version: i16) -> Result<Frame> {
        let start = SystemTime::now();

        let mut reader = bytes.reader();
        let mut deserializer = Decoder::response(&mut reader, api_key, api_version);
        Frame::deserialize(&mut deserializer)
            .inspect(|encoded| debug!(elapsed_millis = Self::elapsed_millis(start)))
    }

    /// API request key
    pub fn api_key(&self) -> Result<i16> {
        if let Header::Request { api_key, .. } = self.header {
            Ok(api_key)
        } else {
            Err(Error::ResponseFrame)
        }
    }

    /// API name
    pub fn api_name(&self) -> &str {
        self.body.api_name()
    }

    /// API request version
    pub fn api_version(&self) -> Result<i16> {
        if let Header::Request { api_version, .. } = self.header {
            Ok(api_version)
        } else {
            Err(Error::ResponseFrame)
        }
    }

    /// API request/response correlation ID
    pub fn correlation_id(&self) -> Result<i32> {
        match self.header {
            Header::Request { correlation_id, .. } | Header::Response { correlation_id } => {
                Ok(correlation_id)
            }
        }
    }

    /// API request client ID
    pub fn client_id(&self) -> Result<Option<&str>> {
        if let Header::Request { ref client_id, .. } = self.header {
            Ok(client_id.as_deref())
        } else {
            Err(Error::ResponseFrame)
        }
    }
}

/// A Kafka API request or response header.
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "HeaderMezzanine")]
#[serde(into = "HeaderMezzanine")]
pub enum Header {
    /// An API request header.
    Request {
        /// The API key being used for this request.
        api_key: i16,

        /// The API version being used for this request.
        api_version: i16,

        /// The correlation ID that should be used by the response to this request.
        correlation_id: i32,

        /// An optional client ID.
        client_id: Option<String>,
    },

    /// An API Response header.
    Response {
        /// The correlation ID for the corresponding request.
        correlation_id: i32,
    },
}

impl MaximumAllocationSize for Header {
    fn maximum_allocation_size(&self) -> Result<usize> {
        match self {
            Self::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            } => Ok(api_key.maximum_allocation_size()?
                + api_version.maximum_allocation_size()?
                + correlation_id.maximum_allocation_size()?
                + client_id.maximum_allocation_size()?),
            Self::Response { correlation_id } => correlation_id.maximum_allocation_size(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
pub(crate) enum HeaderMezzanine {
    Request {
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
        tag_buffer: Option<TagBuffer>,
    },
    Response {
        correlation_id: i32,
        tag_buffer: Option<TagBuffer>,
    },
}

impl TryFrom<HeaderMezzanine> for Header {
    type Error = Error;

    fn try_from(value: HeaderMezzanine) -> Result<Self, Self::Error> {
        debug!(?value);

        match value {
            HeaderMezzanine::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
                ..
            } => Ok(Self::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            }),

            HeaderMezzanine::Response { correlation_id, .. } => {
                Ok(Self::Response { correlation_id })
            }
        }
    }
}

impl From<Header> for HeaderMezzanine {
    fn from(value: Header) -> Self {
        debug!("value: {value:?}");

        match value {
            Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            } => HeaderMezzanine::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
                tag_buffer: Some(TagBuffer([].into())),
            },

            Header::Response { correlation_id } => HeaderMezzanine::Response {
                correlation_id,
                tag_buffer: Some(TagBuffer([].into())),
            },
        }
    }
}

mod error_code;
pub use error_code::ErrorCode;

mod protocol_types;
pub use protocol_types::{
    Ack, BatchAttribute, Compression, ConfigResource, ConfigSource, ConfigType, ControlBatch,
    CoordinatorType, Decode, Encode, EndTransactionMarker, EndpointType, IsolationLevel,
    ListOffset, OpType, ScramMechanism, TimestampType,
};
pub use protocol_types::{to_system_time, to_timestamp};

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use super::*;

    #[test]
    fn frame_elapsed_millis() {
        let pause = 6;
        let now = SystemTime::now();
        sleep(Duration::from_millis(pause));

        assert!(Frame::elapsed_millis(now) >= pause);
    }

    #[test]
    fn batch_attribute() {
        assert_eq!(0, i16::from(BatchAttribute::default()));
        assert_eq!(
            0,
            i16::from(BatchAttribute::default().compression(Compression::None))
        );
        assert_eq!(
            1,
            i16::from(BatchAttribute::default().compression(Compression::Gzip))
        );
        assert_eq!(
            2,
            i16::from(BatchAttribute::default().compression(Compression::Snappy))
        );
        assert_eq!(
            3,
            i16::from(BatchAttribute::default().compression(Compression::Lz4))
        );
        assert_eq!(
            4,
            i16::from(BatchAttribute::default().compression(Compression::Zstd))
        );
        assert_eq!(
            8,
            i16::from(BatchAttribute::default().timestamp(TimestampType::LogAppendTime))
        );
        assert_eq!(16, i16::from(BatchAttribute::default().transaction(true)));
        assert_eq!(32, i16::from(BatchAttribute::default().control(true)));
        assert_eq!(
            64,
            i16::from(BatchAttribute::default().delete_horizon(true))
        );
    }

    #[test]
    fn reset_compression() -> Result<()> {
        let mut attribute: i16 = BatchAttribute::default()
            .compression(Compression::Gzip)
            .into();

        assert_eq!(
            Compression::Gzip,
            BatchAttribute::try_from(attribute).map(|attribute| attribute.compression)?
        );

        attribute = BatchAttribute::try_from(attribute)
            .map(|attribute| attribute.compression(Compression::None).into())?;

        assert_eq!(
            Compression::None,
            BatchAttribute::try_from(attribute).map(|attribute| attribute.compression)?
        );

        Ok(())
    }
}

include!(concat!(env!("OUT_DIR"), "/generate.rs"));
