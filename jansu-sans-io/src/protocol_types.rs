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
//! Kafka protocol primitive types: isolation levels, acknowledgements,
//! timestamps, compression, batch attributes, control batches,
//! configuration enums, and SCRAM mechanisms.

use crate::{de::Decoder, ser::Encoder, ByteSize, Error, Result};
use bytes::{Buf, Bytes, BytesMut};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, Cursor, Read},
    str::FromStr,
    time::{Duration, SystemTime},
};
use tracing::{debug, error};


#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
/// The fetch isolation level.
pub enum IsolationLevel {
    #[default]
    ReadUncommitted,
    ReadCommitted,
}

impl TryFrom<i8> for IsolationLevel {
    type Error = Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ReadUncommitted),
            1 => Ok(Self::ReadCommitted),
            _ => Err(Error::InvalidIsolationLevel(value)),
        }
    }
}

impl From<IsolationLevel> for i8 {
    fn from(value: IsolationLevel) -> Self {
        Self::from(&value)
    }
}

impl From<&IsolationLevel> for i8 {
    fn from(value: &IsolationLevel) -> Self {
        match value {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
/// Produce message acknowledgement.
pub enum Ack {
    None,
    Leader,
    FullIsr,
}

impl Ack {
    const FULL_ISR: i16 = -1;
    const NONE: i16 = 0;
    const LEADER: i16 = 1;
}

impl From<Ack> for i16 {
    fn from(value: Ack) -> Self {
        match value {
            Ack::FullIsr => Ack::FULL_ISR,
            Ack::None => Ack::NONE,
            Ack::Leader => Ack::LEADER,
        }
    }
}

impl TryFrom<i16> for Ack {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            Self::FULL_ISR => Ok(Self::FullIsr),
            Self::NONE => Ok(Self::None),
            Self::LEADER => Ok(Self::Leader),
            _ => Err(Error::InvalidAckValue(value)),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// The timestamp type.
pub enum TimestampType {
    #[default]
    CreateTime,
    LogAppendTime,
}

impl TimestampType {
    const TIMESTAMP_TYPE_BITMASK: i16 = 8;
}

impl From<i16> for TimestampType {
    fn from(value: i16) -> Self {
        if value & Self::TIMESTAMP_TYPE_BITMASK == Self::TIMESTAMP_TYPE_BITMASK {
            Self::LogAppendTime
        } else {
            Self::CreateTime
        }
    }
}

impl From<TimestampType> for i16 {
    fn from(value: TimestampType) -> Self {
        match value {
            TimestampType::CreateTime => 0,
            TimestampType::LogAppendTime => TimestampType::TIMESTAMP_TYPE_BITMASK,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// Kafka message compression types.
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl TryFrom<i16> for Compression {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value & 0b111i16 {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            otherwise => Err(Error::UnknownCompressionType(otherwise)),
        }
    }
}

impl From<Compression> for i16 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::Gzip => 1,
            Compression::Snappy => 2,
            Compression::Lz4 => 3,
            Compression::Zstd => 4,
        }
    }
}

impl Compression {
    pub(crate) fn inflator(&self, mut deflated: impl BufRead + 'static) -> Result<Box<dyn Read>> {
        match self {
            Compression::None => Ok(Box::new(deflated)),
            Compression::Gzip => Ok(Box::new(GzDecoder::new(deflated))),
            Compression::Snappy => {
                let mut input = vec![];
                _ = deflated.read_to_end(&mut input)?;
                debug!(?input);

                let mut decoder = snap::raw::Decoder::new();

                decoder
                    .decompress_vec(
                        // https://github.com/xerial/snappy-java/tree/master?tab=readme-ov-file#compatibility-notes
                        if input.starts_with(b"\x82SNAPPY\0") {
                            if let (b"\x82SNAPPY\0", remainder) = input.split_at(8) {
                                let (version, remainder) = remainder.split_at(4);
                                let version: i32 = version.try_into().map(i32::from_be_bytes)?;

                                let (compatible_version, remainder) = remainder.split_at(4);
                                let compatible_version: i32 =
                                    compatible_version.try_into().map(i32::from_be_bytes)?;

                                let (block_size, _) = remainder.split_at(4);
                                let block_size: i32 =
                                    block_size.try_into().map(i32::from_be_bytes)?;

                                debug!(version, compatible_version, block_size);
                            }

                            let skip_header = &input[20..];
                            debug!(?skip_header);
                            skip_header
                        } else {
                            &input[..]
                        },
                    )
                    .map_err(Into::into)
                    .map(Bytes::from)
                    .map(|bytes| bytes.reader())
                    .map(Box::new)
                    .map(|boxed| boxed as Box<dyn Read>)
                    .inspect_err(|err| error!(?err))
            }
            Compression::Lz4 => lz4::Decoder::new(deflated)
                .map(Box::new)
                .map(|boxed| boxed as Box<dyn Read>)
                .map_err(Into::into),
            Compression::Zstd => zstd::stream::read::Decoder::with_buffer(deflated)
                .map(Box::new)
                .map(|boxed| boxed as Box<dyn Read>)
                .map_err(Into::into),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// The produce batch attributes.
pub struct BatchAttribute {
    pub compression: Compression,
    pub timestamp: TimestampType,
    pub transaction: bool,
    pub control: bool,
    pub delete_horizon: bool,
}

impl BatchAttribute {
    const TRANSACTION_BITMASK: i16 = 16;
    const CONTROL_BITMASK: i16 = 32;
    const DELETE_HORIZON_BITMASK: i16 = 64;

    pub fn compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    pub fn timestamp(self, timestamp: TimestampType) -> Self {
        Self { timestamp, ..self }
    }

    pub fn transaction(self, transaction: bool) -> Self {
        Self {
            transaction,
            ..self
        }
    }

    pub fn control(self, control: bool) -> Self {
        Self { control, ..self }
    }

    pub fn delete_horizon(self, delete_horizon: bool) -> Self {
        Self {
            delete_horizon,
            ..self
        }
    }
}

impl From<BatchAttribute> for i16 {
    fn from(value: BatchAttribute) -> Self {
        let mut attributes = i16::from(value.compression);
        attributes |= i16::from(value.timestamp);

        if value.transaction {
            attributes |= BatchAttribute::TRANSACTION_BITMASK;
        }

        if value.control {
            attributes |= BatchAttribute::CONTROL_BITMASK;
        }

        if value.delete_horizon {
            attributes |= BatchAttribute::DELETE_HORIZON_BITMASK;
        }

        attributes
    }
}

impl TryFrom<i16> for BatchAttribute {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        Compression::try_from(value).map(|compression| {
            Self::default()
                .compression(compression)
                .timestamp(TimestampType::from(value))
                .transaction(value & Self::TRANSACTION_BITMASK == Self::TRANSACTION_BITMASK)
                .control(value & Self::CONTROL_BITMASK == Self::CONTROL_BITMASK)
                .delete_horizon(
                    value & Self::DELETE_HORIZON_BITMASK == Self::DELETE_HORIZON_BITMASK,
                )
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// The control batch marker.
pub struct ControlBatch {
    pub version: i16,
    pub r#type: i16,
}

impl ByteSize for ControlBatch {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.version) + size_of_val(&self.r#type))
    }
}

impl ControlBatch {
    const ABORT: i16 = 0;
    const COMMIT: i16 = 1;

    pub fn is_abort(&self) -> bool {
        self.r#type == Self::ABORT
    }

    pub fn is_commit(&self) -> bool {
        self.r#type == Self::COMMIT
    }

    pub fn version(self, version: i16) -> Self {
        Self { version, ..self }
    }

    pub fn commit(self) -> Self {
        Self {
            r#type: Self::COMMIT,
            ..self
        }
    }

    pub fn abort(self) -> Self {
        Self {
            r#type: Self::ABORT,
            ..self
        }
    }
}

impl TryFrom<Bytes> for ControlBatch {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut c = Cursor::new(value);
        let mut deserializer = Decoder::new(&mut c);
        Self::deserialize(&mut deserializer)
    }
}

impl TryFrom<ControlBatch> for Bytes {
    type Error = Error;

    fn try_from(value: ControlBatch) -> Result<Self, Self::Error> {
        let mut encoder = Encoder::new(BytesMut::with_capacity(value.size_in_bytes()?));
        value.serialize(&mut encoder)?;

        Ok(Bytes::from(encoder))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// An end transaction marker.
pub struct EndTransactionMarker {
    pub version: i16,
    pub coordinator_epoch: i32,
}

impl ByteSize for EndTransactionMarker {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.version) + size_of_val(&self.coordinator_epoch))
    }
}

impl TryFrom<Bytes> for EndTransactionMarker {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut c = Cursor::new(value);
        let mut deserializer = Decoder::new(&mut c);
        Self::deserialize(&mut deserializer)
    }
}

impl TryFrom<EndTransactionMarker> for Bytes {
    type Error = Error;

    fn try_from(value: EndTransactionMarker) -> Result<Self, Self::Error> {
        let mut encoder = Encoder::new(BytesMut::with_capacity(value.size_in_bytes()?));
        value.serialize(&mut encoder)?;
        Ok(Bytes::from(encoder))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The endpoint type.
pub enum EndpointType {
    #[default]
    Unknown,
    Broker,
    Controller,
}

impl From<i8> for EndpointType {
    fn from(value: i8) -> Self {
        match value {
            1 => Self::Broker,
            2 => Self::Controller,
            _ => Self::Unknown,
        }
    }
}

impl From<EndpointType> for i8 {
    fn from(value: EndpointType) -> Self {
        match value {
            EndpointType::Unknown => 0,
            EndpointType::Broker => 1,
            EndpointType::Controller => 2,
        }
    }
}

/// The coordinator type.
pub enum CoordinatorType {
    Group,
    Transaction,
    Share,
}

impl TryFrom<i8> for CoordinatorType {
    type Error = Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Group),
            1 => Ok(Self::Transaction),
            2 => Ok(Self::Share),
            otherwise => Err(Error::InvalidCoordinatorType(otherwise)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// What type of resource is the configuration describing.
pub enum ConfigResource {
    Group,
    ClientMetric,
    BrokerLogger,
    Broker,
    Topic,
    Unknown,
}

impl From<i8> for ConfigResource {
    fn from(value: i8) -> Self {
        match value {
            2 => Self::Topic,
            4 => Self::Broker,
            8 => Self::BrokerLogger,
            16 => Self::ClientMetric,
            32 => Self::Group,
            _ => Self::Unknown,
        }
    }
}

impl From<CoordinatorType> for i8 {
    fn from(value: CoordinatorType) -> Self {
        match value {
            CoordinatorType::Group => 0,
            CoordinatorType::Transaction => 1,
            CoordinatorType::Share => 2,
        }
    }
}

impl From<ConfigResource> for i8 {
    fn from(value: ConfigResource) -> Self {
        match value {
            ConfigResource::Unknown => 0,
            ConfigResource::Topic => 2,
            ConfigResource::Broker => 4,
            ConfigResource::BrokerLogger => 8,
            ConfigResource::ClientMetric => 16,
            ConfigResource::Group => 32,
        }
    }
}

impl From<ConfigResource> for i32 {
    fn from(value: ConfigResource) -> Self {
        match value {
            ConfigResource::Unknown => 0,
            ConfigResource::Topic => 2,
            ConfigResource::Broker => 4,
            ConfigResource::BrokerLogger => 8,
            ConfigResource::ClientMetric => 16,
            ConfigResource::Group => 32,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The type of configuration.
pub enum ConfigType {
    #[default]
    Unknown,
    Boolean,
    String,
    Int,
    Short,
    Long,
    Double,
    List,
    Class,
    Password,
}

impl From<i8> for ConfigType {
    fn from(value: i8) -> Self {
        match value {
            1 => Self::Boolean,
            2 => Self::String,
            3 => Self::Int,
            4 => Self::Short,
            5 => Self::Long,
            6 => Self::Double,
            7 => Self::List,
            8 => Self::Class,
            9 => Self::Password,
            _ => Self::Unknown,
        }
    }
}

impl From<ConfigType> for i8 {
    fn from(value: ConfigType) -> i8 {
        match value {
            ConfigType::Boolean => 1,
            ConfigType::String => 2,
            ConfigType::Int => 3,
            ConfigType::Short => 4,
            ConfigType::Long => 5,
            ConfigType::Double => 6,
            ConfigType::List => 7,
            ConfigType::Class => 8,
            ConfigType::Password => 9,
            ConfigType::Unknown => 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// From which source was the configuration provided.
pub enum ConfigSource {
    DynamicTopicConfig,
    DynamicBrokerLoggerConfig,
    DynamicBrokerConfig,
    DynamicDefaultBrokerConfig,
    DynamicClientMetricsConfig,
    DynamicGroupConfig,
    StaticBrokerConfig,
    DefaultConfig,
    Unknown,
}

impl From<i8> for ConfigSource {
    fn from(value: i8) -> Self {
        match value {
            1 => Self::DynamicTopicConfig,
            2 => Self::DynamicBrokerConfig,
            3 => Self::DynamicDefaultBrokerConfig,
            4 => Self::StaticBrokerConfig,
            5 => Self::DefaultConfig,
            6 => Self::DynamicBrokerLoggerConfig,
            7 => Self::DynamicClientMetricsConfig,
            8 => Self::DynamicGroupConfig,
            _ => Self::Unknown,
        }
    }
}

impl From<ConfigSource> for i8 {
    fn from(value: ConfigSource) -> i8 {
        match value {
            ConfigSource::DynamicTopicConfig => 1,
            ConfigSource::DynamicBrokerConfig => 2,
            ConfigSource::DynamicDefaultBrokerConfig => 3,
            ConfigSource::StaticBrokerConfig => 4,
            ConfigSource::DefaultConfig => 5,
            ConfigSource::DynamicBrokerLoggerConfig => 6,
            ConfigSource::DynamicClientMetricsConfig => 7,
            ConfigSource::DynamicGroupConfig => 8,
            ConfigSource::Unknown => 0,
        }
    }
}

/// The configuration operation type.
pub enum OpType {
    Set,
    Delete,
    Append,
    Subtract,
}

impl TryFrom<i8> for OpType {
    type Error = Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Set),
            1 => Ok(Self::Delete),
            2 => Ok(Self::Append),
            3 => Ok(Self::Subtract),
            otherwise => Err(Error::InvalidOpType(otherwise)),
        }
    }
}

impl From<OpType> for i8 {
    fn from(value: OpType) -> Self {
        match value {
            OpType::Set => 0,
            OpType::Delete => 1,
            OpType::Append => 2,
            OpType::Subtract => 3,
        }
    }
}

/// convert a Kafka timestamp into system time
pub fn to_system_time(timestamp: i64) -> Result<SystemTime> {
    u64::try_from(timestamp)
        .map(|timestamp| SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp))
        .map_err(Into::into)
}

/// convert system time into a kafka timestamp
pub fn to_timestamp(system_time: &SystemTime) -> Result<i64> {
    system_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(Into::into)
        .map(|since_epoch| since_epoch.as_millis())
        .and_then(|since_epoch| i64::try_from(since_epoch).map_err(Into::into))
}

/// List Offset
///
/// An enumeration of offset request types, with conversion from/to an i64 protocol representation.
///
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ListOffset {
    Earliest,
    Latest,
    Timestamp(SystemTime),
}

impl ListOffset {
    const EARLIEST_OFFSET: i64 = -2;
    const LATEST_OFFSET: i64 = -1;
}

impl TryFrom<ListOffset> for i64 {
    type Error = Error;

    fn try_from(value: ListOffset) -> Result<Self, Self::Error> {
        match value {
            ListOffset::Earliest => Ok(ListOffset::EARLIEST_OFFSET),
            ListOffset::Latest => Ok(ListOffset::LATEST_OFFSET),
            ListOffset::Timestamp(timestamp) => to_timestamp(&timestamp),
        }
    }
}

impl TryFrom<i64> for ListOffset {
    type Error = Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            Self::EARLIEST_OFFSET => Ok(Self::Earliest),
            Self::LATEST_OFFSET => Ok(Self::Latest),
            timestamp => to_system_time(timestamp).map(Self::Timestamp),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ScramMechanism {
    Scram256,
    Scram512,
}

impl FromStr for ScramMechanism {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "SCRAM-SHA-256" => Ok(ScramMechanism::Scram256),
            "SCRAM-SHA-512" => Ok(ScramMechanism::Scram512),
            otherwise => Err(Error::ParseScram(otherwise.to_string())),
        }
    }
}

impl TryFrom<i8> for ScramMechanism {
    type Error = Error;

    fn try_from(value: i8) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(ScramMechanism::Scram256),
            2 => Ok(ScramMechanism::Scram512),
            otherwise => Err(Error::UnknownScramMechanism(value)),
        }
    }
}

impl From<ScramMechanism> for i32 {
    fn from(value: ScramMechanism) -> Self {
        match value {
            ScramMechanism::Scram256 => 1,
            ScramMechanism::Scram512 => 2,
        }
    }
}

impl From<ScramMechanism> for i8 {
    fn from(value: ScramMechanism) -> Self {
        match value {
            ScramMechanism::Scram256 => 1,
            ScramMechanism::Scram512 => 2,
        }
    }
}

pub trait Encode {
    fn encode(&self) -> Result<Bytes>;
}

pub trait Decode: Sized {
    fn decode(encoded: &mut Bytes) -> Result<Self>;
}
