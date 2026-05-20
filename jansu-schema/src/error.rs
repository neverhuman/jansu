// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Schema error type

use std::{
    fmt::{self, Display, Formatter},
    io,
    num::TryFromIntError,
    result,
    string::FromUtf8Error,
    sync::PoisonError,
};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use arrow::{datatypes::DataType, error::ArrowError};

use bytes::Bytes;

#[cfg(any(feature = "iceberg", feature = "delta"))]
use datafusion::error::DataFusionError;

#[cfg(feature = "delta")]
use deltalake::DeltaTableError;

use governor::InsufficientCapacity;

#[cfg(feature = "iceberg")]
use iceberg::spec::DataFileBuilderError;

use jansu_sans_io::ErrorCode;
use jsonschema::ValidationError;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use parquet::errors::ParquetError;

use rhai::EvalAltResult;
use serde_json::Value;
use tracing_subscriber::filter::ParseError;
use url::Url;

/// Error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    Anyhow(#[from] anyhow::Error),

    Api(ErrorCode),

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    Arrow(#[from] ArrowError),

    Avro(Box<apache_avro::Error>),

    AvroToJson(apache_avro::types::Value),

    BadDowncast {
        field: String,
    },

    EvalAlt(#[from] Box<EvalAltResult>),

    BuilderExhausted,

    ChronoParse(#[from] chrono::ParseError),

    #[cfg(feature = "iceberg")]
    DataFileBuilder(#[from] DataFileBuilderError),

    #[cfg(any(feature = "iceberg", feature = "delta"))]
    DataFusion(Box<DataFusionError>),

    #[cfg(feature = "delta")]
    DeltaTable(Box<DeltaTableError>),

    Downcast,

    FromUtf8(#[from] FromUtf8Error),

    #[cfg(feature = "iceberg")]
    Iceberg(Box<::iceberg::Error>),

    InvalidValue(apache_avro::types::Value),

    InsufficientCapacity(#[from] InsufficientCapacity),

    Io(#[from] io::Error),

    JsonToAvro(Box<apache_avro::Schema>, Box<Value>),

    JsonToAvroFieldNotFound {
        schema: Box<apache_avro::Schema>,
        value: Box<Value>,
        field: String,
    },

    KafkaSansIo(#[from] jansu_sans_io::Error),

    Message(String),

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    NoCommonType(Vec<DataType>),

    ObjectStore(#[from] object_store::Error),

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    Parquet(#[from] ParquetError),

    ParseFilter(#[from] ParseError),

    ParseUrl(#[from] url::ParseError),

    Poison,

    ProtobufJsonMapping(#[from] protobuf_json_mapping::ParseError),

    ProtobufJsonMappingPrint(#[from] protobuf_json_mapping::PrintError),

    Protobuf(#[from] protobuf::Error),

    ProtobufFileDescriptorMissing(Bytes),

    SchemaValidation,

    SerdeJson(#[from] serde_json::Error),

    #[cfg(any(feature = "iceberg", feature = "delta"))]
    SqlParser(#[from] datafusion::logical_expr::sqlparser::parser::ParserError),

    TopicWithoutSchema(String),

    TryFromInt(#[from] TryFromIntError),

    UnsupportedIcebergCatalogUrl(Url),

    UnsupportedLakeHouseUrl(Url),

    UnsupportedSchemaRegistryUrl(Url),

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    UnsupportedSchemaRuntimeValue(DataType, Value),

    Uuid(#[from] uuid::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[cfg(any(feature = "iceberg", feature = "delta"))]
impl From<DataFusionError> for Error {
    fn from(value: DataFusionError) -> Self {
        Self::DataFusion(Box::new(value))
    }
}

#[cfg(feature = "iceberg")]
impl From<::iceberg::Error> for Error {
    fn from(value: ::iceberg::Error) -> Self {
        Self::Iceberg(Box::new(value))
    }
}

#[cfg(feature = "delta")]
impl From<DeltaTableError> for Error {
    fn from(value: DeltaTableError) -> Self {
        Self::DeltaTable(Box::new(value))
    }
}

impl From<apache_avro::Error> for Error {
    fn from(value: apache_avro::Error) -> Self {
        Self::Avro(Box::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<ValidationError<'_>> for Error {
    fn from(_value: ValidationError<'_>) -> Self {
        Self::SchemaValidation
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;
