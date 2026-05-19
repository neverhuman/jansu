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

use crate::{Error, Result};
use arrow::datatypes::DataType;
use datafusion::sql::sqlparser::{
    ast::{DataType as SqlDataType, ExactNumberInfo, Expr, Value as SqlValue},
    dialect::GenericDialect,
    parser::Parser,
};
use tracing::debug;

pub(crate) fn typeof_sql_expr(expr: &str) -> Result<DataType> {
    let dialect = GenericDialect {};
    let ast = Parser::new(&dialect)
        .try_with_sql(expr)?
        .parse_expr()
        .inspect(|ast| debug!(?ast))?;

    typeof_ast_expr(ast)
}

fn typeof_ast_expr(expr: Expr) -> Result<DataType> {
    match expr {
        Expr::Cast { data_type, .. } | Expr::TypedString(TypedString { data_type, .. }) => {
            delta_sql_type(data_type)
        }
        Expr::Nested(expr) => typeof_ast_expr(*expr),
        Expr::Value(value) => match value.value {
            SqlValue::Boolean(_) => Ok(DataType::Boolean),
            SqlValue::Number(_, _) => Ok(DataType::Float64),
            SqlValue::SingleQuotedString(_)
            | SqlValue::DoubleQuotedString(_)
            | SqlValue::TripleSingleQuotedString(_)
            | SqlValue::TripleDoubleQuotedString(_)
            | SqlValue::DollarQuotedString(_)
            | SqlValue::EscapedStringLiteral(_)
            | SqlValue::SingleQuotedByteStringLiteral(_)
            | SqlValue::DoubleQuotedByteStringLiteral(_)
            | SqlValue::TripleSingleQuotedByteStringLiteral(_)
            | SqlValue::TripleDoubleQuotedByteStringLiteral(_) => Ok(DataType::Utf8),
            SqlValue::Null => Ok(DataType::Null),
            other => Err(Error::Message(format!(
                "unsupported SQL literal for type inference: {other:?}"
            ))),
        },
        Expr::Identifier(identifier) => identifier_type(&identifier.value),
        Expr::CompoundIdentifier(identifiers) => match identifiers.last() {
            Some(identifier) => identifier_type(&identifier.value),
            None => Err(Error::Message("empty SQL identifier".into())),
        },
        otherwise => Err(Error::Message(format!(
            "unsupported SQL expression for type inference: {otherwise:?}"
        ))),
    }
}

use datafusion::sql::sqlparser::ast::TypedString;

fn identifier_type(identifier: &str) -> Result<DataType> {
    match identifier.to_ascii_lowercase().as_str() {
        "timestamp" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        "date" => Ok(DataType::Date32),
        "offset" | "partition" | "leader_epoch" => Ok(DataType::Int64),
        other => Err(Error::Message(format!(
            "unknown SQL identifier type for generated column: {other}"
        ))),
    }
}

fn delta_sql_type(data_type: SqlDataType) -> Result<DataType> {
    match data_type {
        SqlDataType::Bool | SqlDataType::Boolean => Ok(DataType::Boolean),
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::TinyInt(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::Int(_)
        | SqlDataType::Int2(_)
        | SqlDataType::Int4(_)
        | SqlDataType::Integer(_)
        | SqlDataType::Int16
        | SqlDataType::Int32 => Ok(DataType::Int32),
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) | SqlDataType::Int64 => Ok(DataType::Int64),
        SqlDataType::Float(_) | SqlDataType::Float4 | SqlDataType::Float32 | SqlDataType::Real => {
            Ok(DataType::Float32)
        }
        SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float64 => Ok(DataType::Float64),
        SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::CharVarying(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Nvarchar(_)
        | SqlDataType::String(_)
        | SqlDataType::Text => Ok(DataType::Utf8),
        SqlDataType::Binary(_)
        | SqlDataType::Varbinary(_)
        | SqlDataType::Blob(_)
        | SqlDataType::Bytea
        | SqlDataType::Bytes(_) => Ok(DataType::Binary),
        SqlDataType::Timestamp(_, _) | SqlDataType::TimestampNtz | SqlDataType::Datetime(_) => Ok(
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
        ),
        SqlDataType::Decimal(info)
        | SqlDataType::Dec(info)
        | SqlDataType::Numeric(info)
        | SqlDataType::BigNumeric(info)
        | SqlDataType::BigDecimal(info) => decimal_type(info),

        otherwise => Err(Error::Message(format!(
            "unsupported SQL type for Delta generation: {otherwise:?}"
        ))),
    }
}

fn decimal_type(info: ExactNumberInfo) -> Result<DataType> {
    match info {
        ExactNumberInfo::None => Ok(DataType::Decimal128(38, 18)),
        ExactNumberInfo::Precision(precision) => {
            Ok(DataType::Decimal128(u8::try_from(precision)?, 0))
        }
        ExactNumberInfo::PrecisionAndScale(precision, scale) => Ok(DataType::Decimal128(
            u8::try_from(precision)?,
            i8::try_from(scale)?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::Error;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[test]
    fn simple_cast() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(
            typeof_sql_expr("cast(meta.timestamp as date)")?,
            DataType::Date32
        );

        Ok(())
    }
}
