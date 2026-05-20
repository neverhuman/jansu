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

//! Delta Lake SQL parser tests

use datafusion::{
    logical_expr::sqlparser::ast::Expr,
    sql::sqlparser::{
        ast::{CastKind, DataType as SqlDataType},
        dialect::GenericDialect,
        parser::Parser,
    },
};

use super::*;

#[test]
fn sql_parser_cast() -> Result<()> {
    let _guard = init_tracing()?;

    let dialect = GenericDialect {};

    let sql = "cast(meta.timestamp as date)";

    let expr = Parser::new(&dialect)
        .try_with_sql(sql)?
        .parse_expr()
        .inspect(|ast| debug!(?ast))?;

    assert!(matches!(
        expr,
        Expr::Cast {
            kind: CastKind::Cast,
            data_type: SqlDataType::Date,
            format: None,
            ..
        }
    ));

    Ok(())
}
