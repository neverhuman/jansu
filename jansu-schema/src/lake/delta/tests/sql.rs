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
