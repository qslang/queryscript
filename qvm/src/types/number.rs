use datafusion::{prelude::Expr, sql::planner::parse_sql_number};

use super::error::{ts_fail, Result};
use super::types::Type;

pub fn parse_numeric_type(n: &str) -> Result<Type> {
    match parse_sql_number(n)? {
        Expr::Literal(v) => (&v.get_datatype()).try_into(),
        v => ts_fail!("Unexpected value after parsing number: {:?}", v),
    }
}
