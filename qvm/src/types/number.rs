use datafusion::{prelude::Expr, sql::planner::parse_sql_number};

use super::error::{ts_fail, Result};
use super::types::{AtomicType, Type};

pub fn parse_numeric_type(n: &str) -> Result<AtomicType> {
    match parse_sql_number(n)? {
        Expr::Literal(v) => {
            let type_: Type = (&v.get_datatype()).try_into()?;
            match type_ {
                Type::Atom(a) => Ok(a),
                _ => ts_fail!("Expected atomic type after parsing number: {:?}", v),
            }
        }
        v => ts_fail!("Unexpected value after parsing number: {:?}", v),
    }
}
