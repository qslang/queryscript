use super::error::{ts_fail, NumericParseSnafu, Result};
use super::{
    types::{AtomicType, Type},
    Value,
};

pub fn parse_numeric_type(n: &str) -> Result<AtomicType> {
    match parse_sql_number(n)?.type_() {
        Type::Atom(t) => Ok(t),
        t => ts_fail!("Expected atomic type after parsing number: {:?}", t),
    }
}

// NOTE: This is originally copied from datafusion.
// Parse number in sql string, convert to Expr::Literal
pub fn parse_sql_number(n: &str) -> Result<Value> {
    // parse first as i64
    n.parse::<i64>()
        .map(|i| Value::Int64(i))
        // if parsing as i64 fails try f64
        .or_else(|_| n.parse::<f64>().map(|f| Value::Float64(f)))
        .map_err(|_| NumericParseSnafu { val: n.to_string() }.build())
}
