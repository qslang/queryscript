use crate::runtime::error::*;
use crate::schema;
use sqlparser::ast as sqlast;

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Number(f64),
    String(String),
    Bool(bool),
}

pub fn eval(schema: &schema::Schema, expr: &schema::Expr) -> Result<Value> {
    match expr {
        schema::Expr::Unknown => {
            return Err(RuntimeError::new("unresolved extern"));
        }
        schema::Expr::Path { .. } => {
            return Err(RuntimeError::unimplemented("imported values"));
        }
        schema::Expr::SQLQuery { query, .. } => {
            super::sql::eval(schema, query)?;
            Ok(Value::Null)
        }
        schema::Expr::SQLExpr { expr, .. } => match expr {
            sqlast::Expr::Value(v) => match v {
                sqlast::Value::Number(n, _) => Ok(Value::Number(n.parse()?)),
                sqlast::Value::SingleQuotedString(s)
                | sqlast::Value::EscapedStringLiteral(s)
                | sqlast::Value::NationalStringLiteral(s)
                | sqlast::Value::HexStringLiteral(s)
                | sqlast::Value::DoubleQuotedString(s) => Ok(Value::String(s.clone())),
                sqlast::Value::Boolean(b) => Ok(Value::Bool(*b)),
                sqlast::Value::Null => Ok(Value::Null),
                _ => {
                    return Err(RuntimeError::unimplemented(expr.to_string().as_str()));
                }
            },
            _ => {
                return Err(RuntimeError::unimplemented(expr.to_string().as_str()));
            }
        },
    }
}
