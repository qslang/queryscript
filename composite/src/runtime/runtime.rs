use crate::ast;
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

pub fn eval(_schema: &schema::Schema, expr: &ast::Expr) -> Result<Value> {
    match expr {
        ast::Expr::Unknown => {
            return Err(RuntimeError::new("unresolved extern"));
        }
        ast::Expr::SQLQuery(_) => {
            return Err(RuntimeError::unimplemented("SELECT"));
        }
        ast::Expr::SQLExpr(e) => match e {
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
                    return Err(RuntimeError::unimplemented(e.to_string().as_str()));
                }
            },
            _ => {
                return Err(RuntimeError::unimplemented(e.to_string().as_str()));
            }
        },
    }
}
