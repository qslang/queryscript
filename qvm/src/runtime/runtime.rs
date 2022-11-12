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

pub fn eval(schema: schema::SchemaRef, expr: &schema::Expr) -> Result<Value> {
    match expr {
        schema::Expr::Unknown => {
            return Err(RuntimeError::new("unresolved extern"));
        }
        schema::Expr::Ref(schema::PathRef { items, schema }) => {
            // XXX use the instance id
            let (decl, _, _) =
                crate::compile::lookup_path(schema.schema.clone(), &items).expect("invalid path");

            let ret = match decl.borrow().value {
                crate::schema::SchemaEntry::Expr(ref e) => eval(schema.schema.clone(), &e.expr),
                _ => {
                    return rt_unimplemented!("evaluating a non-expression");
                }
            };
            ret
        }
        schema::Expr::Fn { .. } => {
            return Err(RuntimeError::unimplemented("functions"));
        }
        schema::Expr::SQLQuery { query, .. } => {
            super::sql::eval(schema, query)?;
            Ok(Value::Null)
        }
        schema::Expr::SQLExpr(schema::SQLExpr { expr, .. }) => match expr {
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

/*
fn chase_schema(
    schema: &schema::Schema,
    schema_path: schema::SchemaPath,
) -> Result<&schema::Schema> {
    let curr = schema;
    // XXX how do we use the usize instance id?
    for (path, _) in schema_path.iter() {
        match curr.imports.get(path) {
            None => {
                return fail!("Cannot find import: {:?}", path);
            }
            Some(schema) => {
                curr = schema.as_ref().as_ref();
            }
        }
    }
}
*/
