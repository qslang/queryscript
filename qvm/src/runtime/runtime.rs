use crate::runtime::error::*;
use crate::schema;
use sqlparser::ast as sqlast;
use std::collections::HashMap;

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
        schema::Expr::Decl(decl) => {
            let ret = match decl.borrow().value {
                crate::schema::SchemaEntry::Expr(ref e) => eval(schema.clone(), &e.expr),
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
            super::sql::eval(schema, query, HashMap::new())?;
            Ok(Value::Null)
        }
        schema::Expr::SQLExpr(schema::SQLExpr { expr, params }) => {
            let mut param_values = HashMap::new();
            for (name, param) in params {
                let value = eval(schema.clone(), param)?;
                param_values.insert(
                    name.clone(),
                    super::sql::SQLParam {
                        name: name.clone(),
                        type_: schema::Type::Unknown,
                        value,
                    },
                );
            }

            let query = sqlast::Query {
                with: None,
                body: Box::new(sqlast::SetExpr::Select(Box::new(sqlast::Select {
                    distinct: false,
                    top: None,
                    projection: vec![sqlast::SelectItem::ExprWithAlias {
                        expr: expr.clone(),
                        alias: sqlast::Ident {
                            value: "value".to_string(),
                            quote_style: None,
                        },
                    }],
                    into: None,
                    from: Vec::new(),
                    lateral_views: Vec::new(),
                    selection: None,
                    group_by: Vec::new(),
                    cluster_by: Vec::new(),
                    distribute_by: Vec::new(),
                    sort_by: Vec::new(),
                    having: None,
                    qualify: None,
                }))),
                order_by: Vec::new(),
                limit: None,
                offset: None,
                fetch: None,
                lock: None,
            };

            super::sql::eval(schema, &query, param_values)?;
            Ok(Value::Null)
        }
    }
}
