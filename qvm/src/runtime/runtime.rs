use super::sql::SQLParam;
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

pub fn eval_params(
    schema: schema::SchemaRef,
    params: &schema::Params,
) -> Result<HashMap<Vec<String>, SQLParam>> {
    let mut param_values = HashMap::new();
    for (name, param) in params {
        eprintln!("expr: {:?}", &param.expr);
        let value = eval(schema.clone(), &param.expr)?;
        eprintln!("evaluated value: {:?}", value);
        param_values.insert(
            name.clone(),
            SQLParam::new(name.clone(), value, &param.type_),
        );
    }

    Ok(param_values)
}

pub fn eval(schema: schema::SchemaRef, expr: &schema::Expr) -> Result<Value> {
    match expr {
        schema::Expr::Unknown => {
            return Err(RuntimeError::new("unresolved extern"));
        }
        schema::Expr::Decl(decl) => {
            let ret = match &decl.value {
                crate::schema::SchemaEntry::Expr(e) => eval(schema.clone(), &e.borrow().expr),
                _ => {
                    return rt_unimplemented!("evaluating a non-expression");
                }
            };
            ret
        }
        schema::Expr::Fn { .. } => {
            return Err(RuntimeError::unimplemented("functions"));
        }
        schema::Expr::SQLQuery(schema::SQLQuery { query, params }) => {
            let sql_params = eval_params(schema.clone(), params)?;
            super::sql::eval(schema, query, sql_params)?;
            Ok(Value::Null)
        }
        schema::Expr::SQLExpr(schema::SQLExpr { expr, params }) => {
            let sql_params = eval_params(schema.clone(), params)?;
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

            let mut rows = super::sql::eval(schema, &query, sql_params)?;
            if rows.len() != 1 {
                return fail!("Expected an expression to have exactly one row");
            }

            Ok(rows.remove(0))
        }
    }
}
