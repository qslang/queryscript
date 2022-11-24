use super::sql::SQLParam;
use crate::compile::schema;
use crate::runtime::error::*;
use crate::types;
use crate::types::{Arc, Value};
use sqlparser::ast as sqlast;
use std::collections::HashMap;

type TypeRef = schema::Ref<types::Type>;

// This is a type alias for simplicity and to make it easy potentially in the future to allow a
// library user to pass in their own runtime.
pub type Runtime = tokio::runtime::Runtime;

pub fn build() -> Result<Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread().build()?)
}

pub async fn eval_params(
    schema: schema::SchemaRef,
    params: &schema::Params<TypeRef>,
) -> Result<HashMap<String, SQLParam>> {
    let mut param_values = HashMap::new();
    for (name, param) in params {
        let value = eval(schema.clone(), param).await?;
        param_values.insert(
            name.clone(),
            SQLParam::new(name.clone(), value, &*param.type_.read()?),
        );
    }

    Ok(param_values)
}

pub fn eval<'a>(
    schema: schema::SchemaRef,
    typed_expr: &'a schema::TypedExpr<TypeRef>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + 'a>> {
    Box::pin(async move {
        match &*typed_expr.expr.as_ref() {
            schema::Expr::Unknown => {
                return Err(RuntimeError::new("unresolved extern"));
            }
            schema::Expr::SchemaEntry(schema::SchemaEntryExpr { entry, .. }) => {
                let ret = match entry {
                    schema::SchemaEntry::Expr(e) => {
                        eval(
                            schema.clone(),
                            &schema::TypedExpr {
                                type_: typed_expr.type_.clone(),
                                expr: Arc::new(
                                    e.must()?.read()?.expr.must()?.read()?.to_runtime_type()?,
                                ),
                            },
                        )
                        .await
                    }
                    _ => {
                        return rt_unimplemented!("evaluating a non-expression");
                    }
                };
                ret
            }
            schema::Expr::Fn { .. } => {
                return Err(RuntimeError::unimplemented("functions"));
            }
            schema::Expr::NativeFn(name) => {
                use super::functions::*;
                match name.as_str() {
                    "load" => Ok(Value::Fn(Arc::new(LoadFileFn::new(
                        &*typed_expr.type_.read()?,
                    )?))),
                    _ => return rt_unimplemented!("native function: {}", name),
                }
            }
            schema::Expr::FnCall(schema::FnCallExpr { func, args }) => {
                let mut arg_values = Vec::new();
                for arg in args.iter() {
                    arg_values.push(eval(schema.clone(), arg).await?);
                }
                let fn_val = match eval(schema.clone(), func.as_ref()).await? {
                    Value::Fn(f) => f,
                    _ => return fail!("Cannot call non-function"),
                };

                fn_val.execute(arg_values).await
            }
            schema::Expr::SQLQuery(q) => {
                let schema::SQLQuery { query, params } = q.as_ref();
                let sql_params = eval_params(schema.clone(), &params).await?;
                Ok(Value::Relation(super::sql::eval(&query, sql_params).await?))
            }
            schema::Expr::SQLExpr(e) => {
                let schema::SQLExpr { expr, params } = e.as_ref();
                let sql_params = eval_params(schema.clone(), &params).await?;
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

                // TODO: This ownership model implies some necessary copying (below).
                let rows = super::sql::eval(&query, sql_params).await?;

                // TODO: These runtime checks may only be necessary in debug mode
                if rows.num_batches() != 1 {
                    return fail!("Expected an expression to have exactly one row");
                }
                if rows.schema().len() != 1 {
                    return fail!("Expected an expression to have exactly one column");
                }

                let row = &rows.batch(0).records()[0];
                Ok(row.column(0).clone())
            }
        }
    })
}
