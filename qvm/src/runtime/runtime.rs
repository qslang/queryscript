use futures::future::{BoxFuture, FutureExt};
use std::collections::{BTreeMap, HashMap};

use crate::compile::schema;
use crate::{
    types,
    types::{Arc, Value},
};

use super::{context::Context, error::*, new_engine, sql::SQLParam, SQLEngineType};

type TypeRef = schema::Ref<types::Type>;

// This is a type alias for simplicity and to make it easy potentially in the future to allow a
// library user to pass in their own runtime.
pub type Runtime = tokio::runtime::Runtime;

pub fn build() -> Result<Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread().build()?)
}

pub async fn eval_params<'a>(
    ctx: &'a Context,
    params: &'a schema::Params<TypeRef>,
) -> Result<HashMap<String, SQLParam>> {
    let mut param_values = HashMap::new();
    for (name, param) in params {
        let value = eval(ctx, param).await?;
        param_values.insert(
            name.clone(),
            SQLParam::new(name.clone(), value, &*param.type_.read()?),
        );
    }

    Ok(param_values)
}

pub fn build_context(schema: &schema::SchemaRef, engine_type: SQLEngineType) -> Context {
    let schema = schema.read().unwrap();
    Context {
        folder: schema.folder.clone(),
        values: BTreeMap::new(),
        sql_engine: new_engine(engine_type),
    }
}

pub fn eval<'a>(
    ctx: &'a Context,
    typed_expr: &'a schema::TypedExpr<TypeRef>,
) -> BoxFuture<'a, crate::runtime::Result<crate::types::Value>> {
    async move {
        match &*typed_expr.expr.as_ref() {
            schema::Expr::Unknown => {
                return Err(RuntimeError::new("unresolved extern"));
            }
            schema::Expr::SchemaEntry(schema::STypedExpr { expr, .. }) => {
                let rt_expr = {
                    let expr = expr.must()?;
                    let expr = expr.read()?;
                    Arc::new(expr.to_runtime_type()?)
                };

                eval(
                    ctx,
                    &schema::TypedExpr {
                        type_: typed_expr.type_.clone(),
                        expr: rt_expr,
                    },
                )
                .await
            }
            schema::Expr::ContextRef(r) => match ctx.values.get(r) {
                Some(v) => Ok(v.clone()), // Can we avoid this clone??
                None => Err(RuntimeError::new(
                    format!("No such context value {}", r).as_str(),
                )),
            },
            schema::Expr::Fn(f) => {
                use super::functions::*;
                let body = match &f.body {
                    schema::FnBody::Expr(e) => e.clone(),
                    _ => {
                        return fail!(
                            "Non-expression function body should have been optimized away"
                        )
                    }
                };
                QVMFn::new(typed_expr.type_.clone(), body)
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
            schema::Expr::FnCall(schema::FnCallExpr {
                func,
                args,
                ctx_folder,
            }) => {
                let mut new_ctx = ctx.clone();
                new_ctx.folder = ctx_folder.clone();
                let mut arg_values = Vec::new();
                for arg in args.iter() {
                    // Eval the arguments in the calling context
                    //
                    arg_values.push(eval(ctx, arg).await?);
                }
                let fn_val = match eval(&new_ctx, func.as_ref()).await? {
                    Value::Fn(f) => f,
                    _ => return fail!("Cannot call non-function"),
                };

                fn_val.execute(&new_ctx, arg_values).await
            }
            schema::Expr::SQL(e) => {
                let schema::SQL { body, names } = e.as_ref();
                let sql_params = eval_params(ctx, &names.params).await?;
                let query = body.as_query();

                // TODO: This ownership model implies some necessary copying (below).
                let rows = { ctx.sql_engine.eval(&query, sql_params).await? };

                match body {
                    schema::SQLBody::Expr(_) => {
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
                    schema::SQLBody::Query(_) => Ok(Value::Relation(rows)),
                }
            }
        }
    }
    .boxed()
}
