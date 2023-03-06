use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt};
use std::collections::HashMap;

use crate::compile::schema;
use crate::compile::sql::{create_table_as, select_star_from};
use crate::types::LazyValue;
use crate::{
    ast::Ident,
    types,
    types::{arrow::EMPTY_RELATION, Arc, Value},
};

use super::SQLParam;
use super::{context::Context, error::*, sql::LazySQLParam};

type TypeRef = schema::Ref<types::Type>;

// This is a type alias for simplicity and to make it easy potentially in the future to allow a
// library user to pass in their own runtime.
pub type Runtime = tokio::runtime::Runtime;

#[cfg(feature = "multi-thread")]
pub fn build() -> Result<Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()?)
}

#[cfg(not(feature = "multi-thread"))]
pub fn build() -> Result<Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?)
}

pub fn expensive<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    match tokio::runtime::Handle::try_current() {
        #[cfg(feature = "multi-thread")]
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

pub fn eval<'a>(
    ctx: &'a mut Context,
    typed_expr: &'a schema::TypedExpr<TypeRef>,
) -> BoxFuture<'a, crate::runtime::Result<Value>> {
    async move {
        let mut lazy_value = eval_lazy(ctx, typed_expr).await?;
        lazy_value.get().await
    }
    .boxed()
}

pub async fn eval_params_lazy<'a>(
    ctx: &'a mut Context,
    params: &'a schema::Params<TypeRef>,
) -> Result<HashMap<Ident, LazySQLParam>> {
    let mut param_values = HashMap::new();
    for (name, param) in params {
        let value = eval_lazy(ctx, param).await?;
        param_values.insert(
            name.clone(),
            LazySQLParam::new(name.clone(), value, &*param.type_.read()?),
        );
    }

    Ok(param_values)
}

pub async fn resolve_params(
    params: HashMap<Ident, LazySQLParam>,
) -> Result<HashMap<Ident, SQLParam>> {
    let mut resolved_params = HashMap::new();
    for (name, param) in params.into_iter() {
        let value = param.get().await?;
        resolved_params.insert(name, value);
    }

    Ok(resolved_params)
}

pub fn eval_lazy<'a>(
    ctx: &'a mut Context,
    typed_expr: &'a schema::TypedExpr<TypeRef>,
) -> BoxFuture<'a, crate::runtime::Result<Box<dyn LazyValue>>> {
    async move {
        Ok(match &*typed_expr.expr.as_ref() {
            schema::Expr::Unknown => {
                return Err(RuntimeError::new("unresolved extern"));
            }
            schema::Expr::UncompiledFn(def) => {
                return Err(RuntimeError::new(
                    format!("uncompiled function: {:?}", def).as_str(),
                ));
            }
            schema::Expr::SchemaEntry(schema::STypedExpr { .. }) => {
                return Err(RuntimeError::new("unresolved schema entry"));
            }
            schema::Expr::Connection(..) => {
                return Err(RuntimeError::new("unresolved connection"));
            }
            schema::Expr::ContextRef(r) => match ctx.values.get(r) {
                Some(v) => v.clone().into(), // Can we avoid this clone??
                None => {
                    return Err(RuntimeError::new(
                        format!("No such context value {}", r).as_str(),
                    ))
                }
            },
            schema::Expr::Fn(f) => {
                use super::functions::*;
                let body = match &f.body {
                    schema::FnBody::Expr(e) => e.clone().into(),
                    _ => {
                        return fail!(
                            "Non-expression function body should have been optimized away"
                        )
                    }
                };
                QSFn::new(typed_expr.type_.clone(), body)?.into()
            }
            schema::Expr::NativeFn(name) => {
                use super::functions::*;
                match name.as_str() {
                    "load" => {
                        Value::Fn(Arc::new(LoadFileFn::new(&*typed_expr.type_.read()?)?)).into()
                    }
                    "__native_identity" => {
                        Value::Fn(Arc::new(IdentityFn::new(&*typed_expr.type_.read()?)?)).into()
                    }
                    _ => return rt_unimplemented!("native function: {}", name),
                }
            }
            schema::Expr::Materialize(schema::MaterializeExpr {
                key,
                expr,
                inlined,
                decl_name,
                url,
                ..
            }) => {
                if let (false, Some(value)) = (inlined, ctx.materializations.get(key)) {
                    return Ok(value.clone().into());
                }

                if *inlined {
                    let table_name = decl_name.into();
                    let engine = ctx.sql_engine(url.clone()).await?;

                    if !engine.table_exists(&table_name).await? {
                        match expr.expr.as_ref() {
                            schema::Expr::SQL(sql, _) => {
                                let query = create_table_as(table_name, sql.body.as_query()?, true);
                                let sql_params = eval_params_lazy(ctx, &sql.names.params).await?;
                                let _ = ctx
                                    .sql_engine(url.clone())
                                    .await?
                                    .exec(&query, sql_params)
                                    .await?;
                            }
                            schema::Expr::Materialize(schema::MaterializeExpr {
                                decl_name: inner_decl,
                                ..
                            }) => {
                                eval(ctx, &expr).await?;

                                let query =
                                    create_table_as(table_name, select_star_from(inner_decl), true);
                                let _ = ctx
                                    .sql_engine(url.clone())
                                    .await?
                                    .exec(&query, HashMap::new())
                                    .await?;
                            }
                            _ => {
                                let inner_type = expr.type_.read()?.clone();
                                let result = eval(ctx, &expr).await?;

                                let _ = ctx
                                    .sql_engine(url.clone())
                                    .await?
                                    .load(&table_name, result, inner_type, true)
                                    .await?;
                            }
                        }
                    }

                    // Don't stash the fact that we created the temporary table in the materialization index
                    // NOTE: For performance sake, we could cache something here (that we created the temp
                    // table), but for now we assume checking if the table exists is fast enough.
                    EMPTY_RELATION.clone().into()
                } else {
                    let result = eval(ctx, expr).await?;

                    ctx.materializations
                        .entry(key.clone())
                        .or_insert(result)
                        .clone()
                        .into()
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
                    // Eval the arguments in the calling context. Functions do not currently accept
                    // lazy parameters, so evaluate them eagerly.
                    // TODO: Change the function interface to accept lazy parameters
                    //
                    arg_values.push(eval(ctx, arg).await?);
                }
                let fn_val = match eval(&mut new_ctx, func.as_ref()).await? {
                    Value::Fn(f) => f,
                    _ => return fail!("Cannot call non-function"),
                };

                fn_val.execute(&mut new_ctx, arg_values).await?.into()
            }
            schema::Expr::SQL(e, url) => {
                let schema::SQL { body, names } = e.as_ref();
                let sql_params = eval_params_lazy(ctx, &names.params).await?;
                let query = body.as_statement()?;

                let engine = ctx.sql_engine(url.clone()).await?;

                // TODO: This ownership model implies some necessary copying (below).
                let mut rows = engine.query(&query, sql_params).await?;

                // Before returning, we perform some runtime checks that might only be necessary in debug mode:
                // - For expressions, validate that the result is a single row and column
                // - For expressions and queries, check that the RecordBatch's type matches the
                //   expected type from the compiler.
                let expected_type = typed_expr.type_.read()?;
                let value = match body {
                    schema::SQLBody::Expr(_) => {
                        if rows.num_batches() != 1 {
                            return fail!("Expected an expression to have exactly one row");
                        }
                        if rows.schema().len() != 1 {
                            return fail!("Expected an expression to have exactly one column");
                        }

                        let value_type = rows.batch(0).records()[0].column(0).type_();
                        if !ctx.disable_typechecks && *expected_type != value_type {
                            let target_schema = vec![crate::types::Field::new_nullable(
                                "value".into(),
                                expected_type.clone(),
                            )];
                            rows = rows.try_cast(&target_schema)?;
                        }
                        let row = &rows.batch(0).records()[0];
                        row.column(0).clone()
                    }
                    schema::SQLBody::Query(_) | schema::SQLBody::Table(_) => {
                        // Validate that the schema matches the expected type. If not, we have a serious problem
                        // since we may interpret the record batch as a different type than expected.
                        if !ctx.disable_typechecks && rows.num_batches() > 0 {
                            let rows_type = crate::types::Type::List(Box::new(
                                crate::types::Type::Record(rows.schema()),
                            ));
                            if *expected_type != rows_type {
                                let target_schema = match &*expected_type {
                                    crate::types::Type::List(t) => match &**t {
                                        crate::types::Type::Record(s) => s,
                                        _ => {
                                            return Err(RuntimeError::type_mismatch(
                                                expected_type.clone(),
                                                rows_type,
                                            ));
                                        }
                                    },
                                    _ => {
                                        return Err(RuntimeError::type_mismatch(
                                            expected_type.clone(),
                                            rows_type,
                                        ));
                                    }
                                };

                                rows = rows.try_cast(&target_schema)?;
                            }
                        }

                        Value::Relation(rows)
                    }
                };

                value.into()
            }
        })
    }
    .boxed()
}
