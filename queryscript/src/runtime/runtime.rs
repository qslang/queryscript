use futures::future::{BoxFuture, FutureExt};
use std::collections::HashMap;

use crate::compile::schema;
use crate::compile::sql::create_table_as;
use crate::{
    ast::Ident,
    types,
    types::{arrow::EMPTY_RELATION, Arc, Value},
};

use super::{context::Context, error::*, sql::SQLParam};

type TypeRef = schema::Ref<types::Type>;

// This is a type alias for simplicity and to make it easy potentially in the future to allow a
// library user to pass in their own runtime.
pub type Runtime = tokio::runtime::Runtime;

#[cfg(feature = "multi-thread")]
pub fn build() -> Result<Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread().build()?)
}

#[cfg(not(feature = "multi-thread"))]
pub fn build() -> Result<Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread().build()?)
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

pub async fn eval_params<'a>(
    ctx: &'a mut Context,
    params: &'a schema::Params<TypeRef>,
) -> Result<HashMap<Ident, SQLParam>> {
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

pub fn eval<'a>(
    ctx: &'a mut Context,
    typed_expr: &'a schema::TypedExpr<TypeRef>,
) -> BoxFuture<'a, crate::runtime::Result<crate::types::Value>> {
    async move {
        match &*typed_expr.expr.as_ref() {
            schema::Expr::Unknown => {
                return Err(RuntimeError::new("unresolved extern"));
            }
            schema::Expr::UncompiledFn(def) => {
                return Err(RuntimeError::new(
                    format!("uncompiled function: {:?}", def).as_str(),
                ));
            }
            schema::Expr::Expanded(items) => {
                return Err(RuntimeError::new(
                    format!("unresolved expanded items: {:?}", items).as_str(),
                ));
            }
            schema::Expr::SchemaEntry(schema::STypedExpr { .. }) => {
                return Err(RuntimeError::new("unresolved schema entry"));
            }
            schema::Expr::Connection(..) => {
                return Err(RuntimeError::new("unresolved connection"));
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
                QSFn::new(typed_expr.type_.clone(), body)
            }
            schema::Expr::NativeFn(name) => {
                use super::functions::*;
                match name.as_str() {
                    "load" => Ok(Value::Fn(Arc::new(LoadFileFn::new(
                        &*typed_expr.type_.read()?,
                    )?))),
                    "__native_identity" => Ok(Value::Fn(Arc::new(IdentityFn::new(
                        &*typed_expr.type_.read()?,
                    )?))),
                    _ => return rt_unimplemented!("native function: {}", name),
                }
            }
            schema::Expr::Materialize(schema::MaterializeExpr {
                key,
                expr,
                inlined,
                decl_name,
                ..
            }) => {
                if let Some(value) = ctx.materializations.get(key) {
                    return Ok(value.clone());
                }

                let result = match (inlined, expr.expr.as_ref()) {
                    (true, schema::Expr::SQL(sql, sql_url)) => {
                        let table_name = decl_name.into();
                        let engine = ctx.sql_engine(sql_url.clone())?;

                        if !engine.table_exists(&table_name).await? {
                            let query = create_table_as(table_name, sql.body.as_query(), true);
                            let sql_params = eval_params(ctx, &sql.names.params).await?;
                            let _ = ctx
                                .sql_engine(sql_url.clone())?
                                .eval(&query, sql_params)
                                .await?;
                        }

                        // Don't stash the fact that we created the temporary table in the materialization index
                        // NOTE: For performance sake, we could cache something here (that we created the temp
                        // table), but for now we assume checking if the table exists is fast enough.
                        return Ok(EMPTY_RELATION.clone());
                    }
                    _ => eval(ctx, expr).await?,
                };

                Ok(ctx
                    .materializations
                    .entry(key.clone())
                    .or_insert(result)
                    .clone())
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
                let fn_val = match eval(&mut new_ctx, func.as_ref()).await? {
                    Value::Fn(f) => f,
                    _ => return fail!("Cannot call non-function"),
                };

                fn_val.execute(&mut new_ctx, arg_values).await
            }
            schema::Expr::SQL(e, url) => {
                let schema::SQL { body, names } = e.as_ref();
                let sql_params = eval_params(ctx, &names.params).await?;
                let query = body.as_statement();

                let engine = ctx.sql_engine(url.clone())?;

                // TODO: This ownership model implies some necessary copying (below).
                let mut rows = engine.eval(&query, sql_params).await?;

                // Before returning, we perform some runtime checks that might only be necessary in debug mode:
                // - For expressions, validate that the result is a single row and column
                // - For expressions and queries, check that the RecordBatch's type matches the
                //   expected type from the compiler.
                let expected_type = typed_expr.type_.read()?;
                match body {
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
                        Ok(row.column(0).clone())
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

                        Ok(Value::Relation(rows))
                    }
                }
            }
        }
    }
    .boxed()
}
