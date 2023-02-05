// This file is responsible for the QueryScript equivalent of orchestration: saving "views"
// back to the original database.
use futures::future::{BoxFuture, FutureExt};
use sqlparser::ast as sqlast;
use std::collections::HashSet;
use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

use crate::compile::schema::SQLBody;
use crate::runtime::SQLEngine;
use crate::types::Type;
use crate::{
    compile::{
        schema::{Expr, Ident, MaterializeExpr, SQLSnippet},
        sql::{create_table_as, create_view_as},
        ConnectionString, SchemaRef,
    },
    runtime::{self, new_engine, Context, Result, SQLEngineType},
};
use tokio::task::JoinHandle;

async fn execute_create_view(
    ctx: &Context,
    schema: SchemaRef,
    engine: Arc<dyn SQLEngine>,
    url: Option<Arc<ConnectionString>>,
    name: &Ident,
    sql: &Arc<SQLSnippet<Arc<RwLock<Type>>, SQLBody>>,
    query: sqlparser::ast::Statement,
    seen: &mut HashSet<Ident>,
    handles: &mut Vec<JoinHandle<Result<()>>>,
) -> Result<()> {
    for (name, param) in &sql.names.params {
        match param.expr.as_ref() {
            Expr::Materialize(MaterializeExpr {
                inlined: true,
                decl_name,
                ..
            }) => {
                process_view(ctx, schema.clone(), decl_name, seen, handles).await?;
            }
            _ => {
                eprintln!(
            "Skipping \"{}\" because it has parameters that must be evaluated outside the database",
            name
        );
                return Ok(());
            }
        };
    }

    eprintln!("Creating view \"{}\"", name);
    handles.push(tokio::spawn({
        let ctx = ctx.clone();
        let url = url.clone();
        async move {
            engine.eval(&ctx, url, &query, HashMap::new()).await?;
            Ok(())
        }
    }));
    Ok(())
}

fn process_view<'a>(
    ctx: &'a Context,
    schema: SchemaRef,
    name: &'a Ident,
    seen: &'a mut HashSet<Ident>,
    handles: &'a mut Vec<tokio::task::JoinHandle<Result<()>>>,
) -> BoxFuture<'a, Result<()>> {
    async move {
        if seen.contains(name) {
            return Ok(());
        }
        let object_name = sqlast::ObjectName(vec![sqlast::Located::new(name.into(), None)]);

        let decl = schema
            .read()?
            .expr_decls
            .get(name)
            .ok_or_else(|| {
                runtime::error::RuntimeError::new(&format!("No such view \"{}\"", name))
            })?
            .clone();

        if !decl.public {
            return Ok(());
        }

        let expr = decl.value.expr.must()?;
        let expr = expr.read()?.to_runtime_type()?;
        match expr {
            Expr::SQL(ref sql, url) => {
                let engine = match &url {
                    Some(url) => new_engine(SQLEngineType::from_name(url.engine_name())?),
                    None => {
                        eprintln!(
                            "Skipping \"{}\" because it does not belong to a database",
                            name
                        );
                        return Ok(());
                    }
                };

                let query = create_view_as(object_name, sql.body.as_query());
                execute_create_view(
                    &ctx,
                    schema.clone(),
                    engine,
                    url.clone(),
                    &name,
                    &sql,
                    query,
                    seen,
                    handles,
                )
                .await?;
            }
            Expr::Materialize(MaterializeExpr { expr, url, .. }) => {
                match (url, expr.expr.as_ref()) {
                    (Some(url), Expr::SQL(sql, Some(sql_url)))
                        if url.as_ref() == sql_url.as_ref() =>
                    {
                        // If the URL is unspecified, use the SQL query's URL
                        let engine = new_engine(SQLEngineType::from_name(url.engine_name())?);

                        let query = create_table_as(object_name.into(), sql.body.as_query(), false);
                        execute_create_view(
                            &ctx,
                            schema.clone(),
                            engine,
                            Some(url.clone()),
                            &name,
                            &sql,
                            query,
                            seen,
                            handles,
                        )
                        .await?;
                    }
                    (Some(url), _) => {
                        let engine = new_engine(SQLEngineType::from_name(url.engine_name())?);
                        handles.push(tokio::spawn({
                            let ctx = ctx.clone();
                            let url = url.clone();
                            let type_ = expr.type_.read()?.clone();
                            async move {
                                let data = runtime::eval(&ctx, &expr).await?;
                                engine
                                    .load(
                                        &ctx,
                                        url.clone(),
                                        &object_name,
                                        data,
                                        type_,
                                        false, /*temporary*/
                                    )
                                    .await?;
                                Ok(())
                            }
                        }));
                    }
                    _ => {
                        eprintln!(
                            "Skipping \"{}\" because it does not belong to a database",
                            name
                        );
                    }
                };
            }
            _ => {
                eprintln!(
                    "Skipping view \"{}\" because it is not a SQL expression...",
                    name
                );
            }
        };

        // XXX Until we implement some kind of connection pooling, we need this stop-gap to prevent
        // concurrent writes to the same database from conflicting.
        for handle in handles.drain(..) {
            handle.await.expect("Failed to join task")?;
        }

        Ok(())
    }
    .boxed()
}

pub async fn save_views(ctx: &Context, schema: SchemaRef) -> Result<()> {
    let mut handles = Vec::new();
    let mut seen = HashSet::new();
    for name in schema.read()?.expr_decls.keys() {
        process_view(ctx, schema.clone(), name, &mut seen, &mut handles).await?;
    }
    eprintln!("--\nWaiting for all views to complete...");
    for handle in handles {
        handle.await.expect("Failed to join task")?;
    }
    eprintln!("Success!");

    Ok(())
}
