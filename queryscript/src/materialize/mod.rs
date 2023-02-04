// This file is responsible for the QueryScript equivalent of orchestration: saving "views"
// back to the original database.
use sqlparser::ast as sqlast;
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
    types::Relation,
};
use tokio::task::JoinHandle;

fn execute_create_view(
    ctx: &Context,
    engine: Arc<dyn SQLEngine>,
    url: Option<Arc<ConnectionString>>,
    name: &Ident,
    sql: &Arc<SQLSnippet<Arc<RwLock<Type>>, SQLBody>>,
    query: sqlparser::ast::Statement,
    handles: &mut Vec<JoinHandle<Result<()>>>,
) -> Result<()> {
    if !sql.names.params.is_empty() {
        eprintln!(
            "Skipping \"{}\" because it has parameters that must be evaluated outside the database",
            name
        );
        return Ok(());
    }

    eprintln!("Creating view \"{}\"", name);
    handles.push(tokio::spawn({
        let ctx = ctx.clone();
        let url = url.clone();
        async move {
            engine.eval(&ctx, url, &query, HashMap::new()).await;
            Ok(())
        }
    }));
    Ok(())
}

pub async fn save_views(
    schema: SchemaRef,
    engine_type: crate::runtime::SQLEngineType,
) -> Result<()> {
    let ctx = Arc::new(Context::new(schema.read()?.folder.clone(), engine_type));
    let mut handles = Vec::new();
    for (name, decl) in schema.read()?.expr_decls.iter() {
        if !decl.public {
            continue;
        }

        let object_name = sqlast::ObjectName(vec![sqlast::Located::new(name.into(), None)]);

        let expr = decl.value.expr.must()?;
        let expr = expr.read()?;
        match expr.to_runtime_type()? {
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
                execute_create_view(&ctx, engine, url.clone(), &name, &sql, query, &mut handles)?;
            }
            Expr::Materialize(MaterializeExpr { key: _, expr, url }) => {
                let (url, engine) = match &url {
                    Some(url) => (
                        url,
                        new_engine(SQLEngineType::from_name(url.engine_name())?),
                    ),
                    None => {
                        eprintln!(
                            "Skipping \"{}\" because it does not belong to a database",
                            name
                        );
                        continue;
                    }
                };

                match expr.expr.as_ref() {
                    Expr::SQL(sql, Some(sql_url)) if url.as_ref() == sql_url.as_ref() => {
                        let query = create_view_as(object_name.into(), sql.body.as_query());
                        execute_create_view(
                            &ctx,
                            engine,
                            Some(url.clone()),
                            &name,
                            &sql,
                            query,
                            &mut handles,
                        )?;
                    }
                    _ => {
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
                };
            }
            _ => {
                eprintln!(
                    "Skipping view \"{}\" because it is not a SQL expression...",
                    name
                );
            }
        }

        // XXX Until we implement some kind of connection pooling, we need this stop-gap to prevent
        // concurrent writes to the same database from conflicting.
        for handle in handles.drain(..) {
            handle.await.expect("Failed to join task")?;
        }
    }
    eprintln!("--\nWaiting for all views to complete...");
    for handle in handles {
        handle.await.expect("Failed to join task")?;
    }
    eprintln!("Success!");

    Ok(())
}
