// This file is responsible for the QueryScript equivalent of orchestration: saving "views"
// back to the original database.
use snafu::prelude::*;
use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

use crate::compile::inference::mkcref;
use crate::compile::schema::{CRef, Decl, DeclMap, SQLBody, STypedExpr};
use crate::runtime::context::ContextPool;
use crate::types::Type;
use crate::{
    ast::SourceLocation,
    compile::{
        error::RuntimeSnafu,
        schema::{Expr, Ident, MaterializeExpr, SQLSnippet},
        sql::{create_table_as, create_view_as},
        ConnectionString, Result, SchemaRef,
    },
    runtime,
};
use tokio::task::JoinHandle;

type Signals = HashMap<Ident, CRef<()>>;

fn gather_materialize_candidates(decls: &DeclMap<STypedExpr>) -> Result<Signals> {
    let mut signals = Signals::new();
    for (name, decl) in decls.iter() {
        if !decl.public {
            continue;
        }

        let expr = decl.value.expr.must().context(RuntimeSnafu {
            loc: decl.location().clone(),
        })?;
        let expr = expr.read()?.to_runtime_type().context(RuntimeSnafu {
            loc: decl.location().clone(),
        })?;

        match &expr {
            Expr::SQL(_, url) | Expr::Materialize(MaterializeExpr { url, .. }) => {
                if url.is_some() {
                    signals.insert(name.clone(), CRef::new_unknown(&format!("{}", name)));
                }
            }
            _ => continue,
        }
    }

    Ok(signals)
}

fn execute_create_view(
    ctx_pool: &ContextPool,
    url: Option<Arc<ConnectionString>>,
    name: &Ident,
    sql: &Arc<SQLSnippet<Arc<RwLock<Type>>, SQLBody>>,
    query: sqlparser::ast::Statement,
    signals: &Signals,
    handles: &mut Vec<JoinHandle<crate::runtime::Result<()>>>,
) -> crate::runtime::Result<()> {
    let mut dependencies = Vec::new();
    let mut dependency_names = Vec::new();
    let mut params = crate::compile::schema::Params::new();
    for (name, param) in &sql.names.params {
        match param.expr.as_ref() {
            Expr::Materialize(MaterializeExpr {
                inlined: true,
                decl_name,
                ..
            }) => {
                if let Some(signal) = signals.get(decl_name) {
                    dependency_names.push(format!("{}", decl_name));
                    dependencies.push(signal.clone());
                } else {
                    params.insert(name.clone(), param.clone());
                }
            }
            _ => {
                params.insert(name.clone(), param.clone());
            }
        }
    }

    let completed_ref = signals
        .get(name)
        .expect("Should only call execute_create_view() on a pre-approved view")
        .clone();

    let mut ctx = ctx_pool.get();
    let url = url.clone();
    let name = name.clone();
    handles.push(tokio::spawn(async move {
        eprintln!(
            "View \"{}\"{}",
            name,
            if dependency_names.len() > 0 {
                format!(" (depends on: {})", dependency_names.join(", "))
            } else {
                " has no dependencies".into()
            }
        );

        for dep in dependencies {
            dep.await?;
        }

        eprintln!("Creating view \"{}\"", name);
        let sql_params = runtime::eval_params(&mut ctx, &params).await?;

        let result = ctx.sql_engine(url.clone())?.eval(&query, sql_params).await;

        completed_ref.unify(&mkcref(()))?;
        result?;
        Ok(())
    }));
    Ok(())
}

async fn process_view(
    ctx_pool: &ContextPool,
    name: &Ident,
    decl: &Decl<STypedExpr>,
    signals: &Signals,
    handles: &mut Vec<tokio::task::JoinHandle<crate::runtime::Result<()>>>,
) -> crate::runtime::Result<()> {
    let object_name = name.into();

    let expr = decl.value.expr.must()?;
    let expr = expr.read()?.to_runtime_type()?;
    match expr {
        Expr::SQL(ref sql, url) => {
            let query = create_view_as(object_name, sql.body.as_query()?);
            execute_create_view(ctx_pool, url.clone(), &name, &sql, query, signals, handles)?;
        }
        Expr::Materialize(MaterializeExpr { expr, url, .. }) => {
            match (url, expr.expr.as_ref()) {
                (Some(url), Expr::SQL(sql, Some(sql_url))) if url.as_ref() == sql_url.as_ref() => {
                    let query = create_table_as(object_name.into(), sql.body.as_query()?, false);
                    execute_create_view(
                        ctx_pool,
                        Some(url.clone()),
                        &name,
                        &sql,
                        query,
                        signals,
                        handles,
                    )?;
                }
                (Some(url), _) => {
                    let completed_ref = signals.get(name).unwrap().clone();

                    handles.push(tokio::spawn({
                        let mut ctx = ctx_pool.get();
                        let url = url.clone();
                        let type_ = expr.type_.read()?.clone();
                        async move {
                            let data =
                                runtime::eval(&mut ctx, &expr).await.context(RuntimeSnafu {
                                    loc: SourceLocation::Unknown,
                                })?;
                            let engine = ctx.sql_engine(Some(url)).context(RuntimeSnafu {
                                loc: SourceLocation::Unknown,
                            })?;

                            let result = engine
                                .load(&object_name, data, type_, false /*temporary*/)
                                .await
                                .context(RuntimeSnafu {
                                    loc: SourceLocation::Unknown,
                                });
                            completed_ref.unify(&mkcref(()))?;
                            result?;
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
        e => {
            panic!("Should have filtered out {:?} from materializations", e)
        }
    };

    Ok(())
}

pub async fn save_views(ctx_pool: &ContextPool, schema: SchemaRef) -> Result<()> {
    let locked_schema = schema.read()?;
    let signals = gather_materialize_candidates(&locked_schema.expr_decls)?;

    eprintln!("Processing views...\n--");
    let mut handles = Vec::new();
    let mut locations = Vec::new();
    for (name, decl) in locked_schema.expr_decls.iter() {
        if !signals.contains_key(name) {
            continue;
        }
        process_view(ctx_pool, name, decl, &signals, &mut handles)
            .await
            .context(RuntimeSnafu {
                loc: decl.location().clone(),
            })?;
        locations.push(decl.location().clone());
    }
    eprintln!("--\nWaiting for all views to complete...\n--");
    for (i, handle) in handles.into_iter().enumerate() {
        handle
            .await
            .expect("Failed to join task")
            .context(RuntimeSnafu {
                loc: locations[i].clone(),
            })?;
    }
    eprintln!("--\nSuccess!");

    Ok(())
}
