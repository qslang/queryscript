// This file is responsible for the QueryScript equivalent of orchestration: saving "views"
// back to the original database.
use snafu::prelude::*;
use sqlparser::ast as sqlast;
use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

use crate::compile::inference::mkcref;
use crate::compile::schema::{CRef, SQLBody};
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

fn execute_create_view(
    ctx_pool: &ContextPool,
    url: Option<Arc<ConnectionString>>,
    name: &Ident,
    sql: &Arc<SQLSnippet<Arc<RwLock<Type>>, SQLBody>>,
    query: sqlparser::ast::Statement,
    signals: &mut Signals,
    handles: &mut Vec<JoinHandle<Result<()>>>,
) -> crate::runtime::Result<()> {
    let mut dependencies = Vec::new();
    let mut dependency_names = Vec::new();
    for (name, param) in &sql.names.params {
        match param.expr.as_ref() {
            Expr::Materialize(MaterializeExpr {
                inlined: true,
                decl_name,
                ..
            }) => {
                dependency_names.push(format!("{}", decl_name));
                dependencies.push(signals.get(decl_name));
            }
            _ => {
                eprintln!("Skipping \"{}\" because it has parameters that must be evaluated outside the database", name);
            }
        }
    }

    let completed_ref = signals.get(name);
    let mut ctx = ctx_pool.get();
    let url = url.clone();
    let name = name.clone();
    handles.push(tokio::spawn(async move {
        for dep in dependencies {
            dep.await?;
        }

        eprintln!(
            "Creating view \"{}\"{}",
            name,
            if dependency_names.len() > 0 {
                format!(" (depends on: {})", dependency_names.join(", "))
            } else {
                "".into()
            }
        );

        let result = ctx
            .sql_engine(url.clone())
            .context(RuntimeSnafu {
                loc: SourceLocation::Unknown,
            })?
            .eval(&query, HashMap::new())
            .await;

        completed_ref.unify(&mkcref(()))?;
        result.context(RuntimeSnafu {
            loc: SourceLocation::Unknown,
        })?;
        Ok(())
    }));
    Ok(())
}

async fn process_view<'a>(
    ctx_pool: &'a ContextPool,
    schema: SchemaRef,
    name: &'a Ident,
    signals: &'a mut Signals,
    handles: &'a mut Vec<tokio::task::JoinHandle<Result<()>>>,
) -> crate::runtime::Result<()> {
    let object_name = sqlast::ObjectName(vec![sqlast::Located::new(name.into(), None)]);

    let decl = schema
        .read()?
        .expr_decls
        .get(name)
        .ok_or_else(|| runtime::error::RuntimeError::new(&format!("No such view \"{}\"", name)))?
        .clone();

    if !decl.public {
        return Ok(());
    }

    let expr = decl.value.expr.must()?;
    let expr = expr.read()?.to_runtime_type()?;
    match expr {
        Expr::SQL(ref sql, url) => {
            if url.is_none() {
                eprintln!(
                    "Skipping \"{}\" because it does not belong to a database",
                    name
                );
                return Ok(());
            }

            let query = create_view_as(object_name, sql.body.as_query());
            execute_create_view(ctx_pool, url.clone(), &name, &sql, query, signals, handles)?;
        }
        Expr::Materialize(MaterializeExpr { expr, url, .. }) => {
            match (url, expr.expr.as_ref()) {
                (Some(url), Expr::SQL(sql, Some(sql_url))) if url.as_ref() == sql_url.as_ref() => {
                    let query = create_table_as(object_name.into(), sql.body.as_query(), false);
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
                    let completed_ref = signals.get(name);

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
        _ => {
            eprintln!(
                "Skipping view \"{}\" because it is not a SQL expression...",
                name
            );
        }
    };

    Ok(())
}

pub async fn save_views(ctx_pool: &ContextPool, schema: SchemaRef) -> Result<()> {
    let mut handles = Vec::new();
    let mut signals = Signals::new();
    eprintln!("Processing views...");
    for (name, decl) in schema.read()?.expr_decls.iter() {
        process_view(ctx_pool, schema.clone(), name, &mut signals, &mut handles)
            .await
            .context(RuntimeSnafu {
                loc: decl.location().clone(),
            })?;
    }
    eprintln!("Waiting for all views to complete...\n--");
    for handle in handles {
        handle.await.expect("Failed to join task")?;
    }
    eprintln!("--\nSuccess!");

    Ok(())
}

struct Signals(HashMap<Ident, CRef<()>>);
impl Signals {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn get(&mut self, name: &Ident) -> CRef<()> {
        self.0
            .entry(name.clone())
            .or_insert_with(|| CRef::new_unknown(&name.to_string()))
            .clone()
    }
}
