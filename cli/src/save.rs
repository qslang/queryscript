// This file is responsible for the QueryScript equivalent of orchestration: saving "views"
// back to the original database.
use std::collections::HashMap;
use std::sync::Arc;

use queryscript::compile::{
    schema::{Expr, MaterializeExpr},
    sql::{create_table_as, create_view_as},
    SchemaRef,
};
use queryscript::runtime::{new_engine, Result, SQLEngineType};

pub async fn save_views(
    schema: SchemaRef,
    engine_type: queryscript::runtime::SQLEngineType,
) -> Result<()> {
    let ctx = Arc::new(queryscript::runtime::Context::new(
        schema.read()?.folder.clone(),
        engine_type,
    ));
    let mut handles = Vec::new();
    for (name, decl) in schema.read()?.expr_decls.iter() {
        if !decl.public {
            continue;
        }

        let expr = decl.value.expr.must()?;
        let expr = expr.read()?;
        match expr.to_runtime_type()? {
            Expr::SQL(ref sql, url) => {
                let engine = match &url {
                    Some(url) => new_engine(SQLEngineType::from_name(url.engine_name())?),
                    None => {
                        eprintln!(
                            "Skipping view \"{}\" because it does not belong to a database",
                            name
                        );
                        continue;
                    }
                };

                if !sql.names.params.is_empty() {
                    eprintln!(
                        "Skipping view \"{}\" because it has parameters that must be evaluated outside the database",
                        name
                    );
                    continue;
                }

                let query = create_view_as(name.into(), sql.body.as_query());

                eprintln!("Creating view \"{}\"", name);
                handles.push(tokio::spawn({
                    let ctx = ctx.clone();
                    let url = url.clone();
                    async move { engine.eval(&ctx, url.clone(), &query, HashMap::new()).await }
                }));
            }
            Expr::Materialize(MaterializeExpr { key, expr, url }) => {
                let (sql, sql_url) = match expr.expr.as_ref() {
                    Expr::SQL(sql, sql_url) => (sql.clone(), sql_url.clone()),
                    e => {
                        eprintln!(
                            "Skipping view \"{}\" because it is not a SQL expression... (found {:?})",
                            name, e
                        );
                        continue;
                    }
                };

                match (&url, &sql_url) {
                    (Some(url), Some(sql_url)) if url.as_ref() == sql_url.as_ref() => {}
                    _ => {
                        eprintln!(
                            "Skipping materialized view \"{}\" because it is not in the same database as its source",
                            name
                        );
                        continue;
                    }
                };

                let query = create_table_as(name.into(), sql.body.as_query(), false);

                // XXX: The below can be refactored into a function with the same logic as above
                let engine = match &url {
                    Some(url) => new_engine(SQLEngineType::from_name(url.engine_name())?),
                    None => {
                        eprintln!(
                            "Skipping view \"{}\" because it does not belong to a database",
                            name
                        );
                        continue;
                    }
                };

                if !sql.names.params.is_empty() {
                    eprintln!(
                        "Skipping materialized view \"{}\" because it has parameters that must be evaluated outside the database",
                        name
                    );
                    continue;
                }

                eprintln!("Creating materialized table \"{}\"", name);
                handles.push(tokio::spawn({
                    let ctx = ctx.clone();
                    let url = url.clone();
                    async move { engine.eval(&ctx, url.clone(), &query, HashMap::new()).await }
                }));
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
