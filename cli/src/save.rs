// This file is responsible for the QueryScript equivalent of orchestration: saving "views"
// back to the original database.
use std::collections::HashMap;
use std::sync::Arc;

use queryscript::compile::{schema::Expr, sql::create_view_as, SchemaRef};
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
        match &*expr {
            Expr::SQL(ref sql, url) => {
                let engine = match url {
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

                eprintln!("Inlining \"{}\"", name);
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
    }
    eprintln!("--\nWaiting for all views to complete...");
    for handle in handles {
        handle.await.expect("Failed to join task")?;
    }
    eprintln!("Success!");

    Ok(())
}
