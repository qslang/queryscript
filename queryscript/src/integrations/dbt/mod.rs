use snafu::prelude::*;
use std::{cell::RefCell, collections::HashSet};

use pyo3::prelude::*;

use crate::{
    ast::sqlast,
    compile::{
        error::RuntimeSnafu,
        schema::{Decl, DeclMap, Expr, Ident, MaterializeExpr, Ref, STypedExpr},
        sql::IntoTableFactor,
        traverse::{SQLVisitor, VisitSQL},
        CompileError, SchemaRef,
    },
    materialize::gather_materialize_candidates,
    runtime::{normalize, RuntimeError},
    types::Type,
};

#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[pyclass]
#[derive(Clone)]
pub struct SimpleExpr {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub code: String,
    #[pyo3(get, set)]
    pub materialized: Option<String>,
    #[pyo3(get, set)]
    pub deps: Vec<String>,
}
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[pyclass]
#[derive(Clone)]
pub struct SimpleSchema {
    #[pyo3(get, set)]
    pub exprs: Vec<SimpleExpr>,
}

fn process_expr(
    referenceable_names: &HashSet<Ident>,
    name: &Ident,
    expr: &Expr<Ref<Type>>,
) -> Result<SimpleExpr, RuntimeError> {
    Ok(match expr {
        Expr::SQL(expr, url) => {
            let url = url.clone().expect("all exported decls should have a URL");
            let snippet = expr.body.as_statement()?;
            let names = NameCollector::new(referenceable_names);

            let normalized = normalize(url.engine_type(), &snippet)?;
            let snippet = normalized.visit_sql(&names);

            SimpleExpr {
                name: name.into(),
                code: snippet.to_string(),
                materialized: Some("view".to_string()),
                deps: names
                    .names
                    .take()
                    .into_iter()
                    .map(|i| (&i).into())
                    .collect(),
            }
        }
        Expr::Materialize(MaterializeExpr { expr, .. }) => {
            let mut expr = process_expr(referenceable_names, name, &expr.expr)?;
            expr.materialized = Some("table".to_string());
            expr
        }
        _ => {
            unreachable!("gather_materialize_candidates should only return SQL or MaterializeExpr")
        }
    })
}

fn process_decl(
    referenceable_names: &HashSet<Ident>,
    name: &Ident,
    decl: &Decl<STypedExpr>,
) -> Result<SimpleExpr, RuntimeError> {
    let expr = decl.value.expr.must()?;
    let expr = expr.read()?.to_runtime_type()?;
    process_expr(referenceable_names, name, &expr)
}

pub fn gather_dbt_candidates(decls: &DeclMap<STypedExpr>) -> Result<HashSet<Ident>, CompileError> {
    Ok(gather_materialize_candidates(decls)?.into_keys().collect())
}

pub fn extract_metadata(schema: SchemaRef) -> Result<SimpleSchema, CompileError> {
    let candidates = gather_dbt_candidates(&schema.read()?.expr_decls)?;
    let referenceable_names = HashSet::new();

    let mut exprs = Vec::new();
    for (name, decl) in schema.read()?.expr_decls.iter() {
        if !candidates.contains(name) {
            continue;
        }

        exprs.push(
            process_decl(&referenceable_names, name, decl).context(RuntimeSnafu {
                loc: decl.location().clone(),
            })?,
        );
    }

    Ok(SimpleSchema { exprs })
}

struct NameCollector<'a> {
    referenceable_names: &'a HashSet<Ident>,
    names: RefCell<HashSet<Ident>>,
}

impl<'a> NameCollector<'a> {
    pub fn new(referenceable_names: &'a HashSet<Ident>) -> Self {
        Self {
            referenceable_names,
            names: RefCell::new(HashSet::new()),
        }
    }
}

impl<'a> SQLVisitor for NameCollector<'a> {
    fn visit_sqltable(&self, table: &sqlast::TableFactor) -> Option<sqlast::TableFactor> {
        match table {
            sqlast::TableFactor::Table { name, args, .. } => {
                if name.0.len() != 1 || args.is_some() {
                    return None;
                }

                let ident = name.0[0].get().into();
                if !self.referenceable_names.contains(&ident) {
                    // We _may_ want to emit refs() here, but I think we can get away with just declaring the dependencies
                    let ret = sqlast::Located::new(
                        sqlast::Ident {
                            value: format!("{{{{ ref('{}') }}}}", &ident),
                            quote_style: None,
                        },
                        None,
                    )
                    .to_table_factor();
                    self.names.borrow_mut().insert(ident);
                    Some(ret)
                    // None
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
