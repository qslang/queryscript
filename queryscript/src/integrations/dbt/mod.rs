use snafu::prelude::*;
use std::{cell::RefCell, collections::HashSet};

use crate::{
    ast,
    ast::{sqlast, SourceLocation, ToPath},
    compile::{
        error::{JSONSnafu, RuntimeSnafu},
        schema::{Decl, DeclMap, Expr, Ident, MaterializeExpr, Ref, STypedExpr},
        sql::IntoTableFactor,
        traverse::{SQLVisitor, VisitSQL},
        CompileError, SchemaRef,
    },
    runtime::RuntimeError,
    types::Type,
};

#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SimpleExpr {
    pub name: Ident,
    pub code: String,
    pub deps: Vec<Ident>,
}
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SimpleSchema {
    pub exprs: Vec<SimpleExpr>,
}

fn process_expr(
    referenceable_names: &HashSet<Ident>,
    name: &Ident,
    expr: &Expr<Ref<Type>>,
) -> Result<SimpleExpr, RuntimeError> {
    Ok(match expr {
        Expr::SQL(expr, _) => {
            let snippet = expr.body.as_query()?;
            let names = NameCollector::new(referenceable_names);
            let snippet = snippet.visit_sql(&names);

            SimpleExpr {
                name: name.clone(),
                code: snippet.to_string(),
                deps: names.names.take().into_iter().collect(),
            }
        }
        Expr::Materialize(MaterializeExpr { expr, .. }) => {
            process_expr(referenceable_names, name, &expr.expr)?
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
    let mut ret = HashSet::new();
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
            Expr::SQL(..) | Expr::Materialize(..) => {
                ret.insert(name.clone());
            }
            _ => continue,
        }
    }

    Ok(ret)
}

pub fn print_hacky_parse(schema: SchemaRef) -> Result<(), CompileError> {
    // First, gather all decl names that are referenceable in the schema
    let referenceable_names = {
        let mut names = HashSet::new();
        for name in schema.read()?.expr_decls.keys() {
            names.insert(name.clone());
        }
        for name in schema.read()?.schema_decls.keys() {
            names.insert(name.clone());
        }
        names
    };

    let candidates = gather_dbt_candidates(&schema.read()?.expr_decls)?;

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

    println!(
        "{}",
        serde_json::to_string_pretty(&SimpleSchema { exprs }).context(JSONSnafu {
            loc: SourceLocation::File(schema.read()?.file.clone()),
        })?
    );

    Ok(())
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
                    /*
                    let ret = sqlast::Located::new(
                        sqlast::Ident {
                            value: format!("{{{{ ref('{}') }}}}", &ident),
                            quote_style: None,
                        },
                        None,
                    )
                    .to_table_factor();
                     */
                    self.names.borrow_mut().insert(ident);
                    // Some(ret)
                    None
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
