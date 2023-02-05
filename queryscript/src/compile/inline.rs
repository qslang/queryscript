use async_trait::async_trait;
use snafu::prelude::*;
use sqlparser::ast as sqlast;

use crate::ast::SourceLocation;
use crate::compile::error::*;
use crate::compile::schema::*;
use crate::compile::traverse::{SQLVisitor, Visit, VisitSQL, Visitor};

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

pub struct ContextInliner {
    context: BTreeMap<Ident, Arc<Expr<CRef<MType>>>>,
}

impl SQLVisitor for ContextInliner {}

#[async_trait]
impl Visitor<CRef<MType>> for ContextInliner {
    async fn visit_expr(&self, expr: &Expr<CRef<MType>>) -> Result<Option<Expr<CRef<MType>>>> {
        Ok(match expr {
            Expr::ContextRef(name) => {
                if let Some(c) = self.context.get(name) {
                    Some(c.as_ref().clone())
                } else {
                    None
                }
            }
            _ => None,
        })
    }
}

pub async fn inline_context(
    expr: Arc<Expr<CRef<MType>>>,
    context: BTreeMap<Ident, Arc<Expr<CRef<MType>>>>,
) -> Result<Arc<Expr<CRef<MType>>>> {
    let visitor = ContextInliner { context };
    Ok(Arc::new(expr.visit(&visitor).await?))
}

pub struct ParamInliner {
    context: BTreeMap<Ident, SQLBody>,
}

impl SQLVisitor for ParamInliner {
    fn visit_sqlexpr(&self, expr: &sqlast::Expr) -> Option<sqlast::Expr> {
        let ident = match expr {
            sqlast::Expr::Identifier(x) => x.clone(),
            sqlast::Expr::CompoundIdentifier(v) => {
                if v.len() != 1 {
                    return None;
                }

                v[0].clone()
            }
            _ => return None,
        }
        .get()
        .into();

        if let Some(e) = self.context.get(&ident) {
            Some(e.as_expr())
        } else {
            None
        }
    }

    fn visit_sqltable(&self, table: &sqlast::TableFactor) -> Option<sqlast::TableFactor> {
        match table {
            sqlast::TableFactor::Table {
                name, alias, args, ..
            } => {
                if name.0.len() != 1 || args.is_some() {
                    return None;
                }

                if let Some(e) = self.context.get(&name.0[0].get().into()) {
                    let new_alias = match alias {
                        Some(alias) => alias.clone(),
                        None => sqlast::TableAlias {
                            name: name.0[0].clone(),
                            columns: vec![],
                        },
                    };
                    Some(e.as_table(Some(new_alias)))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[async_trait]
impl Visitor<CRef<MType>> for ParamInliner {
    async fn visit_expr(&self, expr: &Expr<CRef<MType>>) -> Result<Option<Expr<CRef<MType>>>> {
        Ok(match expr {
            Expr::SQL(sql, url) => {
                let SQL { names, body } = sql.as_ref();
                let (mut names, params) = (
                    SQLNames {
                        params: BTreeMap::new(),
                        unbound: names.unbound.clone(),
                    },
                    names.params.clone(),
                );
                let mut context = BTreeMap::new();

                let mut inlined_params = Vec::new(); // Each paramater, after inlining
                let mut remaining_params = 0; // The aggregate number of remaining parameters
                let mut conn_strings = BTreeSet::new(); // The connection string for any remote SQL expressions

                if let Some(url) = url {
                    conn_strings.insert(url.clone());
                }

                for (_, param) in params.iter() {
                    let expr = inline_params(&param.expr.unwrap_schema_entry().await?)
                        .await?
                        .unwrap_schema_entry()
                        .await?;

                    match &expr {
                        Expr::SQL(sql, inner_url) => {
                            if let Some(inner_url) = inner_url {
                                conn_strings.insert(inner_url.clone());
                            }
                            remaining_params += sql.names.params.len();
                        }
                        Expr::Materialize(MaterializeExpr { url: inner_url, .. }) => {
                            if let Some(inner_url) = inner_url {
                                conn_strings.insert(inner_url.clone());
                            }
                        }
                        _ => {
                            remaining_params += 1;
                        }
                    }

                    inlined_params.push(expr);
                }

                let can_inline_tables = remaining_params == 0 && conn_strings.len() <= 1;

                for ((name, param), expr) in params.into_iter().zip(inlined_params) {
                    match &expr {
                        // Only inline SQL expressions that point to the same database.
                        Expr::SQL(sql, inner_url)
                            if matches!(inner_url, None) || can_inline_tables =>
                        {
                            names.extend(sql.names.clone());
                            context.insert(name.clone(), sql.body.clone());
                        }
                        Expr::Materialize(MaterializeExpr {
                            expr,
                            key,
                            url,
                            decl_name,
                            inlined,
                        }) => {
                            // If we can inline tables, then we can inline materialized expressions (we simply expect them
                            // to have been saved to the database at some point).
                            let mut inlined = false;
                            if can_inline_tables {
                                context.insert(
                                    name.clone(),
                                    SQLBody::Table(sqlast::TableFactor::Table {
                                        alias: None,
                                        name: sqlast::ObjectName(vec![sqlast::Located::new(
                                            decl_name.into(),
                                            None,
                                        )]),
                                        args: None,
                                        with_hints: vec![],
                                    }),
                                );
                                inlined = true;
                            }

                            // Either way, we still want to keep the materialized expression in the param list, to
                            // force ourselves to either compute or resolve it prior to executing this one
                            names.params.insert(
                                name.clone(),
                                TypedExpr {
                                    type_: param.type_.clone(),
                                    expr: Arc::new(Expr::Materialize(MaterializeExpr {
                                        expr: expr.clone(),
                                        key: key.clone(),
                                        url: url.clone(),
                                        decl_name: decl_name.clone(),
                                        inlined,
                                    })),
                                },
                            );
                        }
                        _ => {
                            names.params.insert(
                                name.clone(),
                                TypedExpr {
                                    type_: param.type_.clone(),
                                    expr: Arc::new(expr),
                                },
                            );
                        }
                    }
                }

                let url = if can_inline_tables {
                    conn_strings.into_iter().next()
                } else {
                    url.clone()
                };

                let visitor = ParamInliner { context };
                let body = body.visit_sql(&visitor);
                Some(Expr::SQL(Arc::new(SQL { names, body }), url))
            }
            Expr::Materialize(MaterializeExpr {
                expr,
                key,
                url,
                decl_name,
                inlined,
            }) => {
                let expr = TypedExpr {
                    type_: expr.type_.clone(),
                    expr: Arc::new(inline_params(&expr.expr).await?),
                };

                let url = match url {
                    Some(url) => Some(url.clone()),
                    None => match expr.expr.as_ref() {
                        Expr::SQL(_, url) => url.clone(),
                        Expr::Materialize(MaterializeExpr { url, .. }) => url.clone(),
                        _ => None,
                    },
                };
                Some(Expr::Materialize(MaterializeExpr {
                    expr,
                    key: key.clone(),
                    url: url.clone(),
                    decl_name: decl_name.clone(),
                    inlined: *inlined,
                }))
            }
            _ => None,
        })
    }
}

pub async fn inline_params(expr: &Expr<CRef<MType>>) -> Result<Expr<CRef<MType>>> {
    let visitor = ParamInliner {
        context: BTreeMap::new(),
    };
    Ok(expr.visit(&visitor).await?)
}
