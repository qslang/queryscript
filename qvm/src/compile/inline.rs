use async_trait::async_trait;
use sqlparser::ast as sqlast;

use crate::compile::error::*;
use crate::compile::schema::*;
use crate::compile::traverse::{SQLVisitor, Visit, VisitSQL, Visitor};

use std::collections::BTreeMap;
use std::sync::Arc;

pub struct ContextInliner {
    context: BTreeMap<String, Arc<Expr<CRef<MType>>>>,
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
    context: BTreeMap<String, Arc<Expr<CRef<MType>>>>,
) -> Result<Arc<Expr<CRef<MType>>>> {
    let visitor = ContextInliner { context };
    Ok(Arc::new(expr.visit(&visitor).await?))
}

pub struct ParamInliner {
    context: BTreeMap<String, SQLBody>,
}

impl SQLVisitor for ParamInliner {
    fn visit_sqlexpr(&self, expr: &sqlast::Expr) -> Option<sqlast::Expr> {
        let ident = match expr {
            sqlast::Expr::Identifier(x) => x.value.clone(),
            sqlast::Expr::CompoundIdentifier(v) => {
                if v.len() != 1 {
                    return None;
                }

                v[0].value.clone()
            }
            _ => return None,
        };

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

                if let Some(e) = self.context.get(&name.0[0].value) {
                    Some(e.as_table(alias.clone()))
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
            Expr::SQL(sql) => {
                let SQL { names, body } = sql.as_ref();
                let (mut names, params) = (
                    SQLNames {
                        params: BTreeMap::new(),
                        unbound: names.unbound.clone(),
                    },
                    names.params.clone(),
                );
                let mut context = BTreeMap::new();
                for (name, param) in params {
                    let expr = inline_params(param.expr.unwrap_schema_entry().await?).await?;
                    match expr.as_ref() {
                        Expr::SQL(sql) => {
                            names.extend(sql.names.clone());
                            context.insert(name.clone(), sql.body.clone());
                        }
                        _ => {
                            names.params.insert(
                                name.clone(),
                                TypedExpr {
                                    type_: param.type_.clone(),
                                    expr,
                                },
                            );
                        }
                    }
                }

                let visitor = ParamInliner { context };
                let body = body.visit_sql(&visitor);
                Some(Expr::SQL(Arc::new(SQL { names, body })))
            }
            _ => None,
        })
    }
}

pub async fn inline_params(expr: Arc<Expr<CRef<MType>>>) -> Result<Arc<Expr<CRef<MType>>>> {
    let visitor = ParamInliner {
        context: BTreeMap::new(),
    };
    Ok(Arc::new(expr.visit(&visitor).await?))
}
