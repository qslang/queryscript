use crate::ast;
use crate::compile::compile::{compile_reference, resolve_global_atom};
use crate::compile::error::{CompileError, Result};
use crate::compile::inference::*;
use crate::compile::schema::*;
use crate::types::{number::parse_numeric_type, AtomicType};
use sqlparser::ast as sqlast;

use std::collections::BTreeMap;
use std::rc::Rc;

const QVM_NAMESPACE: &str = "__qvm";

pub fn new_placeholder_name(schema: Ref<Schema>, kind: &str) -> String {
    let placeholder = schema.borrow().next_placeholder;
    schema.borrow_mut().next_placeholder += 1;
    format!("{}{}", kind, placeholder)
}

pub fn intern_placeholder(
    schema: Ref<Schema>,
    kind: &str,
    expr: TypedExpr<CRef<MType>>,
) -> Result<TypedSQLExpr<CRef<MType>>> {
    match &*expr.expr {
        Expr::SQLExpr(sqlexpr) => Ok(TypedSQLExpr {
            type_: expr.type_,
            expr: sqlexpr.clone(),
        }),
        _ => {
            let placeholder_name = "@".to_string() + new_placeholder_name(schema, kind).as_str();

            Ok(TypedSQLExpr {
                type_: expr.type_.clone(),
                expr: Rc::new(SQLExpr {
                    params: Params::from([(placeholder_name.clone(), expr)]),
                    expr: sqlast::Expr::Identifier(sqlast::Ident {
                        value: placeholder_name.clone(),
                        quote_style: None,
                    }),
                }),
            })
        }
    }
}

pub fn compile_sqlarg(
    schema: Ref<Schema>,
    expr: &sqlast::Expr,
) -> Result<TypedSQLExpr<CRef<MType>>> {
    Ok(intern_placeholder(
        schema.clone(),
        "param",
        compile_sqlexpr(schema.clone(), expr)?,
    )?)
}

pub fn combine_sqlparams(
    all: Vec<&TypedSQLExpr<CRef<MType>>>,
) -> Result<BTreeMap<ast::Ident, TypedExpr<CRef<MType>>>> {
    Ok(all
        .iter()
        .map(|t| t.expr.params.clone())
        .flatten()
        .collect())
}

pub fn compile_select(schema: Ref<Schema>, select: &sqlast::Select) -> Result<CTypedExpr> {
    if select.distinct {
        return Err(CompileError::unimplemented("DISTINCT"));
    }

    if select.top.is_some() {
        return Err(CompileError::unimplemented("TOP"));
    }

    if select.into.is_some() {
        return Err(CompileError::unimplemented("INTO"));
    }

    if select.lateral_views.len() > 0 {
        return Err(CompileError::unimplemented("Lateral views"));
    }

    if select.selection.is_some() {
        return Err(CompileError::unimplemented("WHERE"));
    }

    if select.group_by.len() > 0 {
        return Err(CompileError::unimplemented("GROUP BY"));
    }

    if select.cluster_by.len() > 0 {
        return Err(CompileError::unimplemented("CLUSTER BY"));
    }

    if select.distribute_by.len() > 0 {
        return Err(CompileError::unimplemented("DISTRIBUTE BY"));
    }

    if select.sort_by.len() > 0 {
        return Err(CompileError::unimplemented("ORDER BY"));
    }

    if select.having.is_some() {
        return Err(CompileError::unimplemented("HAVING"));
    }

    if select.qualify.is_some() {
        return Err(CompileError::unimplemented("QUALIFY"));
    }

    match select.from.len() {
        0 => {
            if select.projection.len() != 1 {
                return Err(CompileError::unimplemented(
                    "Multiple projections in projection without FROM",
                ));
            }

            match &select.projection[0] {
                sqlast::SelectItem::UnnamedExpr(e) => Ok(compile_sqlexpr(schema.clone(), e)?),
                sqlast::SelectItem::ExprWithAlias { expr, .. } => {
                    Ok(compile_sqlexpr(schema.clone(), expr)?)
                }
                _ => Err(CompileError::unimplemented(
                    "Wildcard in SELECT without FROM",
                )),
            }
        }
        1 => {
            if select.from[0].joins.len() > 0 {
                return Err(CompileError::unimplemented("JOIN"));
            }

            if select.projection.len() != 1
                || !matches!(select.projection[0], sqlast::SelectItem::Wildcard)
            {
                return Err(CompileError::unimplemented("Projections with FROM"));
            }

            match &select.from[0].relation {
                sqlast::TableFactor::Table {
                    name,
                    alias,
                    args,
                    with_hints,
                } => {
                    if args.is_some() {
                        return Err(CompileError::unimplemented("Table valued functions"));
                    }

                    if with_hints.len() > 0 {
                        return Err(CompileError::unimplemented("WITH hints"));
                    }

                    let relation = compile_reference(schema.clone(), &name.0)?;

                    let list_type = mkcref(MType::List(MType::new_unknown(
                        format!("FROM {}", name.to_string()).as_str(),
                    )));
                    list_type.unify(&relation.type_)?;

                    let placeholder_name = QVM_NAMESPACE.to_string()
                        + new_placeholder_name(schema.clone(), "rel").as_str();

                    let mut ret = select.clone();
                    ret.from = vec![sqlast::TableWithJoins {
                        relation: sqlast::TableFactor::Table {
                            name: sqlast::ObjectName(vec![sqlast::Ident {
                                value: placeholder_name.clone(),
                                quote_style: None,
                            }]),
                            alias: alias.clone(),
                            args: args.clone(),
                            with_hints: with_hints.clone(),
                        },
                        joins: Vec::new(),
                    }];

                    Ok(TypedExpr {
                        type_: relation.type_.clone(),
                        expr: Rc::new(Expr::SQLQuery(Rc::new(SQLQuery {
                            params: Params::from([(
                                placeholder_name.clone(),
                                TypedExpr {
                                    type_: relation.type_,
                                    expr: relation.expr,
                                },
                            )]),
                            query: sqlast::Query {
                                with: None,
                                body: Box::new(sqlast::SetExpr::Select(Box::new(ret))),
                                order_by: Vec::new(),
                                limit: None,
                                offset: None,
                                fetch: None,
                                lock: None,
                            },
                        }))),
                    })
                }
                sqlast::TableFactor::Derived { .. } => {
                    Err(CompileError::unimplemented("Subqueries"))
                }
                sqlast::TableFactor::TableFunction { .. } => {
                    Err(CompileError::unimplemented("TABLE"))
                }
                sqlast::TableFactor::UNNEST { .. } => Err(CompileError::unimplemented("UNNEST")),
                sqlast::TableFactor::NestedJoin { .. } => Err(CompileError::unimplemented("JOIN")),
            }
        }
        _ => Err(CompileError::unimplemented("JOIN")),
    }
}

pub fn compile_sqlquery(schema: Ref<Schema>, query: &sqlast::Query) -> Result<CTypedExpr> {
    if query.with.is_some() {
        return Err(CompileError::unimplemented("WITH"));
    }

    if query.order_by.len() > 0 {
        return Err(CompileError::unimplemented("ORDER BY"));
    }

    if query.limit.is_some() {
        return Err(CompileError::unimplemented("LIMIT"));
    }

    if query.offset.is_some() {
        return Err(CompileError::unimplemented("OFFSET"));
    }

    if query.fetch.is_some() {
        return Err(CompileError::unimplemented("FETCH"));
    }

    if query.lock.is_some() {
        return Err(CompileError::unimplemented("FOR { UPDATE | SHARE }"));
    }

    match query.body.as_ref() {
        sqlast::SetExpr::Select(s) => compile_select(schema.clone(), s),
        sqlast::SetExpr::Query(q) => compile_sqlquery(schema.clone(), q),
        sqlast::SetExpr::SetOperation { .. } => {
            Err(CompileError::unimplemented("UNION | EXCEPT | INTERSECT"))
        }
        sqlast::SetExpr::Values(_) => Err(CompileError::unimplemented("VALUES")),
        sqlast::SetExpr::Insert(_) => Err(CompileError::unimplemented("INSERT")),
    }
}

pub fn compile_sqlexpr(schema: Ref<Schema>, expr: &sqlast::Expr) -> Result<CTypedExpr> {
    let ret = match expr {
        sqlast::Expr::Value(v) => match v {
            sqlast::Value::Number(n, _) => TypedExpr {
                type_: mkcref(MType::Atom(parse_numeric_type(n)?)),
                expr: Rc::new(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::SingleQuotedString(_)
            | sqlast::Value::EscapedStringLiteral(_)
            | sqlast::Value::NationalStringLiteral(_)
            | sqlast::Value::HexStringLiteral(_)
            | sqlast::Value::DoubleQuotedString(_) => TypedExpr {
                type_: resolve_global_atom("string")?,
                expr: Rc::new(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::Boolean(_) => TypedExpr {
                type_: resolve_global_atom("bool")?,
                expr: Rc::new(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::Null => TypedExpr {
                type_: resolve_global_atom("null")?,
                expr: Rc::new(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::Placeholder(_) => {
                return Err(CompileError::unimplemented(
                    format!("SQL Parameter syntax: {}", expr).as_str(),
                ))
            }
        },
        sqlast::Expr::Array(sqlast::Array { elem, .. }) => {
            let c_elems: Vec<CTypedExpr> = elem
                .iter()
                .map(|e| compile_sqlexpr(schema.clone(), e))
                .collect::<Result<Vec<_>>>()?;
            let mut c_elem_iter = c_elems.iter();
            let data_type = if let Some(first) = c_elem_iter.next() {
                for next in c_elem_iter {
                    first.type_.unify(&next.type_)?;
                }
                first.type_.clone()
            } else {
                mkcref(MType::Atom(AtomicType::Null))
            };

            TypedExpr {
                type_: data_type,
                expr: Rc::new(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            }
        }
        sqlast::Expr::BinaryOp { left, op, right } => {
            let cleft = compile_sqlarg(schema.clone(), left)?;
            let cright = compile_sqlarg(schema.clone(), right)?;
            let params = combine_sqlparams(vec![&cleft, &cright])?;
            let type_ = match op {
                sqlast::BinaryOperator::Plus => {
                    cleft.type_.unify(&cright.type_)?;
                    cleft.type_
                }
                _ => {
                    return Err(CompileError::unimplemented(
                        format!("Binary operator: {}", op).as_str(),
                    ));
                }
            };
            TypedExpr {
                type_,
                expr: Rc::new(Expr::SQLExpr(Rc::new(SQLExpr {
                    params,
                    expr: sqlast::Expr::BinaryOp {
                        left: Box::new(cleft.expr.expr.clone()),
                        op: op.clone(),
                        right: Box::new(cright.expr.expr.clone()),
                    },
                }))),
            }
        }
        sqlast::Expr::Function(sqlast::Function {
            name,
            args,
            over,
            distinct,
            ..
        }) => {
            if *distinct {
                return Err(CompileError::unimplemented(
                    format!("Function calls with DISTINCT").as_str(),
                ));
            }

            if over.is_some() {
                return Err(CompileError::unimplemented(
                    format!("Function calls with OVER").as_str(),
                ));
            }

            let func = compile_reference(schema.clone(), &name.0)?;
            let fn_type = match func.type_.must()?.borrow().clone() {
                MType::Fn(f) => f,
                _ => {
                    return Err(CompileError::wrong_type(
                        &MType::Fn(MFnType {
                            args: Vec::new(),
                            ret: MType::new_unknown("ret"),
                        }),
                        &*func.type_.must()?.borrow(),
                    ))
                }
            };
            let mut compiled_args: BTreeMap<String, TypedNameAndSQLExpr<CRef<MType>>> =
                BTreeMap::new();
            let mut pos: usize = 0;
            for arg in args {
                let (name, expr) = match arg {
                    sqlast::FunctionArg::Named { name, arg } => (name.value.clone(), arg),
                    sqlast::FunctionArg::Unnamed(arg) => {
                        if pos >= fn_type.args.len() {
                            return Err(CompileError::no_such_entry(vec![format!(
                                "argument {}",
                                pos
                            )]));
                        }
                        pos += 1;
                        (fn_type.args[pos - 1].name.clone(), arg)
                    }
                };

                let expr = match expr {
                    sqlast::FunctionArgExpr::Expr(e) => e,
                    _ => {
                        return Err(CompileError::unimplemented(
                            format!("Weird function arguments").as_str(),
                        ))
                    }
                };

                if compiled_args.get(&name).is_some() {
                    return Err(CompileError::duplicate_entry(vec![name.clone()]));
                }

                let compiled_arg = compile_sqlarg(schema.clone(), &expr)?;
                compiled_args.insert(
                    name.clone(),
                    TypedNameAndSQLExpr {
                        name: name.clone(),
                        type_: compiled_arg.type_.clone(),
                        expr: compiled_arg.expr.clone(),
                    },
                );
            }

            let mut arg_sqlexprs = Vec::new();
            for arg in &fn_type.args {
                if let Some(compiled_arg) = compiled_args.get_mut(&arg.name) {
                    arg.type_.unify(&compiled_arg.type_)?;
                    arg_sqlexprs.push(compiled_arg.clone());
                } else {
                    return Err(CompileError::no_such_entry(vec![arg.name.clone()]));
                }
            }

            // TODO: Once we start binding SQL queries, we need to detect whether the arguments
            // contain any references to values within the SQL query and avoid lifting the function
            // expression if they do.
            //
            let expr = if true {
                let arg_exprs: Vec<_> = arg_sqlexprs
                    .iter()
                    .map(|e| TypedExpr {
                        type_: e.type_.clone(),
                        expr: Rc::new(Expr::SQLExpr(e.expr.clone())),
                    })
                    .collect();
                Rc::new(Expr::FnCall(FnCallExpr {
                    func: Rc::new(TypedExpr {
                        type_: mkcref(MType::Fn(fn_type.clone())),
                        expr: func.expr,
                    }),
                    args: arg_exprs,
                }))
            } else {
                let args_nonames = arg_sqlexprs
                    .iter()
                    .map(|e| TypedSQLExpr {
                        type_: e.type_.clone(),
                        expr: e.expr.clone(),
                    })
                    .collect::<Vec<_>>();
                let mut params = combine_sqlparams(args_nonames.iter().collect())?;

                let placeholder_name = QVM_NAMESPACE.to_string()
                    + new_placeholder_name(schema.clone(), "func").as_str();

                params.insert(
                    placeholder_name.clone(),
                    TypedExpr {
                        expr: func.expr,
                        type_: fn_type.ret.clone(),
                    },
                );

                Rc::new(Expr::SQLExpr(Rc::new(SQLExpr {
                    params,
                    expr: sqlast::Expr::Function(sqlast::Function {
                        name: sqlast::ObjectName(vec![sqlast::Ident {
                            value: placeholder_name.clone(),
                            quote_style: None,
                        }]),
                        args: arg_sqlexprs
                            .iter()
                            .map(|e| sqlast::FunctionArg::Named {
                                name: sqlast::Ident {
                                    value: e.name.clone(),
                                    quote_style: None,
                                },
                                arg: sqlast::FunctionArgExpr::Expr(e.expr.expr.clone()),
                            })
                            .collect(),
                        over: None,
                        distinct: false,
                        special: false,
                    }),
                })))
            };

            let type_ = fn_type.ret.clone();

            // XXX There are cases here where we could inline eagerly
            //
            TypedExpr { type_, expr }
        }
        sqlast::Expr::CompoundIdentifier(sqlpath) => {
            // XXX There are cases here where we could inline eagerly
            //
            compile_reference(schema.clone(), sqlpath)?
        }
        sqlast::Expr::Identifier(ident) => {
            // XXX There are cases here where we could inline eagerly
            //
            compile_reference(schema.clone(), &vec![ident.clone()])?
        }
        _ => {
            return Err(CompileError::unimplemented(
                format!("Expression: {:?}", expr).as_str(),
            ))
        }
    };

    Ok(ret)
}
