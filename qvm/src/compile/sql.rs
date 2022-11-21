use crate::compile::compile::{lookup_path, resolve_global_atom, typecheck_path};
use crate::compile::error::{CompileError, Result};
use crate::compile::inference::*;
use crate::compile::schema::*;
use crate::types::{number::parse_numeric_type, AtomicType};
use sqlparser::ast as sqlast;

use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

const QVM_NAMESPACE: &str = "__qvm";

#[derive(Clone, Debug)]
pub struct TypedNameAndSQLExpr {
    pub name: String,
    pub type_: CRef<MType>,
    pub expr: CRef<SQLExpr<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct TypedSQLExpr {
    pub type_: CRef<MType>,
    pub expr: CRef<SQLExpr<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct TypedSQLQuery {
    pub type_: CRef<MType>,
    pub query: CRef<SQLQuery<CRef<MType>>>,
}

impl<Ty: Clone + fmt::Debug> Constrainable for SQLExpr<Ty> {}
impl<Ty: Clone + fmt::Debug> Constrainable for SQLQuery<Ty> {}
impl Constrainable for TypedSQLExpr {}
impl Constrainable for TypedSQLQuery {}

pub fn compile_reference(schema: Ref<Schema>, sqlpath: &Vec<sqlast::Ident>) -> Result<CTypedExpr> {
    let path: Vec<_> = sqlpath.iter().map(|e| e.value.clone()).collect();
    let (decl, remainder) = lookup_path(schema.clone(), &path, true /* import_global */)?;

    let entry = decl.value.clone();
    let remainder_cpy = remainder.clone();

    let type_ = match &entry {
        SchemaEntry::Expr(v) => v.clone(),
        _ => return Err(CompileError::wrong_kind(path.clone(), "value", &decl)),
    }
    .then(move |typed: Ref<STypedExpr>| -> Result<CRef<MType>> {
        let type_ = typed
            .borrow()
            .type_
            .then(|t: Ref<SType>| Ok(t.borrow().instantiate()?))?;
        typecheck_path(type_.clone(), remainder_cpy.as_slice())
    })?;

    let top_level_ref = CTypedExpr {
        type_: type_.clone(),
        expr: mkcref(Expr::SchemaEntry(SchemaEntryExpr {
            debug_name: decl.name.clone(),
            entry: entry.clone(),
        })),
    };

    let r = match remainder.len() {
        0 => top_level_ref,
        _ => {
            // Turn the top level reference into a SQL placeholder, and return
            // a path accessing it
            let placeholder = intern_placeholder(schema.clone(), "param", top_level_ref)?;
            let expr = placeholder.expr.then(|expr: Ref<SQLExpr<CRef<MType>>>| {
                let placeholder_name = match &expr.borrow().expr {
                    sqlast::Expr::Identifier(i) => i,
                    _ => panic!("placeholder expected to be an identifier"),
                };
                let mut full_name = vec![placeholder_name.clone()];
                full_name.extend(remainder.clone().into_iter().map(|n| sqlast::Ident {
                    value: n,
                    quote_style: None,
                }));

                let expr = mkcref(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: expr.borrow().params.clone(),
                    expr: sqlast::Expr::CompoundIdentifier(full_name),
                })));

                Ok(expr)
            })?;

            CTypedExpr { type_, expr }
        }
    };

    Ok(r)
}

pub fn new_placeholder_name(schema: Ref<Schema>, kind: &str) -> String {
    let placeholder = schema.borrow().next_placeholder;
    schema.borrow_mut().next_placeholder += 1;
    format!("{}{}", kind, placeholder)
}

pub fn intern_placeholder(
    schema: Ref<Schema>,
    kind: &str,
    expr: CTypedExpr,
) -> Result<TypedSQLExpr> {
    let type_ = expr.type_;
    let sqlexpr = expr
        .expr
        .then(|expr: Ref<Expr<CRef<MType>>>| match &*expr.borrow() {
            Expr::SQLExpr(sqlexpr) => Ok(mkcref(sqlexpr.as_ref().clone())),
            _ => {
                let placeholder_name =
                    "@".to_string() + new_placeholder_name(schema.clone(), kind).as_str();

                Ok(mkcref(SQLExpr {
                    params: Params::from([(
                        placeholder_name.clone(),
                        TypedExpr {
                            type_: type_.clone(),
                            expr: Rc::new(expr.borrow().clone()),
                        },
                    )]),
                    expr: sqlast::Expr::Identifier(sqlast::Ident {
                        value: placeholder_name.clone(),
                        quote_style: None,
                    }),
                }))
            }
        })?;

    Ok(TypedSQLExpr {
        type_: expr.type_.clone(),
        expr: sqlexpr,
    })
}

pub fn compile_sqlarg(
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    expr: &sqlast::Expr,
) -> Result<TypedSQLExpr> {
    Ok(intern_placeholder(
        schema.clone(),
        "param",
        compile_sqlexpr(schema.clone(), scope.clone(), expr)?,
    )?)
}

type CParams = Params<CRef<MType>>;

pub fn combine_sqlparams(all: Vec<&TypedSQLExpr>) -> Result<CRef<CParams>> {
    let mut params: CRef<CParams> = mkcref(BTreeMap::new());

    for expr in all {
        expr.expr.then(move |sqlexpr: Ref<SQLExpr<CRef<MType>>>| {
            let expr_params = sqlexpr.borrow().params.clone();

            let full_params = params.then(move |p: Ref<Params<CRef<MType>>>| {
                for (k, ep) in expr_params.clone() {
                    p.borrow_mut().insert(k, ep);
                }

                Ok(mkcref(p.borrow().clone()))
            })?;
            params = full_params.clone();

            Ok(full_params)
        })?;
    }

    panic!();
}

#[derive(Clone, Debug)]
pub struct SQLScope {
    pub parent: Option<Rc<SQLScope>>,
    pub relations: BTreeMap<Vec<String>, CTypedExpr>,
    pub multiple_rows: bool,
}

impl SQLScope {
    pub fn new(schema: Ref<Schema>, parent: Option<Rc<SQLScope>>) -> SQLScope {
        SQLScope {
            parent,
            relations: BTreeMap::new(),
            multiple_rows: true,
        }
    }
}

impl Constrainable for SQLScope {}

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

    let mut scope = mkref(SQLScope::new(schema.clone(), None));

    match select.from.len() {
        0 => {}
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

                    let path = name.0.iter().map(|n| n.value).collect::<Vec<_>>();
                    scope.borrow_mut().relations.insert(path, relation);
                }
                sqlast::TableFactor::Derived { .. } => {
                    return Err(CompileError::unimplemented("Subqueries"))
                }
                sqlast::TableFactor::TableFunction { .. } => {
                    return Err(CompileError::unimplemented("TABLE"))
                }
                sqlast::TableFactor::UNNEST { .. } => {
                    return Err(CompileError::unimplemented("UNNEST"))
                }
                sqlast::TableFactor::NestedJoin { .. } => {
                    return Err(CompileError::unimplemented("JOIN"))
                }
            }
        }
        _ => return Err(CompileError::unimplemented("JOIN")),
    };

    let relations = &scope.borrow().relations;

    if select.projection.len() == 1 && matches!(select.projection[0], sqlast::SelectItem::Wildcard)
    {
        if relations.len() > 1 {
            return Err(CompileError::unimplemented("Wildcard in SELECT with JOIN"));
        } else if relations.len() == 0 {
            return Err(CompileError::unimplemented(
                "Wildcard in SELECT without FROM",
            ));
        }
        let name = relations.keys().take(1).collect::<Vec<_>>()[0];
        let relation = relations
            .get(name)
            .ok_or_else(|| CompileError::internal("Could not get first relation"))?;

        let placeholder_name =
            QVM_NAMESPACE.to_string() + new_placeholder_name(schema.clone(), "rel").as_str();

        let mut ret = select.clone();
        ret.from = vec![sqlast::TableWithJoins {
            relation: sqlast::TableFactor::Table {
                name: sqlast::ObjectName(vec![sqlast::Ident {
                    value: placeholder_name.clone(),
                    quote_style: None,
                }]),
                alias: None,
                args: None,
                with_hints: Vec::new(),
            },
            joins: Vec::new(),
        }];

        return Ok(CTypedExpr {
            type_: relation.type_.clone(),
            expr: mkcref(Expr::SQLQuery(Rc::new(SQLQuery {
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
        });
    } else {
        let fields = Vec::new();
        let exprs = Vec::new();
        for proj in &select.projection {
            let (name, type_) = match proj {
                sqlast::SelectItem::UnnamedExpr(e) => {
                    let name = format!("{}", e);
                    let compiled = compile_sqlexpr(schema.clone(), scope.clone(), e)?;
                    (name, compiled.type_)
                }
                sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                    let name = alias.value.clone();
                    let compiled = compile_sqlexpr(schema.clone(), scope.clone(), expr)?;
                    (name, compiled.type_)
                }
                _ => {
                    return Err(CompileError::unimplemented("Table wildcard"));
                }
            };
            fields.push(MField {
                name,
                type_,
                nullable: true,
            });
        }
        return Err(CompileError::unimplemented("SELECT no FROM"));
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

pub fn compile_sqlexpr(
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    expr: &sqlast::Expr,
) -> Result<CTypedExpr> {
    let ret = match expr {
        sqlast::Expr::Value(v) => match v {
            sqlast::Value::Number(n, _) => CTypedExpr {
                type_: mkcref(MType::Atom(parse_numeric_type(n)?)),
                expr: mkcref(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::SingleQuotedString(_)
            | sqlast::Value::EscapedStringLiteral(_)
            | sqlast::Value::NationalStringLiteral(_)
            | sqlast::Value::HexStringLiteral(_)
            | sqlast::Value::DoubleQuotedString(_) => CTypedExpr {
                type_: resolve_global_atom("string")?,
                expr: mkcref(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::Boolean(_) => CTypedExpr {
                type_: resolve_global_atom("bool")?,
                expr: mkcref(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::Null => CTypedExpr {
                type_: resolve_global_atom("null")?,
                expr: mkcref(Expr::SQLExpr(Rc::new(SQLExpr {
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
            let cleft = compile_sqlarg(schema.clone(), scope.clone(), left)?;
            let cright = compile_sqlarg(schema.clone(), scope.clone(), right)?;
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
            CTypedExpr {
                type_,
                expr: params.then(|params: Ref<Params<CRef<MType>>>| {
                    Ok(mkcref(Expr::SQLExpr(Rc::new(SQLExpr {
                        params: params.borrow().clone(),
                        expr: sqlast::Expr::BinaryOp {
                            left: Box::new(cleft.expr.expr.clone()),
                            op: op.clone(),
                            right: Box::new(cright.expr.expr.clone()),
                        },
                    }))))
                })?,
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
            let mut compiled_args: BTreeMap<String, TypedNameAndSQLExpr> = BTreeMap::new();
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

                let compiled_arg = compile_sqlarg(schema.clone(), scope.clone(), &expr)?;
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
