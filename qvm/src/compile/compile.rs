use sqlparser::ast as sqlast;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path as FilePath;
use std::rc::Rc;

use crate::ast;
use crate::compile::error::*;
use crate::compile::inference::*;
use crate::compile::schema::*;
use crate::parser::parse_schema;
use crate::types::number::parse_numeric_type;

const QVM_NAMESPACE: &str = "__qvm";

pub fn lookup_schema(schema: Ref<Schema>, path: &ast::Path) -> Result<Ref<ImportedSchema>> {
    if let Some(s) = schema.borrow().imports.get(path) {
        return Ok(s.clone());
    }

    let (k, v) = if let Some(root) = &schema.borrow().folder {
        let mut file_path_buf = FilePath::new(root).to_path_buf();
        for p in path {
            file_path_buf.push(FilePath::new(p));
        }
        file_path_buf.set_extension("co");
        let file_path = file_path_buf.as_path();

        match file_path.to_str() {
            Some(file_path_str) => match compile_schema_from_file(file_path_str) {
                Ok(s) => (path.clone(), s.clone()),
                Err(CompileError::FsError { .. }) => {
                    return Err(CompileError::no_such_entry(path.clone()))
                }
                Err(e) => return Err(e),
            },
            None => return Err(CompileError::no_such_entry(path.clone())),
        }
    } else {
        return Err(CompileError::no_such_entry(path.clone()));
    };

    let imported = mkref(ImportedSchema {
        args: if v.borrow().externs.len() == 0 {
            None
        } else {
            Some(Vec::new())
        },
        schema: v.clone(),
    });

    schema.borrow_mut().imports.insert(k, imported.clone());

    return Ok(imported);
}

pub fn lookup_path(
    mut schema: Ref<Schema>,
    path: &ast::Path,
    import_global: bool,
) -> Result<(Decl, ast::Path)> {
    if path.len() == 0 {
        return Err(CompileError::no_such_entry(path.clone()));
    }

    for (i, ident) in path.iter().enumerate() {
        let new = match schema.borrow().decls.get(ident) {
            Some(decl) => {
                if i > 0 && !decl.public {
                    return Err(CompileError::wrong_kind(path.clone(), "public", decl));
                }

                if i == path.len() - 1 {
                    return Ok((decl.clone(), vec![]));
                }

                match &decl.value {
                    SchemaEntry::Schema(imported) => lookup_schema(schema.clone(), &imported)?
                        .borrow()
                        .schema
                        .clone(),
                    _ => return Ok((decl.clone(), path[i + 1..].to_vec())),
                }
            }
            None => match &schema.borrow().parent_scope {
                Some(parent) => {
                    return lookup_path(parent.clone(), &path[i..].to_vec(), import_global)
                }
                None => {
                    if import_global {
                        return lookup_path(
                            Schema::new_global_schema(),
                            &path[i..].to_vec(),
                            false, /* import_global */
                        );
                    } else {
                        return Err(CompileError::no_such_entry(path.clone()));
                    }
                }
            },
        };

        schema = new;
    }

    return Err(CompileError::no_such_entry(path.clone()));
}

pub fn resolve_type(schema: Ref<Schema>, ast: &ast::Type) -> Result<CRef<MType>> {
    match ast {
        ast::Type::Reference(path) => {
            let (decl, r) = lookup_path(schema.clone(), &path, true /* import_global */)?;
            if r.len() > 0 {
                return Err(CompileError::no_such_entry(r));
            }

            let t = match decl.value {
                SchemaEntry::Type(t) => t,
                _ => return Err(CompileError::wrong_kind(path.clone(), "type", &decl)),
            };

            Ok(t)
        }
        ast::Type::Struct(entries) => {
            let mut fields = Vec::new();
            let mut seen = BTreeSet::new();
            for e in entries {
                match e {
                    ast::StructEntry::NameAndType(nt) => {
                        if seen.contains(&nt.name) {
                            return Err(CompileError::duplicate_entry(vec![nt.name.clone()]));
                        }
                        seen.insert(nt.name.clone());
                        fields.push(MField {
                            name: nt.name.clone(),
                            type_: resolve_type(schema.clone(), &nt.def)?,
                            nullable: true, /* TODO: implement non-null types */
                        });
                    }
                    ast::StructEntry::Include { .. } => {
                        return Err(CompileError::unimplemented("Struct inclusions"));
                    }
                }
            }

            Ok(mkcref(MType::Record(fields)))
        }
        ast::Type::List(inner) => Ok(mkcref(MType::List(resolve_type(schema, inner)?))),
        ast::Type::Exclude { .. } => {
            return Err(CompileError::unimplemented("Struct exclusions"));
        }
    }
}

pub fn resolve_global_atom(name: &str) -> Result<CRef<MType>> {
    let schema = Schema::new_global_schema();
    resolve_type(schema, &ast::Type::Reference(vec![name.to_string()]))
}

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

pub fn compile_select(
    schema: Ref<Schema>,
    select: &sqlast::Select,
) -> Result<TypedExpr<CRef<MType>>> {
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

                    // TODO: This should unify the type with [unknown] instead
                    //
                    if !matches!(&*relation.type_.must()?.borrow(), MType::List { .. }) {
                        return Err(CompileError::wrong_type(
                            &MType::List(MType::new_unknown()),
                            &*relation.type_.must()?.borrow(),
                        ));
                    }

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

pub fn compile_sqlquery(
    schema: Ref<Schema>,
    query: &sqlast::Query,
) -> Result<TypedExpr<CRef<MType>>> {
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

pub fn compile_sqlexpr(schema: Ref<Schema>, expr: &sqlast::Expr) -> Result<TypedExpr<CRef<MType>>> {
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
        sqlast::Expr::BinaryOp { left, op, right } => {
            let cleft = compile_sqlarg(schema.clone(), left)?;
            let cright = compile_sqlarg(schema.clone(), right)?;
            let params = combine_sqlparams(vec![&cleft, &cright])?;
            let type_ = match op {
                sqlast::BinaryOperator::Plus => {
                    unify_types(&cleft.type_, &cright.type_)?;
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
                            ret: MType::new_unknown(),
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
                    unify_types(&arg.type_, &compiled_arg.type_)?;
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
                format!("Expression: {}", expr).as_str(),
            ))
        }
    };

    Ok(ret)
}

fn find_field<'a>(fields: &'a Vec<MField>, name: &str) -> Option<&'a MField> {
    for f in fields.iter() {
        if f.name == name {
            return Some(f);
        }
    }
    None
}

impl SType {
    pub fn instantiate(&self) -> Result<CRef<MType>> {
        let variables: BTreeMap<_, _> = self
            .variables
            .iter()
            .map(|n| (n.clone(), MType::new_unknown()))
            .collect();

        return Ok(self.body.substitute(&variables)?);
    }
}

pub fn typecheck_path(type_: CRef<MType>, path: &[String]) -> Result<CRef<MType>> {
    if path.len() == 0 {
        return Ok(type_);
    }

    let name = path[0].clone();
    let remainder = path[1..].to_vec();

    type_.then(move |type_: Ref<MType>| match &*type_.borrow() {
        MType::Record(fields) => {
            if let Some(field) = find_field(&fields, name.as_str()) {
                typecheck_path(field.type_.clone(), remainder.as_slice())
            } else {
                return Err(CompileError::wrong_type(
                    &MType::Record(vec![MField::new_nullable(
                        name.clone(),
                        MType::new_unknown(),
                    )]),
                    &*type_.borrow(),
                ));
            }
        }
        _ => {
            return Err(CompileError::wrong_type(
                &MType::Record(vec![MField::new_nullable(
                    name.clone(),
                    MType::new_unknown(),
                )]),
                &*type_.borrow(),
            ))
        }
    })
}

pub fn compile_reference(
    schema: Ref<Schema>,
    sqlpath: &Vec<sqlast::Ident>,
) -> Result<TypedExpr<CRef<MType>>> {
    let path: Vec<_> = sqlpath.iter().map(|e| e.value.clone()).collect();
    let (decl, remainder) = lookup_path(schema.clone(), &path, true /* import_global */)?;
    let type_ = match &decl.value {
        SchemaEntry::Expr(v) => v.as_ref(),
        _ => return Err(CompileError::wrong_kind(path.clone(), "value", &decl)),
    }
    .borrow()
    .type_
    .borrow()
    .instantiate()?;

    let top_level_ref = TypedExpr {
        type_: type_.clone(),
        expr: Rc::new(Expr::SchemaEntry(SchemaEntryExpr {
            debug_name: decl.name.clone(),
            entry: decl.value,
        })),
    };

    let r = match remainder.len() {
        0 => top_level_ref,
        _ => {
            let path_type = typecheck_path(type_.clone(), remainder.as_slice())?;

            // Turn the top level reference into a SQL placeholder, and return
            // a path accessing it
            let placeholder = intern_placeholder(schema, "param", top_level_ref)?;
            let placeholder_name = match &placeholder.expr.expr {
                sqlast::Expr::Identifier(i) => i,
                _ => panic!("placeholder expected to be an identifier"),
            };
            let mut full_name = vec![placeholder_name.clone()];
            full_name.extend(remainder.into_iter().map(|n| sqlast::Ident {
                value: n,
                quote_style: None,
            }));

            TypedExpr {
                type_: path_type,
                expr: Rc::new(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: placeholder.expr.params.clone(),
                    expr: sqlast::Expr::CompoundIdentifier(full_name),
                }))),
            }
        }
    };

    Ok(r)
}

pub fn compile_expr(schema: Ref<Schema>, expr: &ast::Expr) -> Result<TypedExpr<CRef<MType>>> {
    match expr {
        ast::Expr::SQLQuery(q) => Ok(compile_sqlquery(schema.clone(), q)?),
        ast::Expr::SQLExpr(e) => Ok(compile_sqlexpr(schema.clone(), e)?),
    }
}

pub fn unify_types(lhs: &CRef<MType>, rhs: &CRef<MType>) -> Result<()> {
    lhs.unify(rhs)
}

pub fn rebind_decl(_schema: SchemaInstance, decl: &Decl) -> Result<SchemaEntry> {
    match &decl.value {
        SchemaEntry::Schema(s) => Ok(SchemaEntry::Schema(s.clone())),
        SchemaEntry::Type(t) => Ok(SchemaEntry::Type(t.clone())),
        SchemaEntry::Expr(e) => Ok(SchemaEntry::Expr(mkref(STypedExpr {
            type_: e.borrow().type_.clone(),
            expr: Rc::new(Expr::SchemaEntry(SchemaEntryExpr {
                debug_name: decl.name.clone(),
                entry: decl.value.clone(),
            })),
        }))),
    }
}

pub fn compile_schema_from_string(contents: &str) -> Result<Ref<Schema>> {
    let ast = parse_schema(contents)?;

    compile_schema(None, &ast)
}

pub fn compile_schema_from_file(file_path: &str) -> Result<Ref<Schema>> {
    let parsed_path = FilePath::new(file_path).canonicalize()?;
    if !parsed_path.exists() {
        return Err(CompileError::no_such_entry(
            parsed_path
                .components()
                .map(|x| format!("{:?}", x))
                .collect(),
        ));
    }
    let parent_path = parsed_path.parent();
    let folder = match parent_path {
        Some(p) => p.to_str().map(|f| f.to_string()),
        None => None,
    };
    let contents = fs::read_to_string(parsed_path).expect("Unable to read file");

    let ast = parse_schema(contents.as_str())?;

    compile_schema(folder, &ast)
}

pub fn compile_schema(folder: Option<String>, ast: &ast::Schema) -> Result<Ref<Schema>> {
    let schema = Schema::new(folder);
    compile_schema_entries(schema.clone(), ast)?;
    Ok(schema)
}

pub fn compile_schema_entries(schema: Ref<Schema>, ast: &ast::Schema) -> Result<()> {
    for stmt in &ast.stmts {
        let entries: Vec<(String, bool, SchemaEntry)> = match &stmt.body {
            ast::StmtBody::Noop => continue,
            ast::StmtBody::Import { path, list, args } => {
                let imported = lookup_schema(schema.clone(), &path)?;

                let mut imports = Vec::new();
                let checked = match args {
                    None => None,
                    Some(args) => {
                        let mut externs = imported.borrow().schema.borrow().externs.clone();
                        let mut checked = BTreeMap::new();
                        for arg in args {
                            let expr = match &arg.expr {
                                None => ast::Expr::SQLExpr(sqlast::Expr::CompoundIdentifier(vec![
                                    sqlast::Ident {
                                        value: arg.name.clone(),
                                        quote_style: None,
                                    },
                                ])),
                                Some(expr) => expr.clone(),
                            };

                            if checked.get(&arg.name).is_some() {
                                return Err(CompileError::duplicate_entry(vec![arg.name.clone()]));
                            }

                            if let Some(extern_) = externs.get_mut(&arg.name) {
                                let compiled = compile_expr(schema.clone(), &expr)?;

                                unify_types(&extern_, &compiled.type_)?;
                                checked.insert(
                                    arg.name.clone(),
                                    TypedNameAndExpr {
                                        name: arg.name.clone(),
                                        type_: extern_.clone(),
                                        expr: compiled.expr,
                                    },
                                );
                            } else {
                                return Err(CompileError::no_such_entry(vec![arg.name.clone()]));
                            }
                        }

                        Some(checked)
                    }
                };

                let id = {
                    let imported_args = &mut imported.borrow_mut().args;
                    if let Some(imported_args) = imported_args {
                        if let Some(checked) = checked {
                            let id = imported_args.len();
                            imported_args.push(checked);
                            Some(id)
                        } else {
                            return Err(CompileError::import_error(
                                path.clone(),
                                "Arguments are not provided to module with extern declarations",
                            ));
                        }
                    } else if args.is_some() {
                        return Err(CompileError::import_error(
                              path.clone(),
                            "Arguments should not be provided to module without extern declarations",
                        ));
                    } else {
                        None
                    }
                };

                match list {
                    ast::ImportList::None => {
                        imports.push((
                            path.last().unwrap().clone(),
                            false, /* extern_ */
                            SchemaEntry::Schema(path.clone()),
                        ));
                    }
                    ast::ImportList::Star => {
                        for (k, v) in imported
                            .borrow()
                            .schema
                            .borrow()
                            .decls
                            .iter()
                            .filter(|(_, v)| v.public)
                        {
                            let imported_schema = SchemaInstance {
                                schema: imported.borrow().schema.clone(),
                                id,
                            };
                            imports.push((
                                k.clone(),
                                false, /* extern_ */
                                rebind_decl(imported_schema, &v)?,
                            ));
                        }
                    }
                    ast::ImportList::Items(items) => {
                        for item in items {
                            if item.len() != 1 {
                                return Err(CompileError::unimplemented("path imports"));
                            }

                            let (decl, r) = lookup_path(
                                imported.borrow().schema.clone(),
                                &item,
                                false, /* import_global */
                            )?;
                            if r.len() > 0 {
                                return Err(CompileError::no_such_entry(r.clone()));
                            }

                            let imported_schema = SchemaInstance {
                                schema: schema.clone(),
                                id,
                            };
                            // XXX This is currently broken, because we don't actually "inject"
                            // any meaningful reference to imported_schema's id into the decl.
                            // We should figure out how to generate a new set of decls for the
                            // imported schema (w/ the imported args)
                            imports.push((
                                item[0].clone(),
                                false, /* extern_ */
                                rebind_decl(imported_schema, &decl)?,
                            ));
                        }
                    }
                }

                imports
            }
            ast::StmtBody::TypeDef(nt) => vec![(
                nt.name.clone(),
                false, /* extern_ */
                SchemaEntry::Type(resolve_type(schema.clone(), &nt.def)?),
            )],
            ast::StmtBody::FnDef {
                name,
                generics,
                args,
                ret,
                body,
            } => {
                if generics.len() > 0 {
                    return Err(CompileError::unimplemented("function generics"));
                }

                let inner_schema = Schema::new(schema.borrow().folder.clone());
                inner_schema.borrow_mut().parent_scope = Some(schema.clone());

                let mut compiled_args = Vec::new();
                for arg in args {
                    if inner_schema.borrow().decls.get(&arg.name).is_some() {
                        return Err(CompileError::duplicate_entry(vec![name.clone()]));
                    }
                    let type_ = resolve_type(inner_schema.clone(), &arg.type_)?;
                    inner_schema.borrow_mut().decls.insert(
                        arg.name.clone(),
                        Decl {
                            public: true,
                            extern_: true,
                            name: arg.name.clone(),
                            value: SchemaEntry::Expr(mkref(STypedExpr {
                                type_: SType::new_mono(type_.clone()),
                                expr: Rc::new(Expr::Unknown),
                            })),
                        },
                    );
                    inner_schema
                        .borrow_mut()
                        .externs
                        .insert(arg.name.clone(), type_.clone());
                    compiled_args.push(MField::new_nullable(arg.name.clone(), type_.clone()));
                }

                let compiled = compile_expr(inner_schema.clone(), body)?;
                if let Some(ret) = ret {
                    unify_types(&resolve_type(inner_schema.clone(), ret)?, &compiled.type_)?
                }

                vec![(
                    name.clone(),
                    false, /* extern_ */
                    SchemaEntry::Expr(mkref(STypedExpr {
                        type_: SType::new_mono(mkcref(MType::Fn(MFnType {
                            args: compiled_args,
                            ret: compiled.type_.clone(),
                        }))),
                        expr: Rc::new(Expr::Fn(FnExpr {
                            inner_schema: inner_schema.clone(),
                            body: compiled.expr.clone(),
                        })),
                    })),
                )]
            }
            ast::StmtBody::Let { name, type_, body } => {
                let lhs_type = if let Some(t) = type_ {
                    resolve_type(schema.clone(), &t)?
                } else {
                    MType::new_unknown()
                };
                let compiled = compile_expr(schema.clone(), &body)?;
                unify_types(&lhs_type, &compiled.type_)?;
                vec![(
                    name.clone(),
                    false, /* extern_ */
                    SchemaEntry::Expr(mkref(STypedExpr {
                        type_: SType::new_mono(lhs_type),
                        expr: compiled.expr,
                    })),
                )]
            }
            ast::StmtBody::Extern { name, type_ } => vec![(
                name.clone(),
                true, /* extern_ */
                SchemaEntry::Expr(mkref(STypedExpr {
                    type_: SType::new_mono(resolve_type(schema.clone(), type_)?),
                    expr: Rc::new(Expr::Unknown),
                })),
            )],
        };

        for (name, extern_, value) in &entries {
            if schema.borrow().decls.contains_key(name) {
                return Err(CompileError::duplicate_entry(vec![name.clone()]));
            }

            schema.borrow_mut().decls.insert(
                name.clone(),
                Decl {
                    public: stmt.export,
                    extern_: *extern_,
                    name: name.clone(),
                    value: value.clone(),
                },
            );

            if *extern_ {
                match value {
                    SchemaEntry::Expr(e) => {
                        schema
                            .borrow_mut()
                            .externs
                            .insert(name.clone(), e.borrow().type_.borrow().instantiate()?);
                    }
                    _ => return Err(CompileError::unimplemented("type externs")),
                }
            }
        }
    }

    Ok(())
}
