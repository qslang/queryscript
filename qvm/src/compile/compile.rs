use crate::ast;
use crate::compile::error::*;
use crate::parser::parse_schema;
use crate::schema::*;
use sqlparser::ast as sqlast;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path as FilePath;
use std::rc::Rc;

const QVM_NAMESPACE: &str = "__qvm";

pub fn lookup_schema(
    schema: Rc<RefCell<Schema>>,
    path: &ast::Path,
) -> Result<Rc<RefCell<ImportedSchema>>> {
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

    let imported = Rc::new(RefCell::new(ImportedSchema {
        args: if v.borrow().externs.len() == 0 {
            None
        } else {
            Some(Vec::new())
        },
        schema: v.clone(),
    }));

    schema.borrow_mut().imports.insert(k, imported.clone());

    return Ok(imported);
}

pub fn lookup_path(
    mut schema: Rc<RefCell<Schema>>,
    path: &ast::Path,
) -> Result<(Rc<RefCell<Decl>>, ast::Path)> {
    if path.len() == 0 {
        return Err(CompileError::no_such_entry(path.clone()));
    }

    for (i, ident) in path.iter().enumerate() {
        let new = match schema.borrow().decls.get(ident) {
            Some(decl) => {
                if i > 0 && !decl.borrow().public {
                    return Err(CompileError::wrong_kind(
                        path.clone(),
                        "public",
                        &decl.borrow().clone(),
                    ));
                }

                if i == path.len() - 1 {
                    return Ok((decl.clone(), vec![]));
                }

                match &decl.borrow().value {
                    SchemaEntry::Schema(imported) => lookup_schema(schema.clone(), &imported)?
                        .borrow()
                        .schema
                        .clone(),
                    _ => return Ok((decl.clone(), path[i + 1..].to_vec())),
                }
            }
            None => {
                return Err(CompileError::no_such_entry(path.clone()));
            }
        };

        schema = new;
    }

    return Err(CompileError::no_such_entry(path.clone()));
}

pub fn resolve_type(schema: Rc<RefCell<Schema>>, ast: &ast::Type) -> Result<Type> {
    match ast {
        ast::Type::Reference(path) => {
            let decl = {
                let mut current = schema.clone();
                let mut tried_global = false;
                let decl = loop {
                    current = match lookup_path(current.clone(), &path) {
                        Err(CompileError::NoSuchEntry { .. }) => {
                            if let Some(parent_scope) = current.borrow().parent_scope.clone() {
                                parent_scope.clone()
                            } else if !tried_global {
                                tried_global = true;
                                Schema::new_global_schema()
                            } else {
                                return Err(CompileError::no_such_entry(path.clone()));
                            }
                        }
                        Err(e) => return Err(e),
                        Ok((d, r)) => {
                            if r.len() > 0 {
                                return Err(CompileError::no_such_entry(r));
                            }
                            break d;
                        }
                    }
                };

                decl.clone()
            };

            let t = match decl.borrow().value.clone() {
                SchemaEntry::Type(t) => t.clone(),
                _ => {
                    return Err(CompileError::wrong_kind(
                        path.clone(),
                        "type",
                        &decl.borrow().clone(),
                    ))
                }
            };

            Ok(t)
        }
        ast::Type::Struct(entries) => {
            let mut fields = BTreeMap::new();
            for e in entries {
                match e {
                    ast::StructEntry::NameAndType(nt) => {
                        if fields.contains_key(&nt.name) {
                            return Err(CompileError::duplicate_entry(vec![nt.name.clone()]));
                        }
                        fields.insert(nt.name.clone(), resolve_type(schema.clone(), &nt.def)?);
                    }
                    ast::StructEntry::Include { .. } => {
                        return Err(CompileError::unimplemented("Struct inclusions"));
                    }
                }
            }

            Ok(Type::Struct(fields))
        }
        ast::Type::List(inner) => Ok(Type::List(Box::new(resolve_type(schema, inner)?))),
        ast::Type::Exclude { .. } => {
            return Err(CompileError::unimplemented("Struct exclusions"));
        }
    }
}

pub fn resolve_global_atom(name: &str) -> Result<Type> {
    let schema = Schema::new_global_schema();
    resolve_type(schema, &ast::Type::Reference(vec![name.to_string()]))
}

pub fn new_placeholder_name(schema: Rc<RefCell<Schema>>, kind: &str) -> String {
    let placeholder = schema.borrow().next_placeholder;
    schema.borrow_mut().next_placeholder += 1;
    format!("{}{}", kind, placeholder)
}

pub fn intern_placeholder(
    schema: Rc<RefCell<Schema>>,
    kind: &str,
    expr: TypedExpr,
) -> Result<TypedSQLExpr> {
    match expr.expr {
        Expr::SQLExpr(sqlexpr) => Ok(TypedSQLExpr {
            type_: expr.type_,
            expr: sqlexpr,
        }),
        _ => {
            let placeholder_name = "@".to_string() + new_placeholder_name(schema, kind).as_str();

            Ok(TypedSQLExpr {
                type_: expr.type_.clone(),
                expr: SQLExpr {
                    params: BTreeMap::from([(vec![placeholder_name.clone()], expr)]),
                    expr: sqlast::Expr::Identifier(sqlast::Ident {
                        value: placeholder_name.clone(),
                        quote_style: None,
                    }),
                },
            })
        }
    }
}

pub fn compile_sqlarg(schema: Rc<RefCell<Schema>>, expr: &sqlast::Expr) -> Result<TypedSQLExpr> {
    Ok(intern_placeholder(
        schema.clone(),
        "param",
        compile_sqlexpr(schema.clone(), expr)?,
    )?)
}

pub fn combine_sqlparams(all: Vec<&TypedSQLExpr>) -> Result<BTreeMap<ast::Path, TypedExpr>> {
    Ok(all
        .iter()
        .map(|t| t.expr.params.clone())
        .flatten()
        .collect())
}

pub fn compile_sqlexpr(schema: Rc<RefCell<Schema>>, expr: &sqlast::Expr) -> Result<TypedExpr> {
    match expr {
        sqlast::Expr::Value(v) => match v {
            sqlast::Value::Number(_, _) => Ok(TypedExpr {
                type_: resolve_global_atom("number")?,
                expr: Expr::SQLExpr(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }),
            }),
            sqlast::Value::SingleQuotedString(_)
            | sqlast::Value::EscapedStringLiteral(_)
            | sqlast::Value::NationalStringLiteral(_)
            | sqlast::Value::HexStringLiteral(_)
            | sqlast::Value::DoubleQuotedString(_) => Ok(TypedExpr {
                type_: resolve_global_atom("string")?,
                expr: Expr::SQLExpr(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }),
            }),
            sqlast::Value::Boolean(_) => Ok(TypedExpr {
                type_: resolve_global_atom("bool")?,
                expr: Expr::SQLExpr(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }),
            }),
            sqlast::Value::Null => Ok(TypedExpr {
                type_: resolve_global_atom("null")?,
                expr: Expr::SQLExpr(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }),
            }),
            sqlast::Value::Placeholder(_) => Err(CompileError::unimplemented(
                format!("SQL Parameter syntax: {}", expr).as_str(),
            )),
        },
        sqlast::Expr::BinaryOp { left, op, right } => {
            let cleft = compile_sqlarg(schema.clone(), left)?;
            let cright = compile_sqlarg(schema.clone(), right)?;
            let params = combine_sqlparams(vec![&cleft, &cright])?;
            let type_ = match op {
                sqlast::BinaryOperator::Plus => {
                    unify_types(&resolve_global_atom("number")?, &cleft.type_)?;
                    unify_types(&resolve_global_atom("number")?, &cright.type_)?;
                    resolve_global_atom("number")?
                }
                _ => {
                    return Err(CompileError::unimplemented(
                        format!("Binary operator: {}", op).as_str(),
                    ));
                }
            };
            Ok(TypedExpr {
                type_,
                expr: Expr::SQLExpr(SQLExpr {
                    params,
                    expr: sqlast::Expr::BinaryOp {
                        left: Box::new(cleft.expr.expr),
                        op: op.clone(),
                        right: Box::new(cright.expr.expr),
                    },
                }),
            })
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

            let function = compile_reference(schema.clone(), &name.0)?;
            let fn_type = match function.type_ {
                Type::Fn(f) => f,
                _ => {
                    return Err(CompileError::wrong_type(
                        &Type::Fn(FnType {
                            args: Vec::new(),
                            ret: Box::new(Type::Unknown),
                        }),
                        &function.type_,
                    ))
                }
            };
            let mut compiled_args = Vec::new();
            let mut compiled_args_by_name: BTreeMap<String, usize> = BTreeMap::new();
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

                let compiled_arg = compile_sqlarg(schema.clone(), &expr)?;
                compiled_args_by_name.insert(name.clone(), compiled_args.len());
                compiled_args.push(TypedNameAndSQLExpr {
                    name: name.clone(),
                    type_: compiled_arg.type_.clone(),
                    expr: compiled_arg.expr.clone(),
                });
            }

            for arg in fn_type.args {
                if let Some(compiled_arg_idx) = compiled_args_by_name.get(&arg.name) {
                    let compiled_arg = &compiled_args[*compiled_arg_idx];
                    unify_types(&arg.type_, &compiled_arg.type_)?;
                    compiled_args_by_name.remove(&arg.name);
                } else {
                    return Err(CompileError::no_such_entry(vec![arg.name]));
                }
            }

            for (name, _) in compiled_args_by_name {
                return Err(CompileError::no_such_entry(vec![name.clone()]));
            }

            let compiled_args_no_name = compiled_args
                .iter()
                .map(|e| TypedSQLExpr {
                    type_: e.type_.clone(),
                    expr: e.expr.clone(),
                })
                .collect::<Vec<_>>();
            let mut params = combine_sqlparams(compiled_args_no_name.iter().collect())?;
            let placeholder_name = vec![
                QVM_NAMESPACE.to_string(),
                new_placeholder_name(schema.clone(), "func"),
            ];
            params.insert(
                placeholder_name.clone(),
                TypedExpr {
                    expr: function.expr,
                    type_: *fn_type.ret.clone(),
                },
            );

            // XXX There are cases here where we could inline eagerly
            //
            Ok(TypedExpr {
                type_: fn_type.ret.as_ref().clone(),
                expr: Expr::SQLExpr(SQLExpr {
                    params,
                    expr: sqlast::Expr::Function(sqlast::Function {
                        name: sqlast::ObjectName(
                            placeholder_name
                                .iter()
                                .map(|n| sqlast::Ident {
                                    value: n.clone(),
                                    quote_style: None,
                                })
                                .collect(),
                        ),
                        args: compiled_args
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
                }),
            })
        }
        sqlast::Expr::CompoundIdentifier(sqlpath) => {
            // XXX There are cases here where we could inline eagerly
            //
            Ok(compile_reference(schema.clone(), sqlpath)?)
        }
        sqlast::Expr::Identifier(ident) => {
            // XXX There are cases here where we could inline eagerly
            //
            Ok(compile_reference(schema.clone(), &vec![ident.clone()])?)
        }
        _ => Err(CompileError::unimplemented(
            format!("Expression: {}", expr).as_str(),
        )),
    }
}

pub fn compile_reference(
    schema: Rc<RefCell<Schema>>,
    sqlpath: &Vec<sqlast::Ident>,
) -> Result<TypedExpr> {
    let path: Vec<_> = sqlpath.iter().map(|e| e.value.clone()).collect();
    let (decl, remainder) = lookup_path(schema.clone(), &path)?;
    let type_ = match &decl.borrow().value {
        SchemaEntry::Expr(v) => v.clone(),
        _ => {
            return Err(CompileError::wrong_kind(
                path.clone(),
                "value",
                &decl.borrow().clone(),
            ))
        }
    }
    .type_;

    let top_level_ref = TypedExpr {
        type_: type_.clone(),
        expr: Expr::Decl(decl),
    };

    let mut current = &type_;
    for i in 0..remainder.len() {
        let name = &remainder[i];
        match current {
            Type::Struct(fields) => {
                if let Some(field) = fields.get(name) {
                    current = field;
                } else {
                    return Err(CompileError::wrong_type(
                        &Type::Struct(BTreeMap::from([(name.clone(), Type::Unknown)])),
                        current,
                    ));
                }
            }
            _ => {
                return Err(CompileError::wrong_type(
                    &Type::Struct(BTreeMap::from([(name.clone(), Type::Unknown)])),
                    current,
                ))
            }
        }
    }

    Ok(match remainder.len() {
        0 => top_level_ref,
        _ => {
            // Turn the top level reference into a SQL placeholder, and return
            // a path accessing it
            let placeholder = intern_placeholder(schema, "param", top_level_ref)?;
            let placeholder_name = match placeholder.expr.expr {
                sqlast::Expr::Identifier(i) => i,
                _ => panic!("placeholder expected to be an identifier"),
            };
            let mut full_name = vec![placeholder_name];
            full_name.extend(remainder.into_iter().map(|n| sqlast::Ident {
                value: n,
                quote_style: None,
            }));

            TypedExpr {
                type_: current.clone(),
                expr: Expr::SQLExpr(SQLExpr {
                    params: placeholder.expr.params,
                    expr: sqlast::Expr::CompoundIdentifier(full_name),
                }),
            }
        }
    })
}

pub fn compile_expr(schema: Rc<RefCell<Schema>>, expr: &ast::Expr) -> Result<TypedExpr> {
    match expr {
        ast::Expr::SQLQuery(_) => Err(CompileError::unimplemented("SELECT")),
        ast::Expr::SQLExpr(e) => Ok(compile_sqlexpr(schema.clone(), e)?),
    }
}

pub fn unify_types(lhs: &Type, rhs: &Type) -> Result<Type> {
    if matches!(rhs, Type::Unknown) {
        return Ok(lhs.clone());
    }

    if matches!(lhs, Type::Unknown) {
        return Ok(rhs.clone());
    }

    if *lhs != *rhs {
        return Err(CompileError::wrong_type(lhs, rhs));
    }

    return Ok(lhs.clone());
}

pub fn rebind_decl(_schema: SchemaInstance, decl: Rc<RefCell<Decl>>) -> Result<SchemaEntry> {
    match &decl.borrow().value {
        SchemaEntry::Schema(s) => Ok(SchemaEntry::Schema(s.clone())),
        SchemaEntry::Type(t) => Ok(SchemaEntry::Type(t.clone())),
        SchemaEntry::Expr(e) => Ok(SchemaEntry::Expr(TypedExpr {
            type_: e.type_.clone(),
            expr: Expr::Decl(decl.clone()),
        })),
    }
}

pub fn compile_schema_from_string(contents: &str) -> Result<Rc<RefCell<Schema>>> {
    let ast = parse_schema(contents)?;

    compile_schema(None, &ast)
}

pub fn compile_schema_from_file(file_path: &str) -> Result<Rc<RefCell<Schema>>> {
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

pub fn compile_schema(folder: Option<String>, ast: &ast::Schema) -> Result<Rc<RefCell<Schema>>> {
    let schema = Schema::new(folder);
    compile_schema_entries(schema.clone(), ast)?;
    Ok(schema)
}

pub fn compile_schema_entries(schema: Rc<RefCell<Schema>>, ast: &ast::Schema) -> Result<()> {
    for stmt in &ast.stmts {
        let entries: Vec<(String, bool, SchemaEntry)> = match &stmt.body {
            ast::StmtBody::Noop => continue,
            ast::StmtBody::Import { path, list, args } => {
                let imported = lookup_schema(schema.clone(), &path)?;

                let mut imports = Vec::new();
                let checked = match args {
                    None => None,
                    Some(args) => {
                        let externs = imported.borrow().schema.borrow().externs.clone();
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

                            if let Some(extern_) = externs.get(&arg.name) {
                                let compiled = compile_expr(schema.clone(), &expr)?;

                                let type_ = unify_types(extern_, &compiled.type_)?;
                                checked.insert(
                                    arg.name.clone(),
                                    TypedNameAndExpr {
                                        name: arg.name.clone(),
                                        type_,
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
                            .filter(|(_, v)| v.borrow().public)
                        {
                            let imported_schema = SchemaInstance {
                                schema: imported.borrow().schema.clone(),
                                id,
                            };
                            imports.push((
                                k.clone(),
                                false, /* extern_ */
                                rebind_decl(imported_schema, v.clone())?,
                            ));
                        }
                    }
                    ast::ImportList::Items(items) => {
                        for item in items {
                            if item.len() != 1 {
                                return Err(CompileError::unimplemented("path imports"));
                            }

                            let (decl, r) = lookup_path(imported.borrow().schema.clone(), &item)?;
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
                                rebind_decl(imported_schema, decl.clone())?,
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
                        Rc::new(RefCell::new(Decl {
                            public: true,
                            extern_: true,
                            name: arg.name.clone(),
                            value: SchemaEntry::Expr(TypedExpr {
                                type_: type_.clone(),
                                expr: Expr::Unknown,
                            }),
                        })),
                    );
                    inner_schema
                        .borrow_mut()
                        .externs
                        .insert(arg.name.clone(), type_.clone());
                    compiled_args.push(TypedName {
                        name: arg.name.clone(),
                        type_: type_.clone(),
                    });
                }

                let compiled = compile_expr(inner_schema.clone(), body)?;
                let type_ = if let Some(ret) = ret {
                    unify_types(&resolve_type(inner_schema.clone(), ret)?, &compiled.type_)?
                } else {
                    compiled.type_
                };

                vec![(
                    name.clone(),
                    false, /* extern_ */
                    SchemaEntry::Expr(TypedExpr {
                        type_: Type::Fn(FnType {
                            args: compiled_args,
                            ret: Box::new(type_.clone()),
                        }),
                        expr: Expr::Fn(FnExpr {
                            inner_schema: inner_schema.clone(),
                            body: Box::new(compiled.expr),
                        }),
                    }),
                )]
            }
            ast::StmtBody::Let { name, type_, body } => {
                let lhs_type = if let Some(t) = type_ {
                    resolve_type(schema.clone(), &t)?
                } else {
                    Type::Unknown
                };
                let compiled = compile_expr(schema.clone(), &body)?;
                let type_ = unify_types(&lhs_type, &compiled.type_)?;
                vec![(
                    name.clone(),
                    false, /* extern_ */
                    SchemaEntry::Expr(TypedExpr {
                        type_,
                        expr: compiled.expr,
                    }),
                )]
            }
            ast::StmtBody::Extern { name, type_ } => vec![(
                name.clone(),
                true, /* extern_ */
                SchemaEntry::Expr(TypedExpr {
                    type_: resolve_type(schema.clone(), type_)?,
                    expr: Expr::Unknown,
                }),
            )],
        };

        for (name, extern_, value) in &entries {
            if schema.borrow().decls.contains_key(name) {
                return Err(CompileError::duplicate_entry(vec![name.clone()]));
            }

            schema.borrow_mut().decls.insert(
                name.clone(),
                Rc::new(RefCell::new(Decl {
                    public: stmt.export,
                    extern_: *extern_,
                    name: name.clone(),
                    value: value.clone(),
                })),
            );

            if *extern_ {
                match value {
                    SchemaEntry::Expr(e) => {
                        schema
                            .borrow_mut()
                            .externs
                            .insert(name.clone(), e.type_.clone());
                    }
                    _ => return Err(CompileError::unimplemented("type externs")),
                }
            }
        }
    }

    Ok(())
}
