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
) -> Result<(Rc<RefCell<Decl>>, SchemaRef, ast::Path)> {
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
                    return Ok((decl.clone(), schema.clone(), vec![]));
                }

                match &decl.borrow().value {
                    SchemaEntry::Schema(imported) => lookup_schema(schema.clone(), &imported)?
                        .borrow()
                        .schema
                        .clone(),
                    _ => return Ok((decl.clone(), schema.clone(), path[i + 1..].to_vec())),
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
                        Ok((d, _, r)) => {
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

pub fn intern_placeholder(schema: Rc<RefCell<Schema>>, expr: TypedExpr) -> Result<TypedSQLExpr> {
    match expr.expr {
        Expr::SQLExpr(sqlexpr) => Ok(TypedSQLExpr {
            type_: expr.type_,
            expr: sqlexpr,
        }),
        _ => {
            let placeholder = schema.borrow().next_placeholder;
            schema.borrow_mut().next_placeholder += 1;
            let placeholder_name = format!("<placeholder {}>", placeholder);

            Ok(TypedSQLExpr {
                type_: expr.type_,
                expr: SQLExpr {
                    params: BTreeMap::from([(placeholder_name.clone(), expr.expr)]),
                    expr: sqlast::Expr::Value(sqlast::Value::Placeholder(placeholder_name.clone())),
                },
            })
        }
    }
}

pub fn compile_sqlarg(schema: Rc<RefCell<Schema>>, expr: &sqlast::Expr) -> Result<TypedSQLExpr> {
    Ok(intern_placeholder(
        schema.clone(),
        compile_sqlexpr(schema.clone(), expr)?,
    )?)
}

pub fn combine_sqlparams(all: Vec<&TypedSQLExpr>) -> Result<BTreeMap<String, Expr>> {
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
        sqlast::Expr::CompoundIdentifier(sqlpath) => {
            Ok(compile_reference(schema.clone(), sqlpath)?)
        }
        sqlast::Expr::Identifier(ident) => {
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
    let (decl, s, remainder) = lookup_path(schema.clone(), &path)?;
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

    let decl_name = decl.borrow().name.clone();

    Ok(TypedExpr {
        type_: current.clone(),
        expr: Expr::Ref(PathRef {
            schema: SchemaInstance::global(s),
            items: vec![vec![decl_name], remainder].concat(),
        }),
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

pub fn rebind_type(type_: &Type) -> Result<Type> {
    match type_ {
        Type::Unknown => Ok(Type::Unknown),
        Type::Atom(a) => Ok(Type::Atom(a.clone())),
        Type::Struct(fields) => Ok(Type::Struct(
            fields
                .iter()
                .map(|(k, v)| Ok((k.clone(), rebind_type(v)?)))
                .collect::<Result<BTreeMap<String, Type>>>()?,
        )),
        Type::List(inner) => Ok(Type::List(Box::new(rebind_type(inner.as_ref())?))),
        Type::Exclude { inner, excluded } => Ok(Type::Exclude {
            inner: Box::new(rebind_type(inner)?),
            excluded: excluded.clone(),
        }),
        Type::Ref(p) => Ok(Type::Ref(PathRef {
            schema: p.schema.clone(),
            items: p.items.clone(),
        })),
    }
}

pub fn rebind_decl(schema: SchemaInstance, decl: Rc<RefCell<Decl>>) -> Result<SchemaEntry> {
    match &decl.borrow().value {
        SchemaEntry::Schema(s) => Ok(SchemaEntry::Schema(s.clone())),
        SchemaEntry::Type(t) => Ok(SchemaEntry::Type(rebind_type(&t)?)),
        SchemaEntry::Expr(e) => Ok(SchemaEntry::Expr(TypedExpr {
            type_: rebind_type(&e.type_)?,
            expr: Expr::Ref(PathRef {
                schema,
                items: vec![decl.borrow().name.clone()],
            }),
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

                            let (decl, s, r) =
                                lookup_path(imported.borrow().schema.clone(), &item)?;
                            if r.len() > 0 {
                                return Err(CompileError::no_such_entry(r.clone()));
                            }

                            let imported_schema = SchemaInstance { schema: s, id };
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
            ast::StmtBody::FnDef { .. } => return Err(CompileError::unimplemented("fn")),
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
