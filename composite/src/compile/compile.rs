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

pub fn lookup_schema(schema: Rc<RefCell<Schema>>, path: &ast::Path) -> Result<Rc<RefCell<Schema>>> {
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

    schema.borrow_mut().imports.insert(k, v.clone());
    return Ok(v);
}

pub fn lookup_path(
    mut schema: Rc<RefCell<Schema>>,
    path: &Path,
) -> Result<(Rc<RefCell<Decl>>, ast::Path)> {
    let debug_path = debug_path(&path);
    if path.items.len() == 0 {
        return Err(CompileError::no_such_entry(debug_path));
    }

    for imported_schema in &path.schema {
        schema = lookup_schema(schema.clone(), &imported_schema)?;
    }

    for (i, ident) in path.items.iter().enumerate() {
        let new = match schema.borrow().decls.get(ident) {
            Some(decl) => {
                if i > 0 && !decl.borrow().public {
                    return Err(CompileError::wrong_kind(
                        debug_path,
                        "public",
                        &decl.borrow().clone(),
                    ));
                }

                let new = match &decl.borrow().value {
                    SchemaEntry::Schema(imported) => lookup_schema(schema.clone(), &imported)?,
                    _ => return Ok((decl.clone(), path.items[i + 1..].to_vec())),
                };

                if i == path.items.len() - 1 {
                    return Ok((decl.clone(), vec![]));
                }

                new
            }
            None => {
                return Err(CompileError::no_such_entry(debug_path));
            }
        };

        schema = new;
    }

    return Err(CompileError::no_such_entry(debug_path));
}

pub fn convert_path(items: &ast::Path) -> Path {
    Path {
        items: items.clone(),
        schema: vec![],
    }
}

pub fn debug_path(path: &Path) -> ast::Path {
    path.schema
        .iter()
        .flatten()
        .chain(&path.items)
        .map(|i| i.clone())
        .collect()
}

pub fn resolve_type(schema: Rc<RefCell<Schema>>, ast: &ast::Type) -> Result<Type> {
    match ast {
        ast::Type::Reference(path) => {
            let decl = {
                let mut current = schema.clone();
                let decl = loop {
                    current = match lookup_path(current.clone(), &convert_path(path)) {
                        Err(CompileError::NoSuchEntry { .. }) => {
                            if let Some(parent_scope) = current.borrow().parent_scope.clone() {
                                parent_scope.clone()
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

pub fn resolve_expr(schema: Rc<RefCell<Schema>>, expr: &ast::Expr) -> Result<Type> {
    match expr {
        ast::Expr::Unknown => Ok(Type::Unknown),
        ast::Expr::ImportedPath {
            schema_path,
            entry_path,
        } => {
            let full_path = Path {
                schema: vec![schema_path.clone()],
                items: entry_path.clone(),
            };
            let (decl, remainder) = lookup_path(schema, &full_path)?;
            if remainder.len() > 0 {
                return Err(CompileError::wrong_kind(
                    debug_path(&full_path),
                    "decl",
                    &decl.borrow().clone(),
                ));
            }

            let t = match &decl.borrow().value {
                SchemaEntry::Expr(e) => e.type_.clone(),
                _ => {
                    return Err(CompileError::wrong_kind(
                        debug_path(&full_path),
                        "value",
                        &decl.borrow().clone(),
                    ))
                }
            };

            Ok(t)
        }
        ast::Expr::SQLQuery(_) => Err(CompileError::unimplemented("SELECT")),
        ast::Expr::SQLExpr(e) => match e {
            sqlast::Expr::Value(v) => match v {
                sqlast::Value::Number(_, _) => resolve_global_atom("number"),
                sqlast::Value::SingleQuotedString(_)
                | sqlast::Value::EscapedStringLiteral(_)
                | sqlast::Value::NationalStringLiteral(_)
                | sqlast::Value::HexStringLiteral(_)
                | sqlast::Value::DoubleQuotedString(_) => resolve_global_atom("string"),
                sqlast::Value::Boolean(_) => resolve_global_atom("bool"),
                sqlast::Value::Null => resolve_global_atom("string"),
                sqlast::Value::Placeholder(_) => Ok(Type::Unknown),
                _ => {
                    return Err(CompileError::unimplemented(e.to_string().as_str()));
                }
            },
            sqlast::Expr::CompoundIdentifier(sqlpath) => {
                let path: Vec<_> = sqlpath.iter().map(|e| e.value.clone()).collect();
                let (decl, remainder) = lookup_path(schema, &convert_path(&path))?;
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

                Ok(current.clone())
            }
            _ => Err(CompileError::unimplemented(e.to_string().as_str())),
        },
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

pub fn typecheck(schema: Rc<RefCell<Schema>>, lhs_type: &Type, expr: &ast::Expr) -> Result<Type> {
    let rhs_type = resolve_expr(schema, expr)?;

    unify_types(lhs_type, &rhs_type)
}

pub fn rebind_type(schema_path: &ast::Path, type_: &Type) -> Result<Type> {
    match type_ {
        Type::Unknown => Ok(Type::Unknown),
        Type::Atom(a) => Ok(Type::Atom(a.clone())),
        Type::Struct(fields) => Ok(Type::Struct(
            fields
                .iter()
                .map(|(k, v)| Ok((k.clone(), rebind_type(schema_path, v)?)))
                .collect::<Result<BTreeMap<String, Type>>>()?,
        )),
        Type::List(inner) => Ok(Type::List(Box::new(rebind_type(
            schema_path,
            inner.as_ref(),
        )?))),
        Type::Exclude { inner, excluded } => Ok(Type::Exclude {
            inner: Box::new(rebind_type(schema_path, inner)?),
            excluded: excluded.clone(),
        }),
        Type::Ref(p) => Ok(Type::Ref(Path {
            schema: vec![vec![schema_path.clone()], p.schema.clone()].concat(),
            items: p.items.clone(),
        })),
    }
}

pub fn rebind_decl(schema_path: &ast::Path, decl: Rc<RefCell<Decl>>) -> Result<SchemaEntry> {
    match &decl.borrow().value {
        SchemaEntry::Schema(s) => Ok(SchemaEntry::Schema(s.clone())),
        SchemaEntry::Type(t) => Ok(SchemaEntry::Type(rebind_type(schema_path, &t)?)),
        SchemaEntry::Expr(e) => Ok(SchemaEntry::Expr(TypedExpr {
            type_: rebind_type(schema_path, &e.type_)?,
            expr: ast::Expr::ImportedPath {
                schema_path: schema_path.clone(),
                entry_path: vec![decl.borrow().name.clone()],
            },
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
        let entries: Vec<(String, SchemaEntry)> = match &stmt.body {
            ast::StmtBody::Noop => continue,
            ast::StmtBody::Import { path, list, args } => {
                let mut imports = Vec::new();

                if args.is_some() {
                    return Err(CompileError::unimplemented("import arguments"));
                }

                let imported = lookup_schema(schema.clone(), &path)?;
                match list {
                    ast::ImportList::None => {
                        imports.push((
                            path.last().unwrap().clone(),
                            SchemaEntry::Schema(path.clone()),
                        ));
                    }
                    ast::ImportList::Star => {
                        for (k, v) in imported
                            .borrow()
                            .decls
                            .iter()
                            .filter(|(_, v)| v.borrow().public)
                        {
                            imports.push((k.clone(), rebind_decl(path, v.clone())?));
                        }
                    }
                    ast::ImportList::Items(items) => {
                        for item in items {
                            if item.len() != 1 {
                                return Err(CompileError::unimplemented("path imports"));
                            }

                            let (decl, r) = lookup_path(imported.clone(), &convert_path(item))?;
                            if r.len() > 0 {
                                return Err(CompileError::no_such_entry(r.clone()));
                            }

                            imports.push((item[0].clone(), rebind_decl(path, decl.clone())?));
                        }
                    }
                }

                imports
            }
            ast::StmtBody::TypeDef(nt) => vec![(
                nt.name.clone(),
                SchemaEntry::Type(resolve_type(schema.clone(), &nt.def)?),
            )],
            ast::StmtBody::FnDef { .. } => return Err(CompileError::unimplemented("fn")),
            ast::StmtBody::Let { name, type_, body } => {
                let lhs_type = if let Some(t) = type_ {
                    resolve_type(schema.clone(), &t)?
                } else {
                    Type::Unknown
                };
                vec![(
                    name.clone(),
                    SchemaEntry::Expr(TypedExpr {
                        type_: typecheck(schema.clone(), &lhs_type, &body)?,
                        expr: body.clone(),
                    }),
                )]
            }
            ast::StmtBody::Extern { name, type_ } => vec![(
                name.clone(),
                SchemaEntry::Expr(TypedExpr {
                    type_: resolve_type(schema.clone(), type_)?,
                    expr: ast::Expr::Unknown,
                }),
            )],
        };

        for (name, value) in entries {
            if schema.borrow().decls.contains_key(&name) {
                return Err(CompileError::duplicate_entry(vec![name]));
            }

            schema.borrow_mut().decls.insert(
                name.clone(),
                Rc::new(RefCell::new(Decl {
                    public: stmt.export,
                    name,
                    value,
                })),
            );
        }
    }

    Ok(())
}
