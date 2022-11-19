use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path as FilePath;
use std::rc::Rc;

use crate::ast;
use crate::compile::error::*;
use crate::compile::inference::*;
use crate::compile::schema::*;
use crate::compile::sql::*;
use crate::parser::parse_schema;

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

        let s = compile_schema_from_file(file_path)?;
        (path.clone(), s.clone())
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
            .map(|n| (n.clone(), MType::new_unknown(n.as_str())))
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
                        MType::new_unknown("field"),
                    )]),
                    &*type_.borrow(),
                ));
            }
        }
        _ => {
            return Err(CompileError::wrong_type(
                &MType::Record(vec![MField::new_nullable(
                    name.clone(),
                    MType::new_unknown("field"),
                )]),
                &*type_.borrow(),
            ))
        }
    })
}

pub fn compile_expr(schema: Ref<Schema>, expr: &ast::Expr) -> Result<CTypedExpr> {
    match expr {
        ast::Expr::SQLQuery(q) => Ok(compile_sqlquery(schema.clone(), q)?),
        ast::Expr::SQLExpr(e) => {
            let scope = mkcref(SQLScope::new(schema.clone(), None));
            Ok(compile_sqlexpr(schema.clone(), scope.clone(), e)?)
        }
    }
}

pub fn rebind_decl(_schema: SchemaInstance, decl: &Decl) -> Result<SchemaEntry> {
    match &decl.value {
        SchemaEntry::Schema(s) => Ok(SchemaEntry::Schema(s.clone())),
        SchemaEntry::Type(t) => Ok(SchemaEntry::Type(t.clone())),
        SchemaEntry::Expr(e) => Ok(SchemaEntry::Expr(mkcref(STypedExpr {
            type_: e.then(|e: Ref<STypedExpr>| Ok(e.borrow().type_.clone()))?,
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

pub fn compile_schema_from_file(file_path: &FilePath) -> Result<Ref<Schema>> {
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
    compile_schema_ast(schema.clone(), ast)?;
    Ok(schema)
}

pub fn compile_schema_ast(schema: Ref<Schema>, ast: &ast::Schema) -> Result<()> {
    declare_schema_entries(schema.clone(), ast)?;
    compile_schema_entries(schema.clone(), ast)?;
    gather_schema_externs(schema)?;
    Ok(())
}

pub fn declare_schema_entries(schema: Ref<Schema>, ast: &ast::Schema) -> Result<()> {
    for stmt in &ast.stmts {
        let entries: Vec<(String, bool, SchemaEntry)> = match &stmt.body {
            ast::StmtBody::Noop => continue,
            ast::StmtBody::Expr(_) => continue,
            ast::StmtBody::Import { path, list, .. } => {
                let imported = lookup_schema(schema.clone(), &path)?;
                if imported.borrow().args.is_some() {
                    return Err(CompileError::unimplemented("Importing with arguments"));
                }

                // XXX Importing schemas with extern values is currently broken, because we don't
                // actually "inject" any meaningful reference to imported_schema's id into the decl
                // during rebind_decl.  We should figure out how to generate a new set of decls for
                // the imported schema (w/ the imported args)
                //
                // let checked = match args {
                //     None => None,
                //     Some(args) => {
                //         let mut externs = imported.borrow().schema.borrow().externs.clone();
                //         let mut checked = BTreeMap::new();
                //         for arg in args {
                //             let expr = match &arg.expr {
                //                 None => ast::Expr::SQLExpr(sqlast::Expr::CompoundIdentifier(vec![
                //                     sqlast::Ident {
                //                         value: arg.name.clone(),
                //                         quote_style: None,
                //                     },
                //                 ])),
                //                 Some(expr) => expr.clone(),
                //             };

                //             if checked.get(&arg.name).is_some() {
                //                 return Err(CompileError::duplicate_entry(vec![arg.name.clone()]));
                //             }

                //             if let Some(extern_) = externs.get_mut(&arg.name) {
                //                 let compiled = compile_expr(schema.clone(), &expr)?;

                //                 extern_.unify(&compiled.type_)?;
                //                 checked.insert(
                //                     arg.name.clone(),
                //                     TypedNameAndExpr {
                //                         name: arg.name.clone(),
                //                         type_: extern_.clone(),
                //                         expr: compiled.expr,
                //                     },
                //                 );
                //             } else {
                //                 return Err(CompileError::no_such_entry(vec![arg.name.clone()]));
                //             }
                //         }

                //         Some(checked)
                //     }
                // };

                // let id = {
                //     let imported_args = &mut imported.borrow_mut().args;
                //     if let Some(imported_args) = imported_args {
                //         if let Some(checked) = checked {
                //             let id = imported_args.len();
                //             imported_args.push(checked);
                //             Some(id)
                //         } else {
                //             return Err(CompileError::import_error(
                //                 path.clone(),
                //                 "Arguments are not provided to module with extern declarations",
                //             ));
                //         }
                //     } else if args.is_some() {
                //         return Err(CompileError::import_error(
                //               path.clone(),
                //             "Arguments should not be provided to module without extern declarations",
                //         ));
                //     } else {
                //         None
                //     }
                // };

                let mut imports = Vec::new();
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
                                id: None,
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
                                id: None,
                            };

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
                SchemaEntry::Type(MType::new_unknown(nt.name.as_str())),
            )],
            ast::StmtBody::FnDef { name, .. } => {
                vec![(
                    name.clone(),
                    false, /* extern_ */
                    SchemaEntry::Expr(STypedExpr::new_unknown(name.as_str())),
                )]
            }
            ast::StmtBody::Let { name, .. } => {
                vec![(
                    name.clone(),
                    false, /* extern_ */
                    SchemaEntry::Expr(STypedExpr::new_unknown(name.as_str())),
                )]
            }
            ast::StmtBody::Extern { name, .. } => vec![(
                name.clone(),
                true, /* extern_ */
                SchemaEntry::Expr(STypedExpr::new_unknown(name.as_str())),
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
        }
    }

    Ok(())
}

pub fn unify_type_decl(schema: Ref<Schema>, name: &str, type_: CRef<MType>) -> Result<()> {
    let s = schema.borrow();
    let decl = s.decls.get(name).ok_or_else(|| {
        CompileError::internal(
            format!(
                "Could not find type declaration {} during reprocessing",
                name
            )
            .as_str(),
        )
    })?;
    match &decl.value {
        SchemaEntry::Type(t) => t.unify(&type_)?,
        _ => {
            return Err(CompileError::internal(
                format!(
                    "Expected {} to be a type declaration during reprocessing",
                    name
                )
                .as_str(),
            ))
        }
    }

    Ok(())
}

pub fn unify_expr_decl(schema: Ref<Schema>, name: &str, value: CRef<STypedExpr>) -> Result<()> {
    let s = schema.borrow();
    let decl = s.decls.get(name).ok_or_else(|| {
        CompileError::internal(
            format!(
                "Could not find type declaration {} during reprocessing",
                name
            )
            .as_str(),
        )
    })?;
    match &decl.value {
        SchemaEntry::Expr(e) => e.unify(&value)?,
        _ => {
            return Err(CompileError::internal(
                format!(
                    "Expected {} to be a type declaration during reprocessing",
                    name
                )
                .as_str(),
            ))
        }
    }

    Ok(())
}

pub fn compile_schema_entries(schema: Ref<Schema>, ast: &ast::Schema) -> Result<()> {
    for stmt in &ast.stmts {
        match &stmt.body {
            ast::StmtBody::Noop => continue,
            ast::StmtBody::Expr(expr) => {
                let compiled = compile_expr(schema.clone(), expr)?;
                schema.borrow_mut().exprs.push(compiled);
            }
            ast::StmtBody::Import { .. } => continue,
            ast::StmtBody::TypeDef(nt) => {
                let type_ = resolve_type(schema.clone(), &nt.def)?;
                unify_type_decl(schema.clone(), nt.name.as_str(), type_)?;
            }
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
                            value: SchemaEntry::Expr(mkcref(STypedExpr {
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
                    resolve_type(inner_schema.clone(), ret)?.unify(&compiled.type_)?
                }

                unify_expr_decl(
                    schema.clone(),
                    name.as_str(),
                    mkcref(STypedExpr {
                        type_: SType::new_mono(mkcref(MType::Fn(MFnType {
                            args: compiled_args,
                            ret: compiled.type_.clone(),
                        }))),
                        expr: Rc::new(Expr::Fn(FnExpr {
                            inner_schema: inner_schema.clone(),
                            body: compiled.expr.clone(),
                        })),
                    }),
                )?;
            }
            ast::StmtBody::Let { name, type_, body } => {
                let lhs_type = if let Some(t) = type_ {
                    resolve_type(schema.clone(), &t)?
                } else {
                    MType::new_unknown(format!("typeof {}", name).as_str())
                };
                let compiled = compile_expr(schema.clone(), &body)?;
                lhs_type.unify(&compiled.type_)?;
                unify_expr_decl(
                    schema.clone(),
                    name.as_str(),
                    mkcref(STypedExpr {
                        type_: SType::new_mono(lhs_type),
                        expr: compiled.expr,
                    }),
                )?;
            }
            ast::StmtBody::Extern { name, type_ } => {
                unify_expr_decl(
                    schema.clone(),
                    name.as_str(),
                    mkcref(STypedExpr {
                        type_: SType::new_mono(resolve_type(schema.clone(), type_)?),
                        expr: Rc::new(Expr::Unknown),
                    }),
                )?;
            }
        };
    }

    Ok(())
}

pub fn gather_schema_externs(schema: Ref<Schema>) -> Result<()> {
    let s = schema.borrow();
    for (name, decl) in &s.decls {
        if decl.extern_ {
            match &decl.value {
                SchemaEntry::Expr(e) => {
                    schema.borrow_mut().externs.insert(
                        name.clone(),
                        e.must()?
                            .borrow()
                            .type_
                            .then(|t: Ref<SType>| Ok(t.borrow().instantiate()?))?,
                    );
                }
                _ => return Err(CompileError::unimplemented("type externs")),
            }
        }
    }

    Ok(())
}
