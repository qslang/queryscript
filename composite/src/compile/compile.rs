use crate::ast;
use crate::compile::error::*;
use crate::parser::parse_schema;
use crate::schema::*;
use sqlparser::ast as sqlast;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

pub fn new_global_schema() -> Schema {
    Schema {
        parent_scope: None,
        externs: BTreeSet::new(),
        decls: BTreeMap::from([
            (
                "number".to_string(),
                Decl {
                    public: true,
                    name: "number".to_string(),
                    value: SchemaEntry::Type(Type::Atom(AtomicType::Number)),
                },
            ),
            (
                "string".to_string(),
                Decl {
                    public: true,
                    name: "string".to_string(),
                    value: SchemaEntry::Type(Type::Atom(AtomicType::String)),
                },
            ),
            (
                "bool".to_string(),
                Decl {
                    public: true,
                    name: "string".to_string(),
                    value: SchemaEntry::Type(Type::Atom(AtomicType::Bool)),
                },
            ),
            (
                "null".to_string(),
                Decl {
                    public: true,
                    name: "string".to_string(),
                    value: SchemaEntry::Type(Type::Atom(AtomicType::Null)),
                },
            ),
        ]),
    }
}

pub fn lookup_path<'a>(mut schema: &'a Schema, path: &ast::Path) -> Result<(&'a Decl, ast::Path)> {
    if path.len() == 0 {
        return Err(CompileError::no_such_entry(path.clone()));
    }

    for i in 0..path.len() - 1 {
        if let Some(next) = schema.decls.get(&path[i]) {
            if i > 0 && !next.public {
                return Err(CompileError::no_such_entry(path.clone()));
            }

            match &next.value {
                SchemaEntry::Schema(s) => {
                    schema = &s;
                }
                _ => return Ok((&next, path[i + 1..].to_vec())),
            }
        } else {
            return Err(CompileError::no_such_entry(path.clone()));
        }
    }

    if let Some(last) = schema.decls.get(&path[path.len() - 1]) {
        if path.len() > 1 && !last.public {
            return Err(CompileError::no_such_entry(path.clone()));
        }

        return Ok((last, Vec::new()));
    }

    return Err(CompileError::no_such_entry(path.clone()));
}

pub fn resolve_type(schema: &Schema, ast: &ast::Type) -> Result<Type> {
    match ast {
        ast::Type::Reference(path) => {
            let decl = {
                let mut current = schema;
                let decl = loop {
                    match lookup_path(current, path) {
                        Err(CompileError::NoSuchEntry { .. }) => {
                            if let Some(parent_scope) = &current.parent_scope {
                                current = parent_scope.as_ref();
                                continue;
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

            match &decl.value {
                SchemaEntry::Type(t) => Ok(t.clone()),
                _ => Err(CompileError::wrong_kind(path.clone(), "type", &decl)),
            }
        }
        ast::Type::Struct(entries) => {
            let mut fields = BTreeMap::new();
            for e in entries {
                match e {
                    ast::StructEntry::NameAndType(nt) => {
                        if fields.contains_key(&nt.name) {
                            return Err(CompileError::duplicate_entry(vec![nt.name.clone()]));
                        }
                        fields.insert(nt.name.clone(), resolve_type(schema, &nt.def)?);
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
    let schema = new_global_schema();
    resolve_type(&schema, &ast::Type::Reference(vec![name.to_string()]))
}

pub fn resolve_expr(schema: &Schema, expr: &ast::Expr) -> Result<Type> {
    match expr {
        ast::Expr::Unknown => Ok(Type::Unknown),
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
                let (decl, remainder) = lookup_path(schema, &path)?;
                let type_ = match &decl.value {
                    SchemaEntry::Expr(v) => v.clone(),
                    _ => return Err(CompileError::wrong_kind(path.clone(), "value", &decl)),
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

pub fn typecheck(schema: &Schema, lhs_type: &Type, expr: &ast::Expr) -> Result<Type> {
    let rhs_type = resolve_expr(schema, expr)?;

    unify_types(lhs_type, &rhs_type)
}

pub fn compile_schema_from_string(contents: &str) -> Result<Schema> {
    let ast = parse_schema(contents)?;

    compile_schema(&ast)
}

pub fn compile_schema(ast: &ast::Schema) -> Result<Schema> {
    let mut schema = Schema::new();
    schema.parent_scope = Some(Rc::new(new_global_schema()));

    for stmt in &ast.stmts {
        let (name, value) = match &stmt.body {
            ast::StmtBody::Noop => continue,
            ast::StmtBody::Import { .. } => return Err(CompileError::unimplemented("import")),
            ast::StmtBody::TypeDef(nt) => (
                nt.name.clone(),
                SchemaEntry::Type(resolve_type(&schema, &nt.def)?),
            ),
            ast::StmtBody::FnDef { .. } => return Err(CompileError::unimplemented("fn")),
            ast::StmtBody::Let { name, type_, body } => {
                let lhs_type = if let Some(t) = type_ {
                    resolve_type(&schema, &t)?
                } else {
                    Type::Unknown
                };
                (
                    name.clone(),
                    SchemaEntry::Expr(TypedExpr {
                        type_: typecheck(&schema, &lhs_type, &body)?,
                        expr: body.clone(),
                    }),
                )
            }
            ast::StmtBody::Extern { name, type_ } => (
                name.clone(),
                SchemaEntry::Expr(TypedExpr {
                    type_: resolve_type(&schema, type_)?,
                    expr: ast::Expr::Unknown,
                }),
            ),
        };

        if schema.decls.contains_key(&name) {
            return Err(CompileError::duplicate_entry(vec![name]));
        }

        schema.decls.insert(
            name.clone(),
            Decl {
                public: stmt.export,
                name,
                value,
            },
        );
    }

    Ok(schema)
}
