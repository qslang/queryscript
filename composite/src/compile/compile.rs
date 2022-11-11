use crate::ast;
use crate::compile::error::*;
use crate::parser::parse_schema;
use crate::runtime;
use crate::schema::*;
use sqlparser::ast as sqlast;
use std::collections::BTreeMap;

lazy_static! {
    static ref GLOBALS: Schema = Schema {
        decls: BTreeMap::from([(
            "number".to_string(),
            Decl {
                public: true,
                name: "number".to_string(),
                value: SchemaEntry::Type(Type::Atom(AtomicType::Number)),
            },
        )]),
    };
}

pub fn lookup_path<'a>(mut schema: &'a Schema, path: &ast::Path) -> Result<&'a Decl> {
    if path.len() == 0 {
        return Err(CompileError::no_such_entry(path.clone()));
    }

    for i in 0..path.len() - 1 {
        if let Some(next) = schema.decls.get(&path[i]) {
            if !next.public {
                return Err(CompileError::no_such_entry(path.clone()));
            }

            match &next.value {
                SchemaEntry::Import(s) => {
                    schema = &s;
                }
                _ => return Err(CompileError::no_such_entry(path.clone())),
            }
        } else {
            return Err(CompileError::no_such_entry(path.clone()));
        }
    }

    if let Some(last) = schema.decls.get(&path[path.len() - 1]) {
        if !last.public {
            return Err(CompileError::no_such_entry(path.clone()));
        }

        return Ok(last);
    }

    return Err(CompileError::no_such_entry(path.clone()));
}

pub fn resolve_type(schema: Option<&Schema>, ast: &ast::Type) -> Result<Type> {
    match ast {
        ast::Type::Reference(path) => {
            let global_decl = match lookup_path(&GLOBALS, path) {
                Ok(d) => Some(d),
                Err(e) => {
                    if matches!(e, CompileError::NoSuchEntry { .. }) {
                        None
                    } else {
                        return Err(e);
                    }
                }
            };

            let decl = if let Some(d) = global_decl {
                d
            } else if let Some(s) = schema {
                lookup_path(s, path)?
            } else {
                return Err(CompileError::no_such_entry(path.clone()));
            };

            match &decl.value {
                SchemaEntry::Type(t) => Ok(t.clone()),
                SchemaEntry::Import { .. } => {
                    Err(CompileError::wrong_kind(path.clone(), "type", "module"))
                }
                SchemaEntry::Value { .. } => {
                    Err(CompileError::wrong_kind(path.clone(), "type", "value"))
                }
            }
        }
        ast::Type::Struct(entries) => {
            let mut fields = Vec::new();
            for e in entries {
                match e {
                    ast::StructEntry::NameAndType(nt) => {
                        fields.push(NameAndType {
                            name: nt.name.clone(),
                            def: resolve_type(schema, &nt.def)?,
                        });
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
    resolve_type(None, &ast::Type::Reference(vec![name.to_string()]))
}

pub fn resolve_expr(_schema: Option<&Schema>, expr: &ast::Expr) -> Result<Type> {
    match expr {
        ast::Expr::SQLQuery(_) => {
            return Err(CompileError::unimplemented("SELECT"));
        }
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
            _ => {
                return Err(CompileError::unimplemented(e.to_string().as_str()));
            }
        },
    }
}

pub fn unify_types(lhs: &Type, rhs: &Type) -> Result<Type> {
    if *lhs != *rhs {
        return Err(CompileError::wrong_type(lhs, rhs));
    }

    Ok(lhs.clone())
}

pub fn typecheck(schema: Option<&Schema>, lhs_type: &Type, expr: &ast::Expr) -> Result<Type> {
    let rhs_type = resolve_expr(schema, expr)?;

    unify_types(lhs_type, &rhs_type)
}

pub fn compile_schema_from_string(contents: &str) -> Result<Schema> {
    let ast = parse_schema(contents)?;

    compile_schema(&ast)
}

pub fn compile_schema(ast: &ast::Schema) -> Result<Schema> {
    let mut schema = Schema::new();

    for stmt in &ast.stmts {
        match &stmt.body {
            ast::StmtBody::Noop => {}
            ast::StmtBody::Import { .. } => return Err(CompileError::unimplemented("import")),
            ast::StmtBody::TypeDef(_) => return Err(CompileError::unimplemented("type")),
            ast::StmtBody::FnDef { .. } => return Err(CompileError::unimplemented("fn")),
            ast::StmtBody::Let { name, type_, body } => {
                let lhs_type = if let Some(t) = type_ {
                    resolve_type(Some(&schema), &t)?
                } else {
                    Type::Unknown
                };
                schema.decls.insert(
                    name.clone(),
                    Decl {
                        public: stmt.export,
                        name: name.clone(),
                        value: SchemaEntry::Value(TypedValue {
                            type_: typecheck(Some(&schema), &lhs_type, &body)?,
                            value: runtime::eval(&schema, &body)?,
                        }),
                    },
                );
            }
            ast::StmtBody::Extern { .. } => return Err(CompileError::unimplemented("extern")),
        }
    }

    Ok(schema)
}
