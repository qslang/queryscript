use crate::ast;
use crate::runtime;
use crate::types::{AtomicType, Type};
use sqlparser::ast as sqlast;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

pub type Ident = ast::Ident;
pub type STypeRef = Rc<RefCell<SType>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SFnType {
    pub args: Vec<SField>,
    pub ret: Box<SType>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SField {
    pub name: String,
    pub type_: SType,
    pub nullable: bool,
}

impl SField {
    pub fn new_nullable(name: String, type_: SType) -> SField {
        SField {
            name,
            type_,
            nullable: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SType {
    Unknown,
    Atom(AtomicType),
    Struct(Vec<SField>),
    List(Box<SType>),
    Exclude {
        inner: Box<SType>,
        excluded: Vec<Ident>,
    },
    Fn(SFnType),
    Ref(STypeRef),
}

impl SType {
    pub fn to_runtime_type(&self) -> runtime::error::Result<Type> {
        match self {
            SType::Atom(a) => Ok(Type::Atom(a.clone())),
            SType::Struct(fields) => Ok(Type::Struct(
                fields
                    .iter()
                    .map(|f| {
                        Ok(Field {
                            name: f.name.clone(),
                            type_: f.type_.to_runtime_type()?,
                            nullable: f.nullable,
                        })
                    })
                    .collect::<runtime::error::Result<Vec<_>>>()?,
            )),
            SType::List(inner) => Ok(Type::List(Box::new(inner.to_runtime_type()?))),
            SType::Fn(SFnType { args, ret }) => Ok(Type::Fn(FnType {
                args: args
                    .iter()
                    .map(|a| {
                        Ok(Field {
                            name: a.name.clone(),
                            type_: a.type_.to_runtime_type()?,
                            nullable: a.nullable,
                        })
                    })
                    .collect::<runtime::error::Result<Vec<_>>>()?,
                ret: Box::new(ret.to_runtime_type()?),
            })),
            _ => runtime::error::fail!("Unresolved type cannot exist at runtime: {:?}", self),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SchemaInstance {
    pub schema: SchemaRef,
    pub id: Option<usize>,
}

impl fmt::Debug for SchemaInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(f.debug_struct("FnExpr")
            .field("id", &self.id)
            .finish_non_exhaustive()?)
    }
}

impl SchemaInstance {
    pub fn global(schema: SchemaRef) -> SchemaInstance {
        SchemaInstance { schema, id: None }
    }

    pub fn instance(schema: SchemaRef, id: usize) -> SchemaInstance {
        SchemaInstance {
            schema,
            id: Some(id),
        }
    }
}

pub type Value = runtime::Value;

pub type Params<Ty> = BTreeMap<ast::Ident, TypedExpr<Ty>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SQLExpr<Ty> {
    pub params: Params<Ty>,
    pub expr: sqlast::Expr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SQLQuery<Ty> {
    pub params: Params<Ty>,
    pub query: sqlast::Query,
}

#[derive(Clone, Eq, PartialEq)]
pub struct FnExpr<Ty> {
    pub inner_schema: Rc<RefCell<Schema>>,
    pub body: Box<Expr<Ty>>,
}

impl<Ty: fmt::Debug> fmt::Debug for FnExpr<Ty> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(f.debug_struct("FnExpr")
            .field("body", &self.body)
            .finish_non_exhaustive()?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FnCallExpr<Ty> {
    pub func: Box<Expr<Ty>>,
    pub args: Vec<Expr<Ty>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr<Ty> {
    SQLQuery(SQLQuery<Ty>),
    SQLExpr(SQLExpr<Ty>),
    Decl(Decl),
    Fn(FnExpr<Ty>),
    FnCall(FnCallExpr<Ty>),
    NativeFn(String),
    Unknown,
}

impl Expr<SType> {
    pub fn to_runtime_type(&self) -> runtime::error::Result<Expr<Type>> {
        match self {
            Expr::SQLQuery(SQLQuery { params, query }) => Ok(Expr::SQLQuery(SQLQuery {
                params: params
                    .iter()
                    .map(|(name, param)| Ok((name.clone(), param.to_runtime_type()?)))
                    .collect::<runtime::error::Result<_>>()?,
                query: query.clone(),
            })),
            Expr::SQLExpr(SQLExpr { params, expr }) => Ok(Expr::SQLExpr(SQLExpr {
                params: params
                    .iter()
                    .map(|(name, param)| Ok((name.clone(), param.to_runtime_type()?)))
                    .collect::<runtime::error::Result<_>>()?,
                expr: expr.clone(),
            })),
            Expr::Fn(FnExpr { inner_schema, body }) => Ok(Expr::Fn(FnExpr {
                inner_schema: inner_schema.clone(),
                body: Box::new(body.to_runtime_type()?),
            })),
            Expr::FnCall(FnCallExpr { func, args }) => Ok(Expr::FnCall(FnCallExpr {
                func: Box::new(func.to_runtime_type()?),
                args: args
                    .iter()
                    .map(|a| Ok(a.to_runtime_type()?))
                    .collect::<runtime::error::Result<_>>()?,
            })),
            Expr::Decl(d) => Ok(Expr::Decl(d.clone())),
            Expr::NativeFn(f) => Ok(Expr::NativeFn(f.clone())),
            Expr::Unknown => Ok(Expr::Unknown),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedNameAndSQLExpr<Ty> {
    pub name: String,
    pub type_: SType,
    pub expr: SQLExpr<Ty>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedSQLExpr<Ty> {
    pub type_: SType,
    pub expr: SQLExpr<Ty>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedExpr<Ty> {
    pub type_: Ty,
    pub expr: Expr<Ty>,
}

impl TypedExpr<SType> {
    pub fn to_runtime_type(&self) -> runtime::error::Result<TypedExpr<Type>> {
        Ok(TypedExpr::<Type> {
            type_: self.type_.to_runtime_type()?,
            expr: self.expr.to_runtime_type()?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaEntry {
    Schema(ast::Path),
    Type(Rc<RefCell<SType>>),
    Expr(Rc<RefCell<TypedExpr<SType>>>),
}

pub fn mkref<T>(t: T) -> Rc<RefCell<T>> {
    Rc::new(RefCell::new(t))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Decl {
    pub public: bool,
    pub extern_: bool,
    pub name: String,
    pub value: SchemaEntry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedNameAndExpr<Ty> {
    pub name: String,
    pub type_: Ty,
    pub expr: Expr<Ty>,
}

pub type SchemaRef = Rc<RefCell<Schema>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedName<Ty> {
    pub name: String,
    pub type_: Ty,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportedSchema {
    pub args: Option<Vec<BTreeMap<String, TypedNameAndExpr<SType>>>>,
    pub schema: SchemaRef,
}

// XXX We should implement a cheaper Eq / PartialEq over Schema, because it's
// currently used to check if two types are equal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Schema {
    pub folder: Option<String>,
    pub parent_scope: Option<Rc<RefCell<Schema>>>,
    pub next_placeholder: usize,
    pub externs: BTreeMap<String, SType>,
    pub decls: BTreeMap<String, Decl>,
    pub imports: BTreeMap<ast::Path, Rc<RefCell<ImportedSchema>>>,
}

impl Schema {
    pub fn new(folder: Option<String>) -> Rc<RefCell<Schema>> {
        Rc::new(RefCell::new(Schema {
            folder,
            parent_scope: None,
            next_placeholder: 1,
            externs: BTreeMap::new(),
            decls: BTreeMap::new(),
            imports: BTreeMap::new(),
        }))
    }

    pub fn new_global_schema() -> Rc<RefCell<Schema>> {
        Rc::new(RefCell::new(Schema {
            folder: None,
            parent_scope: None,
            next_placeholder: 1,
            externs: BTreeMap::new(),
            imports: BTreeMap::new(),
            decls: BTreeMap::from([
                (
                    "number".to_string(),
                    Decl {
                        public: true,
                        extern_: false,
                        name: "number".to_string(),
                        value: SchemaEntry::Type(mkref(SType::Atom(AtomicType::Float64))),
                    },
                ),
                (
                    "string".to_string(),
                    Decl {
                        public: true,
                        extern_: false,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(mkref(SType::Atom(AtomicType::Utf8))),
                    },
                ),
                (
                    "bool".to_string(),
                    Decl {
                        public: true,
                        extern_: false,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(mkref(SType::Atom(AtomicType::Boolean))),
                    },
                ),
                (
                    "null".to_string(),
                    Decl {
                        public: true,
                        extern_: false,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(mkref(SType::Atom(AtomicType::Null))),
                    },
                ),
                (
                    "load_json".to_string(),
                    Decl {
                        public: true,
                        extern_: false,
                        name: "load_json".to_string(),
                        value: SchemaEntry::Expr(mkref(TypedExpr {
                            type_: SType::Fn(SFnType {
                                args: vec![SField {
                                    name: "file".to_string(),
                                    type_: SType::Atom(AtomicType::Utf8),
                                    nullable: false,
                                }],
                                ret: Box::new(SType::List(Box::new(SType::Unknown))),
                            }),
                            expr: Expr::NativeFn("load_json".to_string()),
                        })),
                    },
                ),
            ]),
        }))
    }
}
