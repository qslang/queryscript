use crate::ast;
use crate::runtime;
use crate::types::{AtomicType, Field, FnType, Type};
use sqlparser::ast as sqlast;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

pub type Ident = ast::Ident;

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

pub type Params = BTreeMap<ast::Ident, TypedExpr>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SQLExpr {
    pub params: Params,
    pub expr: sqlast::Expr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SQLQuery {
    pub params: Params,
    pub query: sqlast::Query,
}

#[derive(Clone, Eq, PartialEq)]
pub struct FnExpr {
    pub inner_schema: Rc<RefCell<Schema>>,
    pub body: Box<Expr>,
}

impl fmt::Debug for FnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(f.debug_struct("FnExpr")
            .field("body", &self.body)
            .finish_non_exhaustive()?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FnCallExpr {
    pub func: Box<Expr>,
    pub args: Vec<Expr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    SQLQuery(SQLQuery),
    SQLExpr(SQLExpr),
    Decl(Decl),
    Fn(FnExpr),
    FnCall(FnCallExpr),
    NativeFn(String),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedNameAndSQLExpr {
    pub name: String,
    pub type_: Type,
    pub expr: SQLExpr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedSQLExpr {
    pub type_: Type,
    pub expr: SQLExpr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedExpr {
    pub type_: Type,
    pub expr: Expr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaEntry {
    Schema(ast::Path),
    Type(Rc<RefCell<Type>>),
    Expr(Rc<RefCell<TypedExpr>>),
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
pub struct TypedNameAndExpr {
    pub name: String,
    pub type_: Type,
    pub expr: Expr,
}

pub type SchemaRef = Rc<RefCell<Schema>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedName {
    pub name: String,
    pub type_: Type,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportedSchema {
    pub args: Option<Vec<BTreeMap<String, TypedNameAndExpr>>>,
    pub schema: SchemaRef,
}

// XXX We should implement a cheaper Eq / PartialEq over Schema, because it's
// currently used to check if two types are equal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Schema {
    pub folder: Option<String>,
    pub parent_scope: Option<Rc<RefCell<Schema>>>,
    pub next_placeholder: usize,
    pub externs: BTreeMap<String, Type>,
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
                        value: SchemaEntry::Type(mkref(Type::Atom(AtomicType::Float64))),
                    },
                ),
                (
                    "string".to_string(),
                    Decl {
                        public: true,
                        extern_: false,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(mkref(Type::Atom(AtomicType::Utf8))),
                    },
                ),
                (
                    "bool".to_string(),
                    Decl {
                        public: true,
                        extern_: false,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(mkref(Type::Atom(AtomicType::Boolean))),
                    },
                ),
                (
                    "null".to_string(),
                    Decl {
                        public: true,
                        extern_: false,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(mkref(Type::Atom(AtomicType::Null))),
                    },
                ),
                (
                    "load_json".to_string(),
                    Decl {
                        public: true,
                        extern_: false,
                        name: "load_json".to_string(),
                        value: SchemaEntry::Expr(mkref(TypedExpr {
                            type_: Type::Fn(FnType {
                                args: vec![Field {
                                    name: "file".to_string(),
                                    type_: Type::Atom(AtomicType::Utf8),
                                    nullable: false,
                                }],
                                ret: Box::new(Type::List(Box::new(Type::Unknown))),
                            }),
                            expr: Expr::NativeFn("load_json".to_string()),
                        })),
                    },
                ),
            ]),
        }))
    }
}
