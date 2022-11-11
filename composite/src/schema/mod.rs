use crate::ast;
use crate::runtime;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

pub type Ident = ast::Ident;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AtomicType {
    Null,
    Bool,
    Number,
    String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Type {
    Unknown,
    Atom(AtomicType),
    Struct(BTreeMap<String, Type>),
    List(Box<Type>),
    Exclude {
        inner: Box<Type>,
        excluded: Vec<Ident>,
    },
}

pub type Value = runtime::Value;

#[derive(Clone, Debug)]
pub struct TypedExpr {
    pub type_: Type,
    pub expr: ast::Expr,
}

#[derive(Clone, Debug)]
pub enum SchemaEntry {
    Schema(Schema),
    Type(Type),
    Expr(TypedExpr),
}

#[derive(Clone, Debug)]
pub struct Decl {
    pub public: bool,
    pub name: String,
    pub value: SchemaEntry,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub folder: Option<String>,
    pub parent_scope: Option<Rc<Schema>>,
    pub externs: BTreeSet<String>,
    pub decls: BTreeMap<String, Decl>,
}

impl Schema {
    pub fn new(folder: Option<String>) -> Schema {
        Schema {
            folder,
            parent_scope: None,
            externs: BTreeSet::new(),
            decls: BTreeMap::new(),
        }
    }
}
