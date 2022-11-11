use crate::ast;
use crate::runtime;
use std::collections::BTreeMap;

pub type Ident = ast::Ident;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameAndType {
    pub name: Ident,
    pub def: Type,
}

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
    Struct(Vec<NameAndType>),
    List(Box<Type>),
    Exclude {
        inner: Box<Type>,
        excluded: Vec<Ident>,
    },
}

pub type Value = runtime::Value;

#[derive(Clone, Debug)]
pub struct TypedValue {
    pub type_: Type,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub enum SchemaEntry {
    Import(Schema),
    Type(Type),
    Value(TypedValue),
}

#[derive(Clone, Debug)]
pub struct Decl {
    pub public: bool,
    pub name: String,
    pub value: SchemaEntry,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub decls: BTreeMap<String, Decl>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema {
            decls: BTreeMap::new(),
        }
    }
}
