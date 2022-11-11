use crate::ast;
use crate::runtime;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

pub type Ident = ast::Ident;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Path {
    pub items: ast::Path,
    pub schema: Vec<ast::Path>,
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
    Struct(BTreeMap<String, Type>),
    List(Box<Type>),
    Exclude {
        inner: Box<Type>,
        excluded: Vec<Ident>,
    },
    Ref(Path),
}

pub type Value = runtime::Value;

#[derive(Clone, Debug)]
pub struct TypedExpr {
    pub type_: Type,
    pub expr: ast::Expr,
}

#[derive(Clone, Debug)]
pub enum SchemaEntry {
    Schema(ast::Path),
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
    pub parent_scope: Option<Rc<RefCell<Schema>>>,
    pub externs: BTreeSet<String>,
    pub decls: BTreeMap<String, Rc<RefCell<Decl>>>,
    pub imports: BTreeMap<ast::Path, Rc<RefCell<Schema>>>,
}

impl Schema {
    pub fn new(folder: Option<String>) -> Rc<RefCell<Schema>> {
        Rc::new(RefCell::new(Schema {
            folder,
            parent_scope: Some(Schema::new_global_schema()),
            externs: BTreeSet::new(),
            decls: BTreeMap::new(),
            imports: BTreeMap::new(),
        }))
    }

    pub fn new_global_schema() -> Rc<RefCell<Schema>> {
        Rc::new(RefCell::new(Schema {
            folder: None,
            parent_scope: None,
            externs: BTreeSet::new(),
            imports: BTreeMap::new(),
            decls: BTreeMap::from([
                (
                    "number".to_string(),
                    Rc::new(RefCell::new(Decl {
                        public: true,
                        name: "number".to_string(),
                        value: SchemaEntry::Type(Type::Atom(AtomicType::Number)),
                    })),
                ),
                (
                    "string".to_string(),
                    Rc::new(RefCell::new(Decl {
                        public: true,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(Type::Atom(AtomicType::String)),
                    })),
                ),
                (
                    "bool".to_string(),
                    Rc::new(RefCell::new(Decl {
                        public: true,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(Type::Atom(AtomicType::Bool)),
                    })),
                ),
                (
                    "null".to_string(),
                    Rc::new(RefCell::new(Decl {
                        public: true,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(Type::Atom(AtomicType::Null)),
                    })),
                ),
            ]),
        }))
    }
}
