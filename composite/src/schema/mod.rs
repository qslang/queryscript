use crate::ast;
use crate::runtime;
use sqlparser::ast as sqlast;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

pub type Ident = ast::Ident;

pub type SchemaPathEntry = (ast::Path, Option<usize>);
pub type SchemaPath = Vec<SchemaPathEntry>;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Path {
    pub items: ast::Path,
    pub schema: SchemaPath,
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
pub struct SQLExpr {
    pub params: BTreeMap<String, Expr>,
    pub expr: sqlast::Expr,
}

#[derive(Clone, Debug)]
pub enum Expr {
    SQLQuery {
        // XXX This is just a passthrough and doesn't perform any compilation yet
        query: sqlast::Query,
    },
    SQLExpr(SQLExpr),
    Path(Path),
    Unknown,
}

#[derive(Clone, Debug)]
pub struct TypedSQLExpr {
    pub type_: Type,
    pub expr: SQLExpr,
}

#[derive(Clone, Debug)]
pub struct TypedExpr {
    pub type_: Type,
    pub expr: Expr,
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
    pub extern_: bool,
    pub name: String,
    pub value: SchemaEntry,
}

#[derive(Clone, Debug)]
pub struct TypedNameAndExpr {
    pub name: String,
    pub type_: Type,
    pub expr: Expr,
}

#[derive(Clone, Debug)]
pub struct ImportedSchema {
    pub args: Option<Vec<BTreeMap<String, TypedNameAndExpr>>>,
    pub schema: Rc<RefCell<Schema>>,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub folder: Option<String>,
    pub parent_scope: Option<Rc<RefCell<Schema>>>,
    pub next_placeholder: usize,
    pub externs: BTreeMap<String, Type>,
    pub decls: BTreeMap<String, Rc<RefCell<Decl>>>,
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
                    Rc::new(RefCell::new(Decl {
                        public: true,
                        extern_: false,
                        name: "number".to_string(),
                        value: SchemaEntry::Type(Type::Atom(AtomicType::Number)),
                    })),
                ),
                (
                    "string".to_string(),
                    Rc::new(RefCell::new(Decl {
                        public: true,
                        extern_: false,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(Type::Atom(AtomicType::String)),
                    })),
                ),
                (
                    "bool".to_string(),
                    Rc::new(RefCell::new(Decl {
                        public: true,
                        extern_: false,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(Type::Atom(AtomicType::Bool)),
                    })),
                ),
                (
                    "null".to_string(),
                    Rc::new(RefCell::new(Decl {
                        public: true,
                        extern_: false,
                        name: "string".to_string(),
                        value: SchemaEntry::Type(Type::Atom(AtomicType::Null)),
                    })),
                ),
            ]),
        }))
    }
}
