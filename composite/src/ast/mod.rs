use sqlparser::{
    // ast::{ColumnDef, ColumnOptionDef, Statement as SQLStatement, TableConstraint},
    // dialect::{keywords::Keyword, Dialect, GenericDialect},
    // parser::{Parser, ParserError},
    tokenizer::{Token},
};

pub struct StructEntry {
    name: String,
    def: Type,
}

pub enum Type {
    Reference(String),
    Struct(Vec<StructEntry>),
}

pub struct FnArg {
    name: String,
    type_: Option<Type>,
}

#[derive(Debug)]
pub enum Expr {
}

#[derive(Debug)]
pub enum Stmt {
    Import(),
    TypeDef{
        name: String,
        def: Type,
    },
    FnDef{
        name: String,
        generics: Vec<String>,
        args: Vec<FnArg>,
        ret: Option<Type>,
        body: Expr,
    },
    Let{
        name: String,
        extern_: bool,
        export: bool,
        type_: Option<Type>,
        body: Expr,
    },
}

#[derive(Debug)]
pub enum Schema {
    TODO(Vec<Token>),
    Stmts(Vec<Stmt>),
}

