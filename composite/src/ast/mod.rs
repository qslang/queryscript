use sqlparser::ast as sqlast;
use sqlparser::tokenizer;

pub type Ident = String;
pub type Path = Vec<Ident>;

#[derive(Debug)]
pub struct NameAndType {
    pub name: Ident,
    pub def: Type,
}

#[derive(Debug)]
pub enum StructEntry {
    NameAndType(NameAndType),
    Include(Path),
}

#[derive(Debug)]
pub enum Type {
    TODO(Vec<tokenizer::Token>),
    Reference(Path),
    Struct(Vec<StructEntry>),
}

#[derive(Debug)]
pub struct FnArg {
    pub name: Ident,
    pub type_: Option<Type>,
}

#[derive(Debug)]
pub enum Expr {
    SQLQuery(sqlast::Query),
    SQLExpr(sqlast::Expr),
}

#[derive(Debug)]
pub enum ImportArgs {
    None,
    Star,
    // Items(Vec<Path>),
}

#[derive(Debug)]
pub enum StmtBody {
    Import {
        path: Path,
        args: ImportArgs,
    },
    TypeDef(NameAndType),
    FnDef {
        name: Ident,
        generics: Vec<Ident>,
        args: Vec<FnArg>,
        ret: Option<Type>,
        body: Expr,
    },
    Let {
        name: Ident,
        type_: Option<Type>,
        body: Expr,
    },
    Extern {
        name: Ident,
        type_: Type,
    },
}

#[derive(Debug)]
pub struct Stmt {
    pub export: bool,
    pub body: StmtBody,
}

#[derive(Debug)]
pub struct Schema {
    pub stmts: Vec<Stmt>,
}
