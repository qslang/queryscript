use sqlparser::ast as sqlast;

pub type Ident = String;
pub type Path = Vec<Ident>;

#[derive(Debug)]
pub struct NameAndType {
    pub name: Ident,
    pub def: Type,
}

#[derive(Debug)]
pub struct NameAndExpr {
    pub name: Ident,
    pub val: Option<Expr>,
}

#[derive(Debug)]
pub enum StructEntry {
    NameAndType(NameAndType),
    Include(Path),
}

#[derive(Debug)]
pub enum Type {
    Reference(Path),
    Struct(Vec<StructEntry>),
    List(Box<Type>),
    Exclude {
        inner: Box<Type>,
        excluded: Vec<Ident>,
    },
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
pub enum ImportList {
    Star,
    Items(Vec<Path>),
}

#[derive(Debug)]
pub enum StmtBody {
    Noop,
    Import {
        path: Path,
        list: ImportList,
        args: Option<Vec<NameAndExpr>>,
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
