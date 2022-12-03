use sqlparser::ast as sqlast;

pub use sqlparser::tokenizer::Location;

pub type Ident = String;

pub type Path = Vec<Ident>;

pub trait IntoPath {
    fn to_path(&self) -> Path;
}

impl IntoPath for &Vec<sqlast::Ident> {
    fn to_path(&self) -> Path {
        self.iter()
            .map(|p| match p.quote_style {
                Some(_) => p.value.clone(), // Preserve the case if the string is quoted
                None => p.value.to_lowercase(),
            })
            .collect()
    }
}

impl IntoPath for &sqlast::ObjectName {
    fn to_path(&self) -> Path {
        (&self.0).to_path()
    }
}

#[derive(Clone, Debug)]
pub struct NameAndType {
    pub name: Ident,
    pub def: Type,
}

#[derive(Clone, Debug)]
pub struct NameAndExpr {
    pub name: Ident,
    pub expr: Option<Expr>,
}

#[derive(Clone, Debug)]
pub enum StructEntry {
    NameAndType(NameAndType),
    Include(Path),
}

#[derive(Clone, Debug)]
pub enum TypeBody {
    Reference(Path),
    Struct(Vec<StructEntry>),
    List(Box<Type>),
    Exclude {
        inner: Box<Type>,
        excluded: Vec<Ident>,
    },
}

#[derive(Clone, Debug)]
pub struct Type {
    pub body: TypeBody,
    pub start: Location,
    pub end: Location,
}

#[derive(Clone, Debug)]
pub struct FnArg {
    pub name: Ident,
    pub type_: Type,
}

#[derive(Clone, Debug)]
pub enum ExprBody {
    SQLQuery(sqlast::Query),
    SQLExpr(sqlast::Expr),
}

#[derive(Clone, Debug)]
pub struct Expr {
    pub body: ExprBody,
    pub start: Location,
    pub end: Location,
}

#[derive(Clone, Debug)]
pub enum ImportList {
    None,
    Star,
    Items(Vec<Path>),
}

#[derive(Clone, Debug)]
pub enum FnBody {
    Expr(Expr),
    Native, // A NativeFn (e.g. load)
    SQL,    // A function we expect to exist in the SQL runtime
}

#[derive(Clone, Debug)]
pub enum StmtBody {
    Noop,
    Expr(Expr),
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
        body: FnBody,
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

#[derive(Clone, Debug)]
pub struct Stmt {
    pub export: bool,
    pub body: StmtBody,
    pub start: Location,
    pub end: Location,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub stmts: Vec<Stmt>,
}
