use colored::*;
pub use sqlparser::ast as sqlast;

pub use sqlparser::{location::Range, tokenizer::Location};

mod ident;
pub use ident::{Ident, ToSqlIdent};

pub trait Pretty {
    fn pretty(&self) -> String;
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SourceLocation {
    Unknown,
    File(String),
    Single(String, Location),
    Range(String, Range),
}

impl SourceLocation {
    pub fn from_file_range(
        file: String,
        range: Option<sqlparser::location::Range>,
    ) -> SourceLocation {
        match range {
            Some(range) => SourceLocation::Range(file, range),
            None => SourceLocation::File(file),
        }
    }

    // XXX can we delete this function?
    pub fn range(&self) -> Option<Range> {
        Some(match self {
            SourceLocation::Unknown | SourceLocation::File(_) => return None,
            SourceLocation::Single(_, l) => Range {
                start: l.clone(),
                end: l.clone(),
            },
            SourceLocation::Range(_, r) => r.clone(),
        })
    }

    pub fn contains(&self, loc: &Location) -> bool {
        match self {
            SourceLocation::Unknown | SourceLocation::File(_) => false,
            SourceLocation::Single(_, l) => l == loc,
            SourceLocation::Range(_, r) => {
                loc.line >= r.start.line
                    && loc.line <= r.end.line
                    && (loc.column >= r.start.column || loc.line > r.start.line)
                    && (loc.column <= r.end.column || loc.line < r.end.line)
            }
        }
    }

    pub fn annotate(&self, code: &str) -> Option<String> {
        let lines = code.lines().collect::<Vec<_>>();
        let line_digits = (lines.len() as f64).log10().floor() as usize + 1;
        let (start_line, start_col, end_col, lines) = match self {
            SourceLocation::Unknown | SourceLocation::File(_) => return None,
            SourceLocation::Single(_, l) => (
                l.line,
                l.column,
                l.column,
                &lines[(l.line as usize) - 1..l.line as usize],
            ),
            SourceLocation::Range(_, r) => (
                r.start.line,
                r.start.column,
                r.end.column,
                &lines
                    [r.start.line as usize - 1..(std::cmp::min(r.end.line as usize, lines.len()))],
            ),
        };

        let num_lines = lines.len();
        if num_lines == 0 {
            return None;
        }

        let lines = lines
            .iter()
            .enumerate()
            .flat_map(|(i, l)| {
                let line = start_line + i as u64;
                let start = if i == 0 { start_col } else { 1 };
                let end = if i == num_lines - 1 {
                    end_col
                } else {
                    (l.len()) as u64
                };
                let annotation = (1..start + line_digits as u64 + 4)
                    .map(|_| " ")
                    .collect::<String>()
                    + (start..end + 1).map(|_| "^").collect::<String>().as_str();
                vec![
                    format!("  {: >line_digits$}  ", line)
                        .bright_blue()
                        .to_string()
                        + l.to_string().as_str(),
                    annotation.bright_red().to_string(),
                ]
            })
            .collect::<Vec<_>>();

        Some(lines.join("\n"))
    }

    pub fn file(&self) -> Option<String> {
        match self {
            SourceLocation::Unknown => None,
            SourceLocation::File(file) => Some(file.clone()),
            SourceLocation::Single(file, _) => Some(file.clone()),
            SourceLocation::Range(file, _) => Some(file.clone()),
        }
    }
}

impl Pretty for SourceLocation {
    fn pretty(&self) -> String {
        match self {
            SourceLocation::Unknown => "<qs>".white().bold().to_string(),
            SourceLocation::File(f) => f.clone().white().bold().to_string(),
            SourceLocation::Single(f, l) => format!("{}:{}:{}", f, l.line, l.column)
                .white()
                .bold()
                .to_string(),
            SourceLocation::Range(f, r) => format!(
                "{}:{}:{}-{}:{}",
                f, r.start.line, r.start.column, r.end.line, r.end.column
            )
            .white()
            .bold()
            .to_string(),
        }
    }
}

pub type Located<T> = sqlast::Located<T, SourceLocation>;

pub type Path = Vec<Located<Ident>>;

impl Pretty for Path {
    fn pretty(&self) -> String {
        sqlast::ObjectName(self.iter().map(|i| i.to_sqlident()).collect())
            .to_string()
            .white()
            .bold()
            .to_string()
    }
}

pub trait ToIdents {
    fn to_idents(&self) -> Vec<Ident>;
}

impl ToIdents for Path {
    fn to_idents(&self) -> Vec<Ident> {
        self.iter().map(|i| i.get().clone()).collect()
    }
}

pub trait ToPath {
    fn to_path(&self, file: String) -> Path;
}

impl ToPath for Vec<sqlast::Located<sqlast::Ident>> {
    fn to_path(&self, file: String) -> Path {
        self.iter()
            .map(|p| {
                Ident::from_sqlident(
                    SourceLocation::from_file_range(file.clone(), p.location().clone()),
                    p.get().clone(),
                )
            })
            .collect()
    }
}

impl ToPath for sqlast::ObjectName {
    fn to_path(&self, file: String) -> Path {
        (&self.0).to_path(file)
    }
}

#[derive(Clone, Debug)]
pub struct NameAndType {
    pub name: Located<Ident>,
    pub def: Type,
}

#[derive(Clone, Debug)]
pub struct NameAndExpr {
    pub name: Located<Ident>,
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
        excluded: Vec<Located<Ident>>,
    },
    Generic(Path, Vec<Type>),
}

#[derive(Clone, Debug)]
pub struct Type {
    pub body: TypeBody,
    pub start: Location,
    pub end: Location,
}

#[derive(Clone, Debug)]
pub struct FnArg {
    pub name: Located<Ident>,
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
    pub viz: Option<Located<sqlast::Expr>>,
    pub is_unsafe: bool,
}

impl Expr {
    pub fn unlocated(body: ExprBody) -> Self {
        let empty_loc = Location { line: 0, column: 0 };
        Expr {
            body,
            start: empty_loc.clone(),
            end: empty_loc,
            viz: None,
            is_unsafe: false,
        }
    }
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
pub struct MaterializeArgs {
    pub db: Option<Expr>,
}

#[derive(Clone, Debug)]
pub struct FnDef {
    pub name: Located<Ident>,
    pub generics: Vec<Located<Ident>>,
    pub args: Vec<FnArg>,
    pub variadic_arg: Option<FnArg>,
    pub ret: Option<Type>,
    pub body: FnBody,
}

#[derive(Clone, Debug)]
pub enum StmtBody {
    Noop,
    Unparsed,
    Expr(Expr),
    Import {
        path: Path,
        list: ImportList,
        args: Option<Vec<NameAndExpr>>,
    },
    TypeDef(NameAndType),
    FnDef(FnDef),
    Let {
        name: Located<Ident>,
        type_: Option<Type>,
        body: Expr,
        materialize: Option<MaterializeArgs>,
    },
    Extern {
        name: Located<Ident>,
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

impl Schema {
    pub fn find_stmt(&self, loc: Location) -> Option<Stmt> {
        let mut stmt = None;
        for s in &self.stmts {
            if &s.start > &loc {
                break;
            }
            if &loc <= &s.end {
                stmt = Some(s.clone());
            }
        }
        return stmt;
    }
}
