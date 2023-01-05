use colored::*;
pub use sqlparser::ast as sqlast;
use std::borrow::Cow;

pub use sqlparser::{location::Range, tokenizer::Location};

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
                &lines[r.start.line as usize - 1..r.end.line as usize],
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
            SourceLocation::Unknown => "<qvm>".white().bold().to_string(),
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

// Look into https://crates.io/crates/unicase
#[derive(Clone)]
pub struct Ident(String);

impl AsRef<String> for Ident {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for Ident {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Into<String> for &Ident {
    fn into(self) -> String {
        self.0.clone()
    }
}

impl AsRef<std::ffi::OsStr> for Ident {
    fn as_ref(&self) -> &std::ffi::OsStr {
        self.0.as_ref()
    }
}

impl Into<sqlast::Ident> for &Ident {
    fn into(self) -> sqlast::Ident {
        sqlast::Ident::with_quote_unlocated('\"', self.0.clone())
    }
}

impl From<String> for Ident {
    fn from(s: String) -> Ident {
        Ident(s)
    }
}

impl From<&str> for Ident {
    fn from(s: &str) -> Ident {
        Ident(s.to_string())
    }
}

fn quoted_string_to_ident(value: Cow<String>, quote_style: &Option<char>) -> Ident {
    Ident(match quote_style {
        Some(_) => value.into_owned(),
        None => value.into_owned(), // .to_lowercase(),
    })
}

impl From<crate::parser::Word> for Ident {
    fn from(w: crate::parser::Word) -> Ident {
        quoted_string_to_ident(Cow::Owned(w.value), &w.quote_style)
    }
}

impl From<sqlast::Ident> for Ident {
    fn from(w: sqlast::Ident) -> Ident {
        quoted_string_to_ident(Cow::Owned(w.value), &w.quote_style)
    }
}

impl From<&sqlast::Ident> for Ident {
    fn from(w: &sqlast::Ident) -> Ident {
        quoted_string_to_ident(Cow::Borrowed(&w.value), &w.quote_style)
    }
}

impl std::fmt::Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

// NOTE: This is where we can implement case-insensitivity
impl PartialOrd for Ident {
    fn partial_cmp(&self, other: &Ident) -> Option<std::cmp::Ordering> {
        self.0.to_lowercase().partial_cmp(&other.0.to_lowercase())
    }
}

impl Ord for Ident {
    fn cmp(&self, other: &Ident) -> std::cmp::Ordering {
        self.0.to_lowercase().cmp(&other.0.to_lowercase())
    }
}

impl PartialEq for Ident {
    fn eq(&self, other: &Ident) -> bool {
        self.0.to_lowercase() == other.0.to_lowercase()
    }
}
impl Eq for Ident {}

impl std::hash::Hash for Ident {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_lowercase().hash(state)
    }
}

impl Ident {
    pub fn with_location<T: Into<Ident>>(loc: SourceLocation, value: T) -> Located<Ident> {
        Located::new(value.into(), loc)
    }

    pub fn without_location<T: Into<Ident>>(value: T) -> Located<Ident> {
        Located::new(value.into(), SourceLocation::Unknown)
    }

    pub fn from_sqlident(loc: SourceLocation, ident: sqlast::Ident) -> Located<Ident> {
        Ident::with_location(loc, Into::<Ident>::into(ident))
    }

    pub fn replace_location(&self, loc: SourceLocation) -> Located<Ident> {
        Ident::with_location(loc, self.clone())
    }

    pub fn as_str(&self) -> &str {
        self.as_ref()
    }

    // XXX Remove
    pub fn to_lowercase(&self) -> Ident {
        Ident(self.0.to_lowercase())
    }
}

pub trait ToSqlIdent {
    fn to_sqlident(&self) -> sqlast::Located<sqlast::Ident>;
}

impl ToSqlIdent for Located<Ident> {
    fn to_sqlident(&self) -> sqlast::Located<sqlast::Ident> {
        sqlast::Located::new(
            self.get().into(),
            self.location()
                .range()
                .map(|Range { start, end }| sqlast::Range { start, end }),
        )
    }
}

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
    pub is_unsafe: bool,
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
    Unparsed,
    Expr(Expr),
    Import {
        path: Path,
        list: ImportList,
        args: Option<Vec<NameAndExpr>>,
    },
    TypeDef(NameAndType),
    FnDef {
        name: Located<Ident>,
        generics: Vec<Located<Ident>>,
        args: Vec<FnArg>,
        ret: Option<Type>,
        body: FnBody,
    },
    Let {
        name: Located<Ident>,
        type_: Option<Type>,
        body: Expr,
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
