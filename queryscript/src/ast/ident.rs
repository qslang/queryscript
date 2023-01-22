pub use sqlparser::ast as sqlast;
use std::borrow::Cow;
use unicase::UniCase;

use super::{Located, Range, SourceLocation};

// Look into https://crates.io/crates/unicase
#[derive(Clone)]
pub struct Ident(UniCase<String>);

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
        self.0.as_ref().to_string()
    }
}

impl AsRef<std::ffi::OsStr> for Ident {
    fn as_ref(&self) -> &std::ffi::OsStr {
        let s: &str = self.0.as_ref();
        s.as_ref()
    }
}

impl Into<sqlast::Ident> for &Ident {
    fn into(self) -> sqlast::Ident {
        sqlast::Ident::with_quote_unlocated('\"', self.0.clone())
    }
}

impl From<String> for Ident {
    fn from(s: String) -> Ident {
        Ident(UniCase::new(s))
    }
}

impl From<&str> for Ident {
    fn from(s: &str) -> Ident {
        s.to_string().into()
    }
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

// The current semantics are that, independent of the quoting style, we preserve
// the case of the identifier and do case-insensitive matching. These semantics are
// equivalent to DuckDB's. On the other hand, Postgres is case sensitive but will automatically
// lowercase identifiers if they are not quoted. Snowflake automatically uppercases them.
// We may want to add a flag to the compiler that lets you control this behavior, but for now
// we stick with DuckDB's which is most like a traditional programming language.
fn quoted_string_to_ident(value: Cow<String>, _quote_style: &Option<char>) -> Ident {
    value.into_owned().into()
}

impl PartialOrd for Ident {
    fn partial_cmp(&self, other: &Ident) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Ident {
    fn cmp(&self, other: &Ident) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialEq for Ident {
    fn eq(&self, other: &Ident) -> bool {
        self.0 == other.0
    }
}
impl Eq for Ident {}

impl std::hash::Hash for Ident {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

// In most cases, we want to construct identifiers that are Located<>, hence the constructors
// below. There are some cases where we want to create them directly from strings or preserve
// just their name (e.g. a map of declarations), which you can do using the .into() method on
// a String or &str.
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

    pub fn from_located_sqlident(
        file: Option<String>,
        ident: sqlast::Located<sqlast::Ident>,
    ) -> Located<Ident> {
        let loc = match file {
            Some(f) => SourceLocation::from_file_range(f, ident.location().clone()),
            None => SourceLocation::Unknown,
        };
        Ident::with_location(loc, Into::<Ident>::into(ident.get()))
    }

    pub fn replace_location(&self, loc: SourceLocation) -> Located<Ident> {
        Ident::with_location(loc, self.clone())
    }

    pub fn as_str(&self) -> &str {
        self.as_ref()
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
