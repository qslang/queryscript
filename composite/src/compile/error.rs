use crate::ast;
use crate::parser::error::ParserError;
use crate::runtime::error::RuntimeError;
use crate::schema::{Decl, Type};
use snafu::{Backtrace, Snafu};
pub type Result<T> = std::result::Result<T, CompileError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CompileError {
    #[snafu(display("Parser error: {}", source), context(false))]
    SyntaxError {
        source: ParserError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Parser error: {}", source), context(false))]
    RuntimeError {
        source: RuntimeError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Unimplemented: {}", what))]
    Unimplemented {
        what: String,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Duplicate entry: {:?}", path))]
    DuplicateEntry {
        path: ast::Path,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("No such entry: {:?}", path))]
    NoSuchEntry {
        path: ast::Path,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display(
        "Wrong kind: declaration at {:?} is {:?} not {}",
        path,
        actual,
        expected
    ))]
    WrongKind {
        path: ast::Path,
        expected: String,
        actual: Decl,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Type mismatch: found {:?} not {:?}", rhs, lhs))]
    WrongType {
        lhs: Type,
        rhs: Type,
        backtrace: Option<Backtrace>,
    },
}

impl CompileError {
    pub fn unimplemented(what: &str) -> CompileError {
        return UnimplementedSnafu { what }.build();
    }

    pub fn no_such_entry(path: ast::Path) -> CompileError {
        return NoSuchEntrySnafu { path }.build();
    }

    pub fn duplicate_entry(path: ast::Path) -> CompileError {
        return DuplicateEntrySnafu { path }.build();
    }

    pub fn wrong_kind(path: ast::Path, expected: &str, actual: &Decl) -> CompileError {
        return WrongKindSnafu {
            path,
            expected,
            actual: actual.clone(),
        }
        .build();
    }

    pub fn wrong_type(lhs: &Type, rhs: &Type) -> CompileError {
        return WrongTypeSnafu {
            lhs: lhs.clone(),
            rhs: rhs.clone(),
        }
        .build();
    }
}
