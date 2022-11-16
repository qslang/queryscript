use crate::ast;
use crate::compile::schema::{Decl, MType};
use crate::parser::error::ParserError;
use crate::runtime::error::RuntimeError;
use crate::types::error::TypesystemError;
use snafu::{Backtrace, Snafu};
use std::io;
pub type Result<T> = std::result::Result<T, CompileError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CompileError {
    #[snafu(display("Parser error: {}", source), context(false))]
    SyntaxError {
        source: ParserError,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal error: {}", what))]
    InternalError { what: String, backtrace: Backtrace },

    #[snafu(display("Typesystem error: {}", source), context(false))]
    TypesystemError {
        source: TypesystemError,
        backtrace: Backtrace,
    },

    #[snafu(display("Parser error: {}", source), context(false))]
    RuntimeError {
        source: RuntimeError,
        backtrace: Backtrace,
    },

    #[snafu(context(false))]
    FsError {
        source: io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Unimplemented: {}", what))]
    Unimplemented { what: String, backtrace: Backtrace },

    #[snafu(display("Duplicate entry: {:?}", path))]
    DuplicateEntry {
        path: ast::Path,
        backtrace: Backtrace,
    },

    #[snafu(display("No such entry: {:?}", path))]
    NoSuchEntry {
        path: ast::Path,
        backtrace: Backtrace,
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
        backtrace: Backtrace,
    },

    #[snafu(display("Type mismatch: found {:?} not {:?}", rhs, lhs))]
    WrongType {
        lhs: MType,
        rhs: MType,
        backtrace: Backtrace,
    },

    #[snafu(display("Error importing {:?}: {}", path, what))]
    ImportError {
        path: ast::Path,
        what: String,
        backtrace: Backtrace,
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

    pub fn wrong_type(lhs: &MType, rhs: &MType) -> CompileError {
        return WrongTypeSnafu {
            lhs: lhs.clone(),
            rhs: rhs.clone(),
        }
        .build();
    }

    pub fn import_error(path: ast::Path, what: &str) -> CompileError {
        return ImportSnafu {
            path,
            what: what.to_string(),
        }
        .build();
    }

    pub fn internal(what: &str) -> CompileError {
        return InternalSnafu {
            what: what.to_string(),
        }
        .build();
    }
}
