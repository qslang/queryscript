use crate::ast;
use crate::ast::{Pretty, Range};
use crate::compile::schema::{Decl, MType};
use crate::error::MultiError;
pub use crate::parser::error::ErrorLocation;
use crate::parser::error::{ParserError, PrettyError};
use crate::runtime::error::RuntimeError;
use crate::types::error::TypesystemError;
use colored::*;
use snafu::{Backtrace, Snafu};
use std::io;

pub type Result<T, E = CompileError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CompileError {
    #[snafu(display("Syntax error: {}", source), context(false))]
    SyntaxError {
        #[snafu(backtrace)]
        source: ParserError,
    },

    #[snafu(display("Internal error: {}", what))]
    InternalError {
        what: String,
        backtrace: Option<Backtrace>,
        loc: ErrorLocation,
    },

    #[snafu(display("Internal error: {}", what))]
    ExternalError {
        what: String,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Typesystem error: {}", source))]
    TypesystemError {
        #[snafu(backtrace)]
        source: TypesystemError,
        loc: ErrorLocation,
    },

    #[snafu(display("Runtime error: {}", source))]
    RuntimeError {
        #[snafu(backtrace)]
        source: RuntimeError,
        loc: ErrorLocation,
    },

    #[snafu()]
    FsError {
        source: io::Error,
        backtrace: Option<Backtrace>,
        loc: ErrorLocation,
    },

    #[snafu(display("Unimplemented: {}", what))]
    Unimplemented {
        what: String,
        backtrace: Option<Backtrace>,
        loc: ErrorLocation,
    },

    #[snafu(display("Missing argument: {}", path.pretty()))]
    MissingArg {
        path: ast::Path,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Duplicate entry: {}", path.pretty()))]
    DuplicateEntry {
        path: ast::Path,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("No such entry: {}", path.pretty()))]
    NoSuchEntry {
        path: ast::Path,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display(
        "Wrong kind: expected {} declaration at {}, found {}",
        expected.white().bold(),
        path.pretty(),
        actual.value.kind().white().bold(),
    ))]
    WrongKind {
        path: ast::Path,
        expected: String,
        actual: Decl,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Type mismatch: found {} not {}", rhs.pretty(), lhs.pretty()))]
    WrongType {
        lhs: MType,
        rhs: MType,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Types cannot be coerced: {} and {}", lhs.pretty(), rhs.pretty()))]
    CoercionError {
        lhs: MType,
        rhs: MType,
        backtrace: Option<Backtrace>,
        loc: ErrorLocation,
    },

    #[snafu(display("Error importing {}: {}", path.pretty(), what))]
    ImportError {
        path: ast::Path,
        what: String,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("{}", sources.first().unwrap()))]
    Multiple {
        // This is assumed to be non-empty
        //
        sources: Vec<CompileError>,
    },
}

impl CompileError {
    pub fn unimplemented(loc: ErrorLocation, what: &str) -> CompileError {
        return UnimplementedSnafu { loc, what }.build();
    }

    pub fn missing_arg(path: ast::Path) -> CompileError {
        return MissingArgSnafu { path }.build();
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

    pub fn coercion(loc: ErrorLocation, lhs: &MType, rhs: &MType) -> CompileError {
        return CoercionSnafu {
            loc,
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

    pub fn internal(loc: ErrorLocation, what: &str) -> CompileError {
        return InternalSnafu {
            loc,
            what: what.to_string(),
        }
        .build();
    }

    pub fn external(what: &str) -> CompileError {
        return ExternalSnafu {
            what: what.to_string(),
        }
        .build();
    }
}

pub fn path_location(path: &Vec<ast::Ident>) -> ErrorLocation {
    if path.len() == 0 {
        return ErrorLocation::Unknown;
    }

    let (start_file, start) = match &path[0].loc {
        ErrorLocation::Range(file, range) => (file, Some(&range.start)),
        ErrorLocation::Single(file, _) => (file, None),
        ErrorLocation::File(file) => (file, None),
        ErrorLocation::Unknown => return ErrorLocation::Unknown,
    };

    let (end_file, end) = match &path[path.len() - 1].loc {
        ErrorLocation::Range(file, range) => (file, Some(&range.end)),
        ErrorLocation::Single(file, _) => (file, None),
        ErrorLocation::File(file) => (file, None),
        ErrorLocation::Unknown => return ErrorLocation::Unknown,
    };

    if start_file != end_file {
        return ErrorLocation::Unknown;
    }

    if start.is_none() || end.is_none() {
        return ErrorLocation::File(start_file.clone());
    }

    ErrorLocation::Range(
        start_file.clone(),
        Range {
            start: start.unwrap().clone(),
            end: end.unwrap().clone(),
        },
    )
}

impl PrettyError for CompileError {
    fn location(&self) -> ErrorLocation {
        match self {
            CompileError::SyntaxError { source } => source.location(),
            CompileError::InternalError { loc, .. } => loc.clone(),
            CompileError::ExternalError { .. } => ErrorLocation::File("<qvm>".to_string()),
            CompileError::TypesystemError { loc, .. } => loc.clone(),
            CompileError::RuntimeError { loc, .. } => loc.clone(),
            CompileError::FsError { loc, .. } => loc.clone(),
            CompileError::Unimplemented { loc, .. } => loc.clone(),
            CompileError::MissingArg { path, .. } => path_location(path),
            CompileError::DuplicateEntry { path, .. } => path_location(path),
            CompileError::NoSuchEntry { path, .. } => path_location(path),
            CompileError::WrongKind { path, .. } => path_location(path),
            CompileError::WrongType { lhs, .. } => lhs.location(),
            CompileError::CoercionError { loc, .. } => loc.clone(),
            CompileError::ImportError { path, .. } => path_location(path),
            CompileError::Multiple { sources } => sources.first().unwrap().location(),
        }
    }
}

impl<Guard> From<std::sync::PoisonError<Guard>> for CompileError {
    fn from(e: std::sync::PoisonError<Guard>) -> CompileError {
        CompileError::external(format!("{}", e).as_str())
    }
}

impl From<tokio::task::JoinError> for CompileError {
    fn from(e: tokio::task::JoinError) -> CompileError {
        CompileError::external(format!("{}", e).as_str())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for CompileError {
    fn from(e: tokio::sync::oneshot::error::RecvError) -> CompileError {
        CompileError::external(format!("{}", e).as_str())
    }
}

impl
    From<std::sync::TryLockError<std::sync::RwLockWriteGuard<'_, tokio::sync::watch::Receiver<()>>>>
    for CompileError
{
    fn from(
        e: std::sync::TryLockError<
            std::sync::RwLockWriteGuard<'_, tokio::sync::watch::Receiver<()>>,
        >,
    ) -> CompileError {
        CompileError::external(format!("{}", e).as_str())
    }
}

impl From<tokio::sync::watch::error::RecvError> for CompileError {
    fn from(e: tokio::sync::watch::error::RecvError) -> CompileError {
        CompileError::external(format!("{}", e).as_str())
    }
}

impl From<std::io::Error> for CompileError {
    fn from(e: std::io::Error) -> CompileError {
        CompileError::external(format!("{}", e).as_str())
    }
}

impl MultiError for CompileError {
    fn new_multi_error(errs: Vec<Self>) -> Self {
        CompileError::Multiple { sources: errs }
    }
    fn into_errors(self) -> Vec<Self> {
        match self {
            CompileError::Multiple { sources } => {
                sources.into_iter().flat_map(|e| e.into_errors()).collect()
            }
            CompileError::SyntaxError { source } => {
                source.into_errors().into_iter().map(Into::into).collect()
            }
            _ => vec![self],
        }
    }
}
