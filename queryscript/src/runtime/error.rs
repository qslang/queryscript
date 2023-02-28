use crate::error::MultiError;
use object_store;
use snafu::{Backtrace, Snafu};
use std::num::ParseFloatError;

pub type Result<T, E = RuntimeError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum RuntimeError {
    #[snafu(display("{}", what))]
    StringError {
        what: String,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Unimplemented: {}", what))]
    Unimplemented {
        what: String,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("SQL Engine returned a record batch with an invalid schema: expected {:?}, got {:?}. This can happen if the type is declared incorrectly or if there's a bug in the engine.", expected_type, actual_type))]
    TypeMismatch {
        expected_type: crate::types::Type,
        actual_type: crate::types::Type,
        backtrace: Option<Backtrace>,
    },

    #[snafu(context(false))]
    TypesystemError {
        #[snafu(backtrace)]
        source: crate::types::error::TypesystemError,
    },

    #[snafu(context(false))]
    ArrowError {
        source: arrow::error::ArrowError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(context(false))]
    ParquetError {
        source: parquet::errors::ParquetError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(context(false))]
    DuckDBError {
        source: duckdb::Error,
        backtrace: Option<Backtrace>,
    },

    #[snafu(context(false))]
    IOError {
        source: std::io::Error,
        backtrace: Option<Backtrace>,
    },

    #[snafu(context(false))]
    ParseFloatError {
        source: ParseFloatError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(context(false))]
    PathError {
        source: object_store::path::Error,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Compile error: {}", source))]
    CompileError {
        #[snafu(backtrace)]
        source: Box<crate::compile::CompileError>,
    },

    #[snafu(display("{}", sources.first().unwrap()))]
    Multiple {
        // This is assumed to be non-empty
        //
        sources: Vec<RuntimeError>,
    },
}

impl RuntimeError {
    pub fn new(what: &str) -> RuntimeError {
        return StringSnafu { what }.build();
    }

    pub fn unimplemented(what: &str) -> RuntimeError {
        return UnimplementedSnafu { what }.build();
    }

    pub fn type_mismatch(
        expected_type: crate::types::Type,
        actual_type: crate::types::Type,
    ) -> RuntimeError {
        return TypeMismatchSnafu {
            expected_type,
            actual_type,
        }
        .build();
    }
}

impl Into<arrow::error::ArrowError> for RuntimeError {
    fn into(self) -> arrow::error::ArrowError {
        match self {
            RuntimeError::ArrowError { source, .. } => source,
            _ => arrow::error::ArrowError::ExternalError(Box::new(self)),
        }
    }
}

impl<Guard> From<std::sync::PoisonError<Guard>> for RuntimeError {
    fn from(e: std::sync::PoisonError<Guard>) -> RuntimeError {
        RuntimeError::new(format!("{}", e).as_str())
    }
}

impl From<crate::compile::CompileError> for RuntimeError {
    fn from(e: crate::compile::CompileError) -> RuntimeError {
        RuntimeError::CompileError {
            source: Box::new(e),
        }
    }
}

impl MultiError for RuntimeError {
    fn new_multi_error(errs: Vec<Self>) -> Self {
        RuntimeError::Multiple { sources: errs }
    }
    fn into_errors(self) -> Vec<Self> {
        match self {
            RuntimeError::Multiple { sources } => {
                sources.into_iter().flat_map(|e| e.into_errors()).collect()
            }
            _ => vec![self],
        }
    }
}

#[allow(unused_macros)]
macro_rules! fail {
    ($base: expr $(, $args:expr)* $(,)?) => {
        crate::runtime::error::StringSnafu {
            what: format!($base $(, $args)*),
        }.fail()
    };
}

pub(crate) use fail;

#[allow(unused_macros)]
macro_rules! rt_unimplemented {
    ($base: expr $(, $args:expr)* $(,)?) => {
        crate::runtime::error::UnimplementedSnafu {
            what: format!($base $(, $args)*),
        }.fail()
    };
}

pub(crate) use rt_unimplemented;
