use crate::parser::error::{ErrorLocation, FormattedError, PrettyError};
use snafu::{Backtrace, ErrorCompat, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QVMError {
    #[snafu(display("Syntax error: {}", source), context(false))]
    ParserError {
        #[snafu(backtrace)]
        source: crate::parser::error::ParserError,
    },

    #[snafu(display("Compiler error: {}", source))]
    CompileError {
        #[snafu(backtrace)]
        source: crate::compile::error::CompileError,
    },

    #[snafu(display("Runtime error: {}", source))]
    RuntimeError {
        #[snafu(backtrace)]
        source: crate::runtime::error::RuntimeError,
        file: String,
    },

    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("{}", sources.first().unwrap()))]
    Multiple {
        // This is assumed to be non-empty
        //
        sources: Vec<QVMError>,
    },
}

impl QVMError {
    pub fn format_backtrace(&self) -> Vec<FormattedError> {
        match self {
            QVMError::Multiple { sources } => {
                let mut ret = Vec::new();
                for source in sources {
                    ret.extend(source.format_backtrace());
                }
                ret
            }
            _ => {
                let mut ret = self.format_without_backtrace()[0].clone();
                if let Some(bt) = ErrorCompat::backtrace(&self) {
                    ret.text += format!("\n{:?}", bt).as_str();
                }
                vec![ret]
            }
        }
    }

    pub fn format_without_backtrace(&self) -> Vec<FormattedError> {
        match self {
            QVMError::Multiple { sources } => {
                let mut ret = Vec::new();
                for source in sources {
                    ret.extend(source.format_without_backtrace());
                }
                ret
            }
            _ => {
                let location = self.location();
                let text = self.to_string();
                vec![FormattedError { location, text }]
            }
        }
    }
}

impl PrettyError for QVMError {
    fn location(&self) -> ErrorLocation {
        match self {
            QVMError::ParserError { source } => source.location(),
            QVMError::CompileError { source } => source.location(),
            QVMError::RuntimeError { file, .. } => ErrorLocation::File(file.clone()),
            _ => ErrorLocation::Unknown,
        }
    }
}

impl<Guard> From<std::sync::PoisonError<Guard>> for QVMError {
    fn from(e: std::sync::PoisonError<Guard>) -> QVMError {
        snafu::FromString::without_source(e.to_string())
    }
}

impl From<crate::compile::error::CompileError> for QVMError {
    fn from(e: crate::compile::error::CompileError) -> QVMError {
        match e {
            crate::compile::error::CompileError::Multiple { sources } => {
                let sources = sources.into_iter().map(|e| e.into()).collect();
                QVMError::Multiple { sources }
            }
            _ => QVMError::CompileError { source: e },
        }
    }
}
