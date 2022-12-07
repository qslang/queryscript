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
        file: String,
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
}

impl QVMError {
    pub fn format_backtrace(&self) -> FormattedError {
        let mut ret = self.format_without_backtrace();
        if let Some(bt) = ErrorCompat::backtrace(&self) {
            ret.text += format!("\n{:?}", bt).as_str();
        }
        ret
    }

    pub fn format_without_backtrace(&self) -> FormattedError {
        let location = self.location();
        let text = self.to_string();
        FormattedError { location, text }
    }
}

impl PrettyError for QVMError {
    fn location(&self) -> ErrorLocation {
        match self {
            QVMError::ParserError { source } => source.location(),
            QVMError::CompileError { file, .. } => ErrorLocation::File(file.clone()),
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
