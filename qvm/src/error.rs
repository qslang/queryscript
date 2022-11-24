use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QVMError {
    #[snafu(display("Parser error: {}", source), context(false))]
    ParserError {
        source: crate::parser::error::ParserError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Compiler error: {}", source), context(false))]
    CompileError {
        source: crate::compile::error::CompileError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Runtime error: {}", source), context(false))]
    RuntimeError {
        source: crate::runtime::error::RuntimeError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
        backtrace: Option<Backtrace>,
    },
}

impl<Guard> From<std::sync::PoisonError<Guard>> for QVMError {
    fn from(e: std::sync::PoisonError<Guard>) -> QVMError {
        let e1: crate::compile::error::CompileError = e.into();
        e1.into()
    }
}
