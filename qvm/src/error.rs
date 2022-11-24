use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QVMError {
    #[snafu(display("Parser error: {}", source), context(false))]
    ParserError {
        #[snafu(backtrace)]
        source: crate::parser::error::ParserError,
    },

    #[snafu(display("Compiler error: {}", source), context(false))]
    CompileError {
        #[snafu(backtrace)]
        source: crate::compile::error::CompileError,
    },

    #[snafu(display("Runtime error: {}", source), context(false))]
    RuntimeError {
        #[snafu(backtrace)]
        source: crate::runtime::error::RuntimeError,
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
