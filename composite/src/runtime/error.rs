use snafu::{Backtrace, Snafu};
use std::num::ParseFloatError;
pub type Result<T> = std::result::Result<T, RuntimeError>;

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

    #[snafu(context(false))]
    ParseFloatError {
        source: ParseFloatError,
        backtrace: Option<Backtrace>,
    },
}

impl RuntimeError {
    pub fn new(what: &str) -> RuntimeError {
        return StringSnafu { what }.build();
    }

    pub fn unimplemented(what: &str) -> RuntimeError {
        return UnimplementedSnafu { what }.build();
    }
}
