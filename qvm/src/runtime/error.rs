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
    DataFusionError {
        source: datafusion::common::DataFusionError,
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
}

impl RuntimeError {
    pub fn new(what: &str) -> RuntimeError {
        return StringSnafu { what }.build();
    }

    pub fn unimplemented(what: &str) -> RuntimeError {
        return UnimplementedSnafu { what }.build();
    }
}

#[allow(unused_macros)]
macro_rules! fail {
    ($base: expr $(, $args:expr)* $(,)?) => {
        StringSnafu {
            what: format!($base $(, $args)*),
        }.fail()
    };
}

#[allow(unused_imports)]
pub(crate) use fail;

#[allow(unused_macros)]
macro_rules! rt_unimplemented {
    ($base: expr $(, $args:expr)* $(,)?) => {
        UnimplementedSnafu {
            what: format!($base $(, $args)*),
        }.fail()
    };
}

#[allow(unused_imports)]
pub(crate) use rt_unimplemented;
