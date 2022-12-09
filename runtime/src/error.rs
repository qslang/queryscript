use object_store;
use snafu::{Backtrace, GenerateImplicitData, Snafu};
use std::num::ParseFloatError;
use std::sync::Arc;

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
    TypesystemError {
        #[snafu(backtrace)]
        source: crate::types::error::TypesystemError,
    },

    #[snafu(context(false))]
    DataFusionError {
        source: Arc<datafusion::common::DataFusionError>,
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
}

impl RuntimeError {
    pub fn new(what: &str) -> RuntimeError {
        return StringSnafu { what }.build();
    }

    pub fn unimplemented(what: &str) -> RuntimeError {
        return UnimplementedSnafu { what }.build();
    }
}

impl<Guard> From<std::sync::PoisonError<Guard>> for RuntimeError {
    fn from(e: std::sync::PoisonError<Guard>) -> RuntimeError {
        RuntimeError::new(format!("{}", e).as_str())
    }
}

impl From<datafusion::common::DataFusionError> for RuntimeError {
    fn from(e: datafusion::common::DataFusionError) -> RuntimeError {
        RuntimeError::DataFusionError {
            source: Arc::new(e),
            backtrace: Option::<Backtrace>::generate(),
        }
    }
}

#[macro_export]
macro_rules! fail {
    ($base: expr $(, $args:expr)* $(,)?) => {
        crate::runtime::error::StringSnafu {
            what: format!($base $(, $args)*),
        }.fail()
    };
}

pub use fail;

#[macro_export]
macro_rules! rt_unimplemented {
    ($base: expr $(, $args:expr)* $(,)?) => {
        crate::runtime::error::UnimplementedSnafu {
            what: format!($base $(, $args)*),
        }.fail()
    };
}

pub use rt_unimplemented;
