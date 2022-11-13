use snafu::{Backtrace, Snafu};
use std::num::ParseFloatError;
pub type Result<T> = std::result::Result<T, TypesystemError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum TypesystemError {
    #[snafu(display("{}", what))]
    StringError {
        what: String,
        backtrace: Option<Backtrace>,
    },
}

#[allow(unused_macros)]
macro_rules! ts_fail {
    ($base: expr $(, $args:expr)* $(,)?) => {
        crate::runtime::error::StringSnafu {
            what: format!($base $(, $args)*),
        }.fail()
    };
}

#[allow(unused_imports)]
pub(crate) use ts_fail;
