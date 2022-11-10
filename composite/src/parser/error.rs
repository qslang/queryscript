use snafu::{Backtrace, Snafu};
pub type Result<T> = std::result::Result<T, ParserError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParserError {
    #[snafu(display("Unexpected token {:?}: {}", token, msg))]
    UnexpectedToken {
        msg: String,
        token: sqlparser::tokenizer::Token,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("SQL parser error: {}", source), context(false))]
    SQLParserError {
        source: sqlparser::parser::ParserError,
        backtrace: Option<Backtrace>,
    },
}

#[allow(unused_macros)]
macro_rules! unexpected_token {
    ($token: expr, $base: expr $(, $args:expr)* $(,)?) => {
        crate::parser::error::UnexpectedTokenSnafu {
            msg: format!($base $(, $args)*),
            token: $token.clone(),
        }.fail()
    };
}

#[allow(unused_imports)]
pub(crate) use unexpected_token;
