use snafu::{Backtrace, Snafu};
pub type Result<T> = std::result::Result<T, ParserError>;
use crate::ast::Location;

#[derive(Clone, Debug)]
pub enum ErrorLocation {
    File(String),
    Single(String, Location),
    Range(String, Location, Location),
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParserError {
    #[snafu(display("Unexpected token {:?}: {}", token, msg))]
    UnexpectedToken {
        msg: String,
        token: sqlparser::tokenizer::TokenWithLocation,
        backtrace: Option<Backtrace>,
        file: String,
    },

    #[snafu(display("Tokenizer error: {}", source))]
    TokenizerError {
        source: sqlparser::tokenizer::TokenizerError,
        backtrace: Option<Backtrace>,
        file: String,
    },

    #[snafu(display("SQL parser error: {}", source))]
    SQLParserError {
        source: sqlparser::parser::ParserError,
        backtrace: Option<Backtrace>,
        loc: ErrorLocation,
    },
}

#[allow(unused_macros)]
macro_rules! unexpected_token {
    ($file: expr, $token: expr, $base: expr $(, $args:expr)* $(,)?) => {
        crate::parser::error::UnexpectedTokenSnafu {
            file: $file,
            msg: format!($base $(, $args)*),
            token: $token.clone(),
        }.fail()
    };
}

#[allow(unused_imports)]
pub(crate) use unexpected_token;
