use snafu::{Backtrace, Snafu};
pub type Result<T> = std::result::Result<T, ParserError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParserError {
    #[snafu(display("Unexpected token {:?}: {}", token, msg))]
    UnexpectedToken {
        msg: String,
        token: sqlparser::tokenizer::TokenWithLocation,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Tokenizer error: {}", source), context(false))]
    TokenizerError {
        source: sqlparser::tokenizer::TokenizerError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("SQL parser error: {}", source), context(false))]
    SQLParserError {
        source: sqlparser::parser::ParserError,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("Incomplete parse string: {}", original))]
    Incomplete {
        original: Box<ParserError>,
        backtrace: Option<Backtrace>,
    },
}

pub fn wrap_sql_eof<T>(
    result: std::result::Result<T, sqlparser::parser::ParserError>,
) -> Result<T> {
    let wrap = if let Err(sqlparser::parser::ParserError::ParserError(s)) = &result {
        s.ends_with("found: EOF")
    } else if let Err(sqlparser::parser::ParserError::TokenizerError(s)) = &result {
        s.contains("before EOF")
    } else {
        false
    };

    if wrap {
        let err: ParserError = result.err().unwrap().into();
        IncompleteSnafu { original: err }.fail()
    } else {
        Ok(result?)
    }
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
