use snafu::{Backtrace, Snafu};
use std::fmt;
pub type Result<T> = std::result::Result<T, ParserError>;
use crate::ast::Location;
use colored::*;

pub trait PrettyError: ToString {
    fn location(&self) -> ErrorLocation;

    fn pretty(&self) -> String {
        format!(
            "{}{} {} {}",
            self.location().pretty(),
            ":".white().bold(),
            "error:".bright_red(),
            self.to_string()
        )
    }

    fn pretty_with_code(&self, code: &str) -> String {
        format!("{}\n\n{}", self.pretty(), self.location().annotate(code))
    }
}

#[derive(Clone, Debug)]
pub struct FormattedError {
    pub location: ErrorLocation,
    pub text: String,
}

impl fmt::Display for FormattedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.text.as_str())
    }
}

impl PrettyError for FormattedError {
    fn location(&self) -> ErrorLocation {
        self.location.clone()
    }
}

#[derive(Clone, Debug)]
pub enum ErrorLocation {
    File(String),
    Single(String, Location),
    Range(String, Location, Location),
}

impl ErrorLocation {
    fn annotate(&self, code: &str) -> String {
        let lines = code.lines().collect::<Vec<_>>();
        let (start_col, end_col, lines) = match self {
            ErrorLocation::File(_) => (0, 0, &lines[0..0]),
            ErrorLocation::Single(_, l) => (
                l.column,
                l.column,
                &lines[(l.line as usize) - 1..l.line as usize],
            ),
            ErrorLocation::Range(_, s, e) => (
                s.column,
                e.column,
                &lines[s.line as usize - 1..e.line as usize],
            ),
        };
        let num_lines = lines.len();
        let lines = lines
            .iter()
            .enumerate()
            .flat_map(|(i, l)| {
                let start = if i == 0 { start_col } else { 0 };
                let end = if i == num_lines - 1 {
                    end_col
                } else {
                    (l.len() - 1) as u64
                };
                let annotation = (0..start + 1).map(|_| " ").collect::<String>()
                    + (start..end + 1).map(|_| "^").collect::<String>().as_str();
                vec![
                    "  ".to_string() + l.to_string().as_str(),
                    annotation.bright_red().to_string(),
                ]
            })
            .collect::<Vec<_>>();
        lines.join("\n")
    }

    fn pretty(&self) -> String {
        match self {
            ErrorLocation::File(f) => f.clone().white().bold().to_string(),
            ErrorLocation::Single(f, l) => format!("{}:{}:{}", f, l.line, l.column)
                .white()
                .bold()
                .to_string(),
            ErrorLocation::Range(f, s, e) => {
                format!("{}:{}:{}-{}:{}", f, s.line, s.column, e.line, e.column)
                    .white()
                    .bold()
                    .to_string()
            }
        }
    }
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

impl PrettyError for ParserError {
    fn location(&self) -> ErrorLocation {
        match self {
            ParserError::UnexpectedToken { file, token, .. } => {
                ErrorLocation::Single(file.clone(), token.location.clone())
            }
            ParserError::TokenizerError { file, source, .. } => ErrorLocation::Single(
                file.clone(),
                Location {
                    line: source.line,
                    column: source.col,
                },
            ),
            ParserError::SQLParserError { loc, .. } => loc.clone(),
        }
    }
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
