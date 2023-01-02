use crate::parser::error::{ErrorLocation, FormattedError, PrettyError};
use snafu::{Backtrace, ErrorCompat, Snafu};
use std::fmt;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QVMError {
    #[snafu(display("Syntax error: {}", source), context(false))]
    ParserError {
        #[snafu(backtrace)]
        source: crate::parser::error::ParserError,
    },

    #[snafu(display("Compiler error: {}", source))]
    CompileError {
        #[snafu(backtrace)]
        source: crate::compile::error::CompileError,
    },

    #[snafu(display("Runtime error: {}", source))]
    RuntimeError {
        #[snafu(backtrace)]
        source: crate::runtime::error::RuntimeError,
        file: String,
    },

    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
        backtrace: Option<Backtrace>,
    },

    #[snafu(display("{}", sources.first().unwrap()))]
    Multiple {
        // This is assumed to be non-empty
        //
        sources: Vec<QVMError>,
    },
}

impl QVMError {
    pub fn format_backtrace(&self) -> Vec<FormattedError> {
        match self {
            QVMError::Multiple { sources } => {
                let mut ret = Vec::new();
                for source in sources {
                    ret.extend(source.format_backtrace());
                }
                ret
            }
            _ => {
                let mut ret = self.format_without_backtrace()[0].clone();
                if let Some(bt) = ErrorCompat::backtrace(&self) {
                    ret.text += format!("\n{:?}", bt).as_str();
                }
                vec![ret]
            }
        }
    }

    pub fn format_without_backtrace(&self) -> Vec<FormattedError> {
        match self {
            QVMError::Multiple { sources } => {
                let mut ret = Vec::new();
                for source in sources {
                    ret.extend(source.format_without_backtrace());
                }
                ret
            }
            _ => {
                let location = self.location();
                let text = self.to_string();
                vec![FormattedError { location, text }]
            }
        }
    }
}

impl PrettyError for QVMError {
    fn location(&self) -> ErrorLocation {
        match self {
            QVMError::ParserError { source } => source.location(),
            QVMError::CompileError { source } => source.location(),
            QVMError::RuntimeError { file, .. } => ErrorLocation::File(file.clone()),
            _ => ErrorLocation::Unknown,
        }
    }
}

impl<Guard> From<std::sync::PoisonError<Guard>> for QVMError {
    fn from(e: std::sync::PoisonError<Guard>) -> QVMError {
        snafu::FromString::without_source(e.to_string())
    }
}

impl From<crate::compile::error::CompileError> for QVMError {
    fn from(e: crate::compile::error::CompileError) -> QVMError {
        match e {
            crate::compile::error::CompileError::Multiple { sources } => {
                let sources = sources.into_iter().map(|e| e.into()).collect();
                QVMError::Multiple { sources }
            }
            _ => QVMError::CompileError { source: e },
        }
    }
}

pub trait MultiError: fmt::Debug + Sized {
    fn new_multi_error(errs: Vec<Self>) -> Self;
    fn into_errors(self) -> Vec<Self>;
}

#[derive(Debug)]
pub struct MultiResult<V, E: MultiError> {
    pub result: V,
    pub errors: Vec<(Option<usize>, E)>,
}

impl<V, E: MultiError> MultiResult<V, E> {
    pub fn new(result: V) -> MultiResult<V, E> {
        MultiResult {
            result,
            errors: Vec::new(),
        }
    }

    pub fn map<U, F>(self, f: F) -> MultiResult<U, E>
    where
        F: FnOnce(V) -> U,
    {
        MultiResult {
            result: f(self.result),
            errors: self.errors,
        }
    }

    pub fn set_result(&mut self, value: V) {
        self.result = value;
    }

    pub fn add_error(&mut self, idx: Option<usize>, error: E) {
        for err in error.into_errors() {
            self.errors.push((idx, err))
        }
    }

    pub fn absorb<U, E2: MultiError + Into<E>>(&mut self, other: MultiResult<U, E2>) -> U {
        self.errors
            .extend(other.errors.into_iter().map(|(idx, err)| (idx, err.into())));
        other.result
    }

    pub fn replace(&mut self, other: MultiResult<V, E>) {
        self.result = other.result;
        self.errors.extend(other.errors);
    }

    pub fn expect(self, debug: &str) -> V {
        if self.errors.len() == 0 {
            self.result
        } else {
            panic!("{:?}: {:?}", debug, self.errors);
        }
    }

    pub fn unwrap(self) -> V {
        self.expect("error")
    }

    pub fn as_result(mut self) -> Result<V, E> {
        match self.errors.len() {
            0 => Ok(self.result),
            _ => Err(E::new_multi_error(
                self.errors.drain(..).map(|e| e.1).collect(),
            )),
        }
    }

    pub fn ok<'s>(&'s self) -> &'s V {
        &self.result
    }
}

#[macro_export]
macro_rules! c_try {
    ($result: expr, $expr: expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                $result.add_error(None, e.into());
                return $result;
            }
        }
    };
}
