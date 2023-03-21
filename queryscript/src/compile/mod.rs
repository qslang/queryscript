pub mod autocomplete;
pub mod builtin_types;
mod coerce;
pub mod compile;
mod connection;
pub mod error;
mod external;
mod fmtstring;
pub mod generics;
pub mod inference;
pub mod inline;
pub mod schema;
mod scope;
pub mod sql;
pub mod traverse;
mod unsafe_expr;
mod util;

pub(crate) use compile::casync;
pub use compile::{
    lookup_path, lookup_schema, Compiler, CompilerConfig, OnSchema, OnSymbol, SymbolKind,
};
pub use connection::ConnectionString;
pub use error::{CompileError, Result};
pub use schema::{mkref, Schema, SchemaRef};
pub use sql::compile_reference;
