pub mod context;
pub mod error;
pub mod functions;
mod normalize;
pub mod runtime;
pub mod sql;

pub mod duckdb;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;

// NOTE: MySQL is half-supported, but I didn't finish because it has very different syntax
// than what sqlparser spits out
// pub mod mysql;

// NOTE: Datafusion is no longer in the repo, so this is technically dead code.
// pub mod datafusion;

pub use crate::runtime::runtime::*;
pub use context::{Context, ContextPool};
pub use error::{Result, RuntimeError};
pub use sql::*;
