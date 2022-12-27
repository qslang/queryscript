use sqlparser::ast as sqlast;
use std::{collections::HashMap, sync::Arc};

use super::{error::Result, Context};
use crate::types::{Relation, Type, Value};

use async_trait::async_trait;

// TODO: For foreign databases (e.g. Postgres), we'll either need to store
// parameters to these databases in the engine (which may manage 1 or more
// connections), or pass them in. Either way, engines are expected to manage
// interior mutability, since concurrency properties may differ across them.
#[async_trait]
pub trait SQLEngine: std::fmt::Debug + Send + Sync {
    async fn eval(
        &self,
        ctx: &Context,
        query: &sqlast::Query,
        params: HashMap<String, SQLParam>,
    ) -> Result<Arc<dyn Relation>>;
}

#[derive(Debug)]
pub struct SQLParam {
    pub name: String,
    pub value: Value,
    pub type_: Type,
}

impl SQLParam {
    pub fn new(name: String, value: Value, type_: &Type) -> SQLParam {
        SQLParam {
            name,
            value,
            type_: type_.clone(), // TODO: We should make this a reference that lives as long as
                                  // the SQLParam
        }
    }
}

#[derive(Copy, Clone)]
pub enum SQLEngineType {
    DuckDB,
}

impl SQLEngineType {
    pub fn from_name(name: &str) -> Result<SQLEngineType> {
        use SQLEngineType::*;
        Ok(match name.to_lowercase().as_str() {
            "duckdb" => DuckDB,
            name => {
                return Err(crate::runtime::RuntimeError::unimplemented(
                    format!("SQL engine {}", name).as_str(),
                ))
            }
        })
    }
}

pub fn new_engine(kind: SQLEngineType) -> Arc<dyn SQLEngine> {
    use SQLEngineType::*;
    match kind {
        DuckDB => Arc::new(super::duckdb::DuckDBEngine::new()),
    }
}
