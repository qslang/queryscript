use sqlparser::ast as sqlast;
use std::{collections::HashMap, sync::Arc};

use super::error::Result;
use crate::ast::Ident;
use crate::compile::ConnectionString;
use crate::types::{Relation, Type, Value};

use async_trait::async_trait;

// TODO: For foreign databases (e.g. Postgres), we'll either need to store
// parameters to these databases in the engine (which may manage 1 or more
// connections), or pass them in. Either way, engines are expected to manage
// interior mutability, since concurrency properties may differ across them.
#[async_trait]
pub trait SQLEngine: std::fmt::Debug + Send + Sync {
    async fn eval(
        &mut self,
        query: &sqlast::Statement,
        params: HashMap<Ident, SQLParam>,
    ) -> Result<Arc<dyn Relation>>;

    async fn load(
        &mut self,
        table: &sqlast::ObjectName,
        value: Value,
        type_: Type,
        temporary: bool,
    ) -> Result<()>;

    async fn create(&mut self) -> Result<()>;

    fn engine_type(&self) -> SQLEngineType;
}

pub trait SQLEnginePool {
    fn new(url: Option<Arc<ConnectionString>>) -> Result<Box<dyn SQLEngine>>;
}

#[derive(Debug, Clone)]
pub struct SQLParam {
    pub name: Ident,
    pub value: Value,
    pub type_: Type,
}

impl SQLParam {
    pub fn new(name: Ident, value: Value, type_: &Type) -> SQLParam {
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

pub fn embedded_engine(kind: SQLEngineType) -> Box<dyn SQLEngine> {
    use SQLEngineType::*;
    match kind {
        DuckDB => super::duckdb::DuckDBEngine::new(None),
    }
    .expect("Failed to create embedded engine")
}

// TODO We should turn this into a real pool
pub fn new_engine(url: Arc<ConnectionString>) -> Result<Box<dyn SQLEngine>> {
    use SQLEngineType::*;
    match url.engine_type() {
        DuckDB => super::duckdb::DuckDBEngine::new(Some(url)),
    }
}
