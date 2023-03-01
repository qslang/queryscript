use sqlparser::ast as sqlast;
use std::{collections::HashMap, sync::Arc};

use super::error::Result;
use crate::ast::Ident;
use crate::compile::ConnectionString;
use crate::types::{Relation, Type, Value};

use async_trait::async_trait;

/// An engine is a wrapper around a connection to a database, capable of running certain
/// commands. The functions except a mutable reference to the engine, so that we can leverage
/// the Rust compiler to ensure exclusive access.
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

    /// Ideally, this gets generalized and we use information schema tables. However, there's
    /// no standard way to tell what database we're currently in. We should generalize this function
    /// eventually.
    async fn table_exists(&mut self, name: &sqlast::ObjectName) -> Result<bool>;

    fn engine_type(&self) -> SQLEngineType;
}

#[async_trait]
pub trait SQLEnginePool {
    async fn new(url: Arc<ConnectionString>) -> Result<Box<dyn SQLEngine>>;
}

pub trait SQLEmbedded {
    fn new_embedded() -> Result<Box<dyn SQLEngine>>;
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

#[derive(Copy, Clone, Debug)]
pub enum SQLEngineType {
    DuckDB,
    MySQL,
}

impl SQLEngineType {
    pub fn from_name(name: &str) -> Result<SQLEngineType> {
        use SQLEngineType::*;
        Ok(match name.to_lowercase().as_str() {
            "duckdb" => DuckDB,
            "mysql" => MySQL,
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
        DuckDB => super::duckdb::DuckDBEngine::new_embedded(),
        o => panic!("Unsupported embedded engine: {:?}", o),
    }
    .expect("Failed to create embedded engine")
}

// TODO We should turn this into a real pool (i.e. cache the state
// globally somewhere)
pub async fn new_engine(url: Arc<ConnectionString>) -> Result<Box<dyn SQLEngine>> {
    use SQLEngineType::*;
    match url.engine_type() {
        DuckDB => super::duckdb::DuckDBEngine::new(url),
        MySQL => super::duckdb::DuckDBEngine::new(url),
    }
    .await
}
