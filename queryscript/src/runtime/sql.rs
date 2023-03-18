use sqlparser::ast as sqlast;
use std::{collections::HashMap, sync::Arc};
use strum::{EnumIter, IntoEnumIterator};

use super::error::Result;
use crate::ast::Ident;
use crate::compile::ConnectionString;
use crate::types::{LazyValue, Relation, Type, Value};

use async_trait::async_trait;

/// An engine is a wrapper around a connection to a database, capable of running certain
/// commands. The functions except a mutable reference to the engine, so that we can leverage
/// the Rust compiler to ensure exclusive access.
#[async_trait]
pub trait SQLEngine: std::fmt::Debug + Send + Sync {
    async fn query(
        &mut self,
        query: &sqlast::Statement,
        params: HashMap<Ident, LazySQLParam>,
    ) -> Result<Arc<dyn Relation>>;

    async fn exec(
        &mut self,
        stmt: &sqlast::Statement,
        params: HashMap<Ident, LazySQLParam>,
    ) -> Result<()>;

    async fn load(
        &mut self,
        table: &sqlast::ObjectName,
        value: Value,
        type_: Type,
        temporary: bool,
    ) -> Result<()>;

    /// Ideally, this gets generalized and we use information schema tables. However, there's
    /// no standard way to tell what database we're currently in. We should generalize this function
    /// eventually.
    async fn table_exists(&mut self, name: &sqlast::ObjectName) -> Result<bool>;

    fn engine_type(&self) -> SQLEngineType;
}

#[async_trait]
pub trait SQLEnginePool {
    async fn create(url: Arc<ConnectionString>) -> Result<()>;
    async fn new(url: Arc<ConnectionString>) -> Result<Box<dyn SQLEngine>>;

    fn normalize(query: &sqlast::Statement) -> Result<sqlast::Statement>;
}

pub trait SQLEmbedded {
    fn new_embedded() -> Result<Box<dyn SQLEngine>>;
}

#[derive(Debug, Clone)]
pub struct LazySQLParam {
    pub name: Ident,
    pub value: Box<dyn LazyValue>,
    pub type_: Type,
}

#[derive(Debug, Clone)]
pub struct SQLParam {
    pub name: Ident,
    pub value: Value,
    pub type_: Type,
}

impl LazySQLParam {
    pub fn new(name: Ident, value: Box<dyn LazyValue>, type_: &Type) -> LazySQLParam {
        LazySQLParam {
            name,
            value,
            type_: type_.clone(), // TODO: We should make this a reference that lives as long as
                                  // the SQLParam
        }
    }

    pub async fn get(mut self) -> Result<SQLParam> {
        Ok(SQLParam {
            name: self.name,
            value: self.value.get().await?,
            type_: self.type_,
        })
    }
}

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum SQLEngineType {
    ClickHouse,
    DuckDB,
}

impl SQLEngineType {
    pub fn from_name(name: &str) -> Result<SQLEngineType> {
        use SQLEngineType::*;
        Ok(match name.to_lowercase().as_str() {
            "clickhouse" => ClickHouse,
            "duckdb" => DuckDB,
            name => {
                return Err(crate::runtime::RuntimeError::unimplemented(
                    format!("SQL engine {}", name).as_str(),
                ))
            }
        })
    }

    pub fn iterator() -> impl Iterator<Item = SQLEngineType> {
        // This just avoids having to import strum::IntoEnumIterator everywhere
        SQLEngineType::iter()
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
pub async fn create_database(url: Arc<ConnectionString>) -> Result<()> {
    use SQLEngineType::*;
    match url.engine_type() {
        ClickHouse => super::clickhouse::ClickHouseEngine::create(url),
        DuckDB => super::duckdb::DuckDBEngine::create(url),
    }
    .await
}

// TODO We should turn this into a real pool (i.e. cache the state
// globally somewhere)
pub async fn new_engine(url: Arc<ConnectionString>) -> Result<Box<dyn SQLEngine>> {
    use SQLEngineType::*;
    match url.engine_type() {
        ClickHouse => super::clickhouse::ClickHouseEngine::new(url),
        DuckDB => super::duckdb::DuckDBEngine::new(url),
    }
    .await
}

pub fn normalize(
    engine_type: SQLEngineType,
    query: &sqlast::Statement,
) -> Result<sqlast::Statement> {
    use SQLEngineType::*;
    match engine_type {
        ClickHouse => super::clickhouse::ClickHouseEngine::normalize(query),
        DuckDB => super::duckdb::DuckDBEngine::normalize(query),
    }
}
