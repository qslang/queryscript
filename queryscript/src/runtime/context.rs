use std::{borrow::BorrowMut, collections::BTreeMap, sync::Arc};

use super::{
    error::Result,
    sql::{embedded_engine, SQLEngine, SQLEngineType},
};
use crate::{ast::Ident, compile::ConnectionString, runtime::new_engine, types::Value};

// A basic context with runtime state we can pass into functions. We may want
// to merge or consolidate this with the DataFusion context at some point
#[derive(Debug)]
pub struct Context {
    pub folder: Option<String>,
    pub values: BTreeMap<Ident, Value>,
    embedded_sql: Box<dyn SQLEngine>,
    pub disable_typechecks: bool,

    /// Connections that persist for the lifetime of the context. This guarantees that any given context
    /// has exactly one connection to a given database.
    connections: BTreeMap<Arc<ConnectionString>, Box<dyn SQLEngine>>,

    /// Materializations that we've saved up and can re-use.
    pub materializations: BTreeMap<String, Value>,
}

impl Context {
    pub fn new(folder: Option<String>, engine_type: SQLEngineType) -> Context {
        Context {
            folder,
            values: BTreeMap::new(),
            embedded_sql: embedded_engine(engine_type),
            disable_typechecks: false,
            connections: BTreeMap::new(),
            materializations: BTreeMap::new(),
        }
    }

    pub fn disable_typechecks(&self) -> Context {
        Context {
            disable_typechecks: true,
            ..self.clone()
        }
    }

    pub async fn sql_engine<'a>(
        &'a mut self,
        url: Option<Arc<crate::compile::ConnectionString>>,
    ) -> Result<&'a mut (dyn SQLEngine + 'static)> {
        let url = match url {
            Some(url) => url,
            None => return Ok(self.embedded_sql.borrow_mut()),
        };

        use std::collections::btree_map::Entry;
        Ok(match self.connections.entry(url.clone()) {
            Entry::Occupied(entry) => entry.into_mut().borrow_mut(),
            Entry::Vacant(entry) => {
                // TODO We should turn this into a real pool
                let engine = new_engine(url)?;
                entry.insert(engine).borrow_mut()
            }
        })
    }
}

impl Clone for Context {
    fn clone(&self) -> Context {
        Context {
            folder: self.folder.clone(),
            values: self.values.clone(),
            embedded_sql: embedded_engine(self.embedded_sql.engine_type()),
            disable_typechecks: self.disable_typechecks,
            materializations: self.materializations.clone(),

            // Connections cannot be shared between contexts
            connections: BTreeMap::new(),
        }
    }
}

// This should eventually be a real pool
pub struct ContextPool {
    pub folder: Option<String>,
    pub engine_type: SQLEngineType,
}

impl ContextPool {
    pub fn new(folder: Option<String>, engine_type: SQLEngineType) -> ContextPool {
        ContextPool {
            folder,
            engine_type,
        }
    }

    pub fn get(&self) -> Context {
        Context::new(self.folder.clone(), self.engine_type)
    }
}
