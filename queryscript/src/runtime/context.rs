use std::{collections::BTreeMap, sync::Arc};

use super::sql::{new_engine, SQLEngine, SQLEngineType};
use crate::ast::Ident;
use crate::types::Value;

// A basic context with runtime state we can pass into functions. We may want
// to merge or consolidate this with the DataFusion context at some point
#[derive(Clone, Debug)]
pub struct Context {
    pub folder: Option<String>,
    pub values: BTreeMap<Ident, Value>,
    pub sql_engine: Arc<dyn SQLEngine>,
    pub disable_typechecks: bool,

    /// Materializations that we've saved up and can re-use. Each materialization is itself
    /// protected by a lock, so that if multiple tasks running in parallel are trying to compute
    /// the same materialization, we can ensure only one does.
    pub materializations: Arc<tokio::sync::Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<Value>>>>>,
}

impl Context {
    pub fn expensive<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(f)
            }
            _ => f(),
        }
    }

    pub fn new(folder: Option<String>, engine_type: SQLEngineType) -> Context {
        Context {
            folder,
            values: BTreeMap::new(),
            sql_engine: new_engine(engine_type),
            disable_typechecks: false,
            materializations: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        }
    }

    pub fn disable_typechecks(&self) -> Context {
        Context {
            disable_typechecks: true,
            ..self.clone()
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
