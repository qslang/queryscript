use std::{collections::BTreeMap, sync::Arc};

use super::sql::{new_engine, SQLEngine, SQLEngineType};
use crate::ast::Ident;
use crate::compile::schema;
use crate::types::Value;

// A basic context with runtime state we can pass into functions. We may want
// to merge or consolidate this with the DataFusion context at some point
#[derive(Clone, Debug)]
pub struct Context {
    pub folder: Option<String>,
    pub values: BTreeMap<Ident, Value>,
    pub sql_engine: Arc<dyn SQLEngine>,
    pub disable_typechecks: bool,
}

impl Context {
    pub fn expensive<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        if tokio::runtime::Handle::current().runtime_flavor()
            == tokio::runtime::RuntimeFlavor::MultiThread
        {
            tokio::task::block_in_place(f)
        } else {
            f()
        }
    }

    pub fn new(schema: &schema::SchemaRef, engine_type: SQLEngineType) -> Context {
        let schema = schema.read().unwrap();
        Context {
            folder: schema.folder.clone(),
            values: BTreeMap::new(),
            sql_engine: new_engine(engine_type),
            disable_typechecks: false,
        }
    }

    pub fn disable_typechecks(&self) -> Context {
        Context {
            disable_typechecks: true,
            ..self.clone()
        }
    }
}
