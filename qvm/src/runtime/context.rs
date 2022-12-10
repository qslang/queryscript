use std::{collections::BTreeMap, sync::Arc};

use super::sql::SQLEngine;
use crate::types::Value;

// A basic context with runtime state we can pass into functions. We may want
// to merge or consolidate this with the DataFusion context at some point
#[derive(Clone, Debug)]
pub struct Context {
    pub folder: Option<String>,
    pub values: BTreeMap<String, Value>,
    pub sql_engine: Arc<dyn SQLEngine>,
}
