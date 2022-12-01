use crate::compile::schema;
use crate::types::Value;
use std::collections::BTreeMap;

// A basic context with runtime state we can pass into functions. We may want
// to merge or consolidate this with the DataFusion context at some point
#[derive(Clone, Debug)]
pub struct Context {
    pub folder: Option<String>,
    pub values: BTreeMap<String, Value>,
}

impl From<&schema::SchemaRef> for Context {
    fn from(schema: &schema::SchemaRef) -> Self {
        let schema = schema.read().unwrap();
        Context {
            folder: schema.folder.clone(),
            values: BTreeMap::new(),
        }
    }
}
