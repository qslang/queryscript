use crate::compile::schema;

// A basic context with runtime state we can pass into functions. We may want
// to merge or consolidate this with the DataFusion context at some point
pub struct Context {
    pub folder: Option<String>,
}

impl From<&schema::SchemaRef> for Context {
    fn from(schema: &schema::SchemaRef) -> Self {
        let schema = schema.read().unwrap();
        Context {
            folder: schema.folder.clone(),
        }
    }
}
