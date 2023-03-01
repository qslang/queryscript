use super::types::*;
use super::value::*;

#[derive(Debug, Clone)]
pub struct VecRow {
    schema: Arc<Vec<Field>>,
    values: Vec<Value>,
}

impl VecRow {
    pub fn new(schema: Arc<Vec<Field>>, values: Vec<Value>) -> Arc<dyn Record> {
        Arc::new(VecRow { schema, values })
    }
}

impl Record for VecRow {
    fn schema(&self) -> Vec<Field> {
        self.schema.as_ref().clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn column(&self, index: usize) -> &Value {
        &self.values[index]
    }
}
