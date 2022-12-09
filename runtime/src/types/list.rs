use super::types::*;
use super::value::*;

#[derive(Debug)]
pub struct VecList {
    data_type: Arc<Type>,
    values: Vec<Value>,
}

impl VecList {
    pub fn new(data_type: Arc<Type>, values: Vec<Value>) -> Arc<dyn List> {
        Arc::new(VecList { data_type, values })
    }
}

impl List for VecList {
    fn data_type(&self) -> Type {
        self.data_type.as_ref().clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_vec(&self) -> Vec<Value> {
        self.values.clone()
    }
}
