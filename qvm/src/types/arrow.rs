use super::types::{try_arrow_fields_to_fields, Field};
use super::value::*;

impl Relation for Vec<ArrowRecordBatch> {
    fn schema(&self) -> Vec<Field> {
        if self.len() == 0 {
            panic!("Empty vector of record batches. Cannot derive schema.");
        }

        let first = self[0].schema();
        try_arrow_fields_to_fields(&first.fields).expect("Failed to convert relation type")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn row(&self, index: usize) -> &dyn Record {
        panic!("Unimplemented: row()")
    }

    fn column(&self, index: usize) -> &dyn List {
        panic!("Unimplemented: column()")
    }

    fn as_arrow_recordbatch(self: Arc<Self>) -> Arc<Vec<ArrowRecordBatch>> {
        self
    }
}
