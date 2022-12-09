use datafusion::arrow::datatypes::Schema as DFSchema;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::memory::MemTable;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

use crate::types::Value;
impl crate::SQLParam {
    // XXX Can we remove or rename this function?
    pub fn register(&self, ctx: &mut SessionContext) -> DFResult<()> {
        let schema: Arc<DFSchema> = match (&self.type_).try_into() {
            Ok(schema) => Arc::new(schema),
            Err(_) => return Ok(()), // Registering a non-table is a no-op
        };
        let record_batch = match &self.value {
            Value::Relation(r) => r.clone().as_arrow_recordbatch(),
            other => {
                return Err(DataFusionError::Internal(format!(
                    "Unexpected non-relation {:?}",
                    other
                )))
            }
        };
        let table = MemTable::try_new(schema, vec![record_batch.as_ref().clone()])?;
        ctx.register_table(self.name.as_str(), Arc::new(table))?;

        Ok(())
    }
}
