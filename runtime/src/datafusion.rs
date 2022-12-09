use datafusion::arrow::datatypes::{
    DataType as DFDataType, Schema as DFSchema, SchemaRef as DFSchemaRef,
};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::datasource::memory::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::physical_expr::var_provider::{VarProvider, VarType};
use datafusion::physical_plan::collect;
use datafusion::sql::planner::SqlToRel;
use sqlparser::ast as sqlast;
use std::{any::Any, collections::HashMap, sync::Arc};

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
