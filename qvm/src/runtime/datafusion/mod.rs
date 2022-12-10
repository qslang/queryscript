use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::datatypes::{
    DataType as DFDataType, Schema as DFSchema, SchemaRef as DFSchemaRef,
};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::datasource::memory::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::TableSource;
use datafusion::physical_expr::var_provider::{VarProvider, VarType};
use datafusion::physical_plan::collect;
use datafusion::sql::planner::SqlToRel;
use sqlparser::ast as sqlast;
use std::{any::Any, collections::HashMap, sync::Arc};

use super::error::Result;
use super::sql::{SQLEngine, SQLParam};
use crate::types::{Relation, Value};

#[derive(Debug)]
pub struct DataFusionEngine();

impl DataFusionEngine {
    pub fn new() -> DataFusionEngine {
        DataFusionEngine()
    }
}

#[async_trait::async_trait]
impl SQLEngine for DataFusionEngine {
    async fn eval(
        &self,
        query: &sqlast::Query,
        params: HashMap<String, SQLParam>,
    ) -> Result<Arc<dyn Relation>> {
        let mut ctx =
            SessionContext::with_config_rt(SessionConfig::new(), Arc::new(RuntimeEnv::default()));

        let schema_provider = Arc::new(SchemaProvider::new(params));
        register_params(schema_provider.clone(), &mut ctx)?;

        let state = ctx.state();
        let sql_to_rel = SqlToRel::new(&state);

        let mut ctes = HashMap::new(); // We may eventually want to parse/support these
        let plan = sql_to_rel.query_to_plan(query.clone(), &mut ctes)?;
        let plan = ctx.optimize(&plan)?;

        let pplan = ctx.create_physical_plan(&plan).await?;
        let task_ctx = ctx.task_ctx();
        let results = Arc::new(collect(pplan, task_ctx).await?);
        Ok(results)
    }
}

impl SQLParam {
    // XXX Can we remove or rename this function?
    pub fn register(&self, ctx: &mut SessionContext) -> DFResult<()> {
        let schema: Arc<ArrowSchema> = match (&self.type_).try_into() {
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

struct SQLTableParam {
    schema: DFSchema,
}

impl TableSource for SQLTableParam {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DFSchemaRef {
        Arc::new(self.schema.clone())
    }
}

struct SchemaProvider {
    params: HashMap<String, SQLParam>,
}

impl SchemaProvider {
    fn new(params: HashMap<String, SQLParam>) -> Self {
        SchemaProvider { params }
    }
}

fn register_params(schema: Arc<SchemaProvider>, ctx: &mut SessionContext) -> Result<()> {
    ctx.register_variable(VarType::UserDefined, schema.clone());

    for (_, param) in &schema.params {
        param.register(ctx)?;
    }

    Ok(())
}

impl VarProvider for SchemaProvider {
    fn get_value(&self, var_names: Vec<String>) -> DFResult<ScalarValue> {
        if var_names.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Invalid mutli-part variable name: {:?}",
                var_names
            )));
        }

        let param = self.params.get(&var_names[0]).unwrap();

        let value = match param.value.clone().try_into() {
            Ok(v) => v,
            Err(e) => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported conversion: {:?}",
                    e
                )));
            }
        };

        Ok(value)
    }

    fn get_type(&self, var_names: &[String]) -> Option<DFDataType> {
        if var_names.len() != 1 {
            return None;
        }

        if let Some(p) = self.params.get(&var_names[0]) {
            (&p.type_).try_into().ok()
        } else {
            None
        }
    }
}
