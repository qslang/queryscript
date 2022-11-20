use datafusion::arrow::{
    datatypes::{DataType as DFDataType, Schema as DFSchema, SchemaRef as DFSchemaRef},
    record_batch::RecordBatch,
};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::physical_expr::var_provider::{VarProvider, VarType};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::sql::planner::SqlToRel;
use object_store::path::Path;
use object_store::ObjectMeta;
use sqlparser::ast as sqlast;
use std::{any::Any, collections::HashMap, sync::Arc};

use super::error::{fail, Result};
use crate::types;
use crate::types::{FnValue, Relation, Type, Value};
use chrono;

pub fn eval(query: &sqlast::Query, params: HashMap<String, SQLParam>) -> Result<Arc<dyn Relation>> {
    let mut ctx =
        SessionContext::with_config_rt(SessionConfig::new(), Arc::new(RuntimeEnv::default()));

    let schema_provider = Arc::new(SchemaProvider::new(params));
    register_params(schema_provider.clone(), &mut ctx)?;

    let state = ctx.state();
    let sql_to_rel = SqlToRel::new(&state);

    let mut ctes = HashMap::new(); // We may eventually want to parse/support these
    let plan = sql_to_rel.query_to_plan(query.clone(), &mut ctes)?;
    let plan = ctx.optimize(&plan)?;

    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    // let physical_planner = DefaultPhysicalPlanner::default();
    let records = runtime.block_on(async { execute_plan(&ctx, &plan).await })?;
    Ok(records)
}

async fn execute_plan(ctx: &SessionContext, plan: &LogicalPlan) -> Result<Arc<dyn Relation>> {
    let pplan = ctx.create_physical_plan(&plan).await?;
    let task_ctx = ctx.task_ctx();
    let results = Arc::new(collect(pplan, task_ctx).await?);
    Ok(results)
}

#[derive(Clone, Debug)]
pub struct LoadJsonFn {
    schema: Arc<DFSchema>,
}

impl LoadJsonFn {
    pub fn new(type_: &types::Type) -> Result<LoadJsonFn> {
        let ret_type = match type_ {
            types::Type::Fn(types::FnType { ret, .. }) => ret,
            _ => return fail!("Type of load_json is not a function"),
        };

        let mut schema = None;
        if let DFDataType::List(dt) = ret_type.as_ref().try_into()? {
            if let DFDataType::Struct(s) = dt.data_type() {
                schema = Some(Arc::new(DFSchema::new(s.clone())));
            }
        }

        let schema = match schema {
            Some(schema) => schema,
            None => {
                return fail!(
                    "Return type of load_json ({:?}) is not a list of records",
                    ret_type.as_ref(),
                )
            }
        };

        Ok(LoadJsonFn { schema })
    }

    pub fn load_json(&self, file: String) -> Result<Value> {
        let format = JsonFormat::default().with_schema_infer_max_rec(Some(0));

        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let location = Path::from_filesystem_path(file.as_str())?;
        let fmeta = std::fs::metadata(file)?;
        let ometa = ObjectMeta {
            location,
            last_modified: fmeta.modified().map(chrono::DateTime::from).unwrap(),
            size: fmeta.len() as usize,
        };
        // let physical_planner = DefaultPhysicalPlanner::default();
        let records = runtime.block_on(async {
            let plan = format
                .create_physical_plan(
                    FileScanConfig {
                        file_schema: self.schema.clone(),
                        file_groups: vec![vec![ometa.into()]],
                        limit: None,
                        object_store_url: ObjectStoreUrl::local_filesystem(),
                        projection: None,
                        statistics: Statistics {
                            num_rows: None,
                            total_byte_size: None,
                            column_statistics: None,
                            is_exact: true,
                        },
                        table_partition_cols: Vec::new(),
                        config_options: ConfigOptions::new().into_shareable(),
                    },
                    &[],
                )
                .await?;

            let ctx = SessionContext::with_config_rt(
                SessionConfig::new(),
                Arc::new(RuntimeEnv::default()),
            );
            let task_ctx = ctx.task_ctx();
            return DFResult::Ok(Arc::new(collect(plan, task_ctx).await? as Vec<RecordBatch>));
        })?;

        Ok(Value::Relation(records))
    }
}

impl FnValue for LoadJsonFn {
    fn execute(&self, args: Vec<Value>) -> Result<Value> {
        if args.len() != 1 {
            return fail!("load_json expects exactly one argument");
        }

        let file = match &args[0] {
            Value::Utf8(s) => s.clone(),
            _ => return fail!("load_json expects its first argument to be a string"),
        };

        self.load_json(file)
    }

    fn fn_type(&self) -> types::FnType {
        types::FnType {
            args: vec![types::Field {
                name: "file".to_string(),
                type_: types::Type::Atom(types::AtomicType::Utf8),
                nullable: false,
            }],
            ret: Box::new(
                (&self.schema.fields)
                    .try_into()
                    .expect("Failed to convert function type back"),
            ),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct SQLParam {
    pub name: String,
    pub value: Value,
    pub type_: Type,
}

impl SQLParam {
    pub fn new(name: String, value: Value, type_: &types::Type) -> SQLParam {
        SQLParam {
            name,
            value,
            type_: type_.clone(), // TODO: We should make this a reference that lives as long as
                                  // the SQLParam
        }
    }

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
