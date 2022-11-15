// TODO: I've left some unused_imports here because they'll be useful while filling in the
// SchemaProvider implementation
use datafusion::arrow::{
    datatypes::{DataType, Field as DFField, Schema as DFSchema, SchemaRef as DFSchemaRef},
    record_batch::RecordBatch,
};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::FileFormat;
#[allow(unused_imports)]
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::object_store::ObjectStoreUrl;
#[allow(unused_imports)]
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::physical_expr::var_provider::{VarProvider, VarType};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::file_format::FileScanConfig;
#[allow(unused_imports)]
use datafusion::sql::{
    planner::{ContextProvider, SqlToRel},
    sqlparser::{dialect::GenericDialect, parser::Parser},
    TableReference,
};
use object_store::path::Path;
use object_store::ObjectMeta;
use sqlparser::ast as sqlast;
use std::{any::Any, collections::HashMap, sync::Arc};

use super::error::{fail, Result};
use crate::schema;
use crate::types;
use crate::types::{FnValue, Relation, Value};
use chrono;

pub fn eval(
    _schema: schema::SchemaRef,
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

    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    // let physical_planner = DefaultPhysicalPlanner::default();
    let records = runtime.block_on(async { execute_plan(&ctx, &plan).await })?;
    Ok(records)

    /*
    let mut ret = Vec::new();
    for batch in records.iter() {
        if batch.num_columns() != 1 {
            return fail!("More than 1 column: {}", batch.num_columns());
        }

        let col = batch.column(0);
        match col.data_type() {
            DataType::Float64 => {
                let arr: &Float64Array = col.as_any().downcast_ref().expect("Arrow cast");
                ret.extend(arr.iter().map(|i| match i {
                    Some(i) => Value::Float64(i as f64),
                    None => Value::Null,
                }));
            }
            DataType::Utf8 => {
                let arr: &StringArray = col.as_any().downcast_ref().expect("Arrow cast");
                // XXX Avoid copying
                ret.extend(arr.iter().map(|i| match i {
                    Some(i) => Value::Utf8(i.to_string()),
                    None => Value::Null,
                }));
            }
            dt => return rt_unimplemented!("Arrays of type {:?} (query: {})", dt, query.clone()),
        }
    }

    Ok(ret)
        */
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

        let schema = Arc::new(match to_dftype(ret_type) {
            DFType::Schema(s) => s,
            _ => return fail!("Return type of load_json is not a list of records"),
        });

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

#[derive(Clone, Debug)]
pub enum DFType {
    DataType(DataType),
    Schema(DFSchema),
    Unknown,
}

#[derive(Debug)]
pub struct SQLParam {
    pub name: String,
    pub value: Value,
    pub type_: DFType,
}

fn to_dfdatatype(type_: &types::Type) -> Option<DataType> {
    match type_ {
        types::Type::Atom(a) => match a {
            types::AtomicType::Null => Some(DataType::Null),
            types::AtomicType::Boolean => Some(DataType::Boolean),
            types::AtomicType::Float64 => Some(DataType::Float64),
            types::AtomicType::Utf8 => Some(DataType::Utf8),
            _ => None,
        },
        _ => None,
    }
}

fn to_dftype(type_: &types::Type) -> DFType {
    match type_ {
        types::Type::Atom(_) => match to_dfdatatype(type_) {
            Some(dt) => DFType::DataType(dt),
            None => DFType::Unknown,
        },
        types::Type::List(inner) => match inner.as_ref() {
            types::Type::Record(fields) => DFType::Schema(DFSchema {
                fields: fields
                    .iter()
                    .map(|x| {
                        DFField::new(
                            x.name.as_str(),
                            to_dfdatatype(&x.type_).unwrap_or(DataType::Null),
                            x.nullable,
                        )
                    })
                    .collect(),
                metadata: HashMap::new(),
            }),
            _ => DFType::Unknown,
        },
        _ => DFType::Unknown,
    }
}

impl SQLParam {
    pub fn new(name: String, value: Value, type_: &types::Type) -> SQLParam {
        SQLParam {
            name,
            value,
            type_: to_dftype(type_),
        }
    }

    pub fn register(&self, ctx: &mut SessionContext) -> DFResult<()> {
        match &self.type_ {
            DFType::Schema(s) => match &self.value {
                Value::Relation(r) => {
                    // XXX This copies the whole result Set
                    //
                    let table = MemTable::try_new(
                        Arc::new(s.clone()),
                        vec![r.clone().as_arrow_recordbatch().as_ref().clone()],
                    )?;
                    eprintln!("registering table: {}", self.name);
                    ctx.register_table(self.name.as_str(), Arc::new(table))?;
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Relations must be represented as record batches"
                    )));
                }
            },
            _ => {}
        };

        Ok(())
    }

    pub fn get_table_source(&self) -> DFResult<Arc<dyn TableSource>> {
        match &self.type_ {
            DFType::Schema(s) => Ok(Arc::new(SQLTableParam { schema: s.clone() })),
            _ => Err(DataFusionError::Execution(format!(
                "Not a table: {}",
                self.name
            ))),
        }
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

        let value = match &param.value {
            Value::Null => ScalarValue::Null,
            Value::Float64(n) => ScalarValue::Float64(Some(*n)),
            Value::Utf8(s) => ScalarValue::Utf8(Some(s.clone())),
            Value::Boolean(b) => ScalarValue::Boolean(Some(*b)),
            Value::Fn(_) => {
                return Err(DataFusionError::Internal(format!(
                    "Cannot use function value as a scalar variable"
                )))
            }
            Value::Relation(_) => {
                return Err(DataFusionError::Internal(format!(
                    "Cannot use record set as a scalar variable"
                )))
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported type: {:?}",
                    other
                )))
            }
        };

        Ok(value)
    }

    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        if var_names.len() != 1 {
            return None;
        }

        if let Some(p) = self.params.get(&var_names[0]) {
            match &p.type_ {
                DFType::DataType(t) => Some(t.clone()),
                _ => None,
            }
        } else {
            None
        }
    }
}
