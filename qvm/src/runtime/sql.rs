// TODO: I've left some unused_imports here because they'll be useful while filling in the
// SchemaProvider implementation
#[allow(unused_imports)]
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::execution::context::{SessionConfig, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
#[allow(unused_imports)]
use datafusion::logical_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, LogicalPlan, ScalarUDF, TableSource,
};
use datafusion::physical_expr::{
    execution_props::ExecutionProps,
    var_provider::{VarProvider, VarType},
};
use datafusion::physical_plan::collect;
#[allow(unused_imports)]
use datafusion::sql::{
    planner::{ContextProvider, SqlToRel},
    sqlparser::{dialect::GenericDialect, parser::Parser},
    TableReference,
};
use sqlparser::ast as sqlast;
use std::{collections::HashMap, sync::Arc};

use super::error::Result;
use crate::runtime::runtime::Value;
use crate::schema;

pub fn eval(
    _schema: schema::SchemaRef,
    query: &sqlast::Query,
    params: HashMap<Vec<String>, SQLParam>,
) -> Result<()> {
    let schema_provider = Arc::new(SchemaProvider::new(params));
    let sql_to_rel = SqlToRel::new(schema_provider.as_ref());

    let mut ctes = HashMap::new(); // We may eventually want to parse/support these
    let plan = sql_to_rel.query_to_plan(query.clone(), &mut ctes)?;

    let mut session_state =
        SessionState::with_config_rt(SessionConfig::new(), Arc::new(RuntimeEnv::default()));

    let plan = session_state.optimize(&plan)?;

    let mut execution_props = ExecutionProps::new();
    execution_props.add_var_provider(VarType::UserDefined, schema_provider.clone());
    session_state.execution_props = execution_props;

    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    // let physical_planner = DefaultPhysicalPlanner::default();
    runtime.block_on(async { execute_plan(&session_state, &plan).await })?;

    Ok(())
}

async fn execute_plan(session_state: &SessionState, plan: &LogicalPlan) -> Result<()> {
    let pplan = session_state.create_physical_plan(&plan).await?;
    let task_ctx = Arc::new(TaskContext::from(session_state));
    let results = collect(pplan, task_ctx).await?;
    eprintln!("Results: {:?}", results);
    Ok(())
}

#[derive(Debug)]
pub struct SQLParam {
    pub name: Vec<String>,
    pub value: Value,
    pub type_: Option<DataType>,
}

impl SQLParam {
    pub fn new(name: Vec<String>, value: Value, type_: &schema::Type) -> SQLParam {
        eprintln!("{:?} value: {:?} type: {:?}", name, value, type_);
        SQLParam {
            name,
            value,
            type_: match type_ {
                schema::Type::Atom(a) => match a {
                    schema::AtomicType::Null => Some(DataType::Null),
                    schema::AtomicType::Bool => Some(DataType::Boolean),
                    schema::AtomicType::Number => Some(DataType::Int64),
                    schema::AtomicType::String => Some(DataType::Utf8),
                },
                _ => None,
            },
        }
    }
}

struct SchemaProvider {
    params: HashMap<Vec<String>, SQLParam>,
}

impl SchemaProvider {
    fn new(params: HashMap<Vec<String>, SQLParam>) -> Self {
        SchemaProvider { params }
    }
}

impl ContextProvider for SchemaProvider {
    fn get_table_provider(&self, name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        Err(DataFusionError::Plan(format!(
            "Table not found: {}",
            name.table()
        )))
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, names: &[String]) -> Option<DataType> {
        if let Some(p) = self.params.get(&names.to_vec()) {
            eprintln!("p: {:?}", p);
            p.type_.clone()
        } else {
            None
        }
    }
}

impl VarProvider for SchemaProvider {
    fn get_value(&self, var_names: Vec<String>) -> DFResult<ScalarValue> {
        let param = self.params.get(&var_names.to_vec()).unwrap();

        let value = match &param.value {
            Value::Null => ScalarValue::Null,
            Value::Number(n) => ScalarValue::Float64(Some(*n)),
            Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
            Value::Bool(b) => ScalarValue::Boolean(Some(*b)),
        };

        eprintln!("value: {:?}", value);
        Ok(value)
    }

    /// Return the type of the given variable
    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        self.get_variable_type(var_names)
    }
}
