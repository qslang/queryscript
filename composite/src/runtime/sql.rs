#[allow(unused_imports)]
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result as DFResult};
#[allow(unused_imports)]
use datafusion::logical_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
#[allow(unused_imports)]
use datafusion::sql::{
    planner::{ContextProvider, SqlToRel},
    sqlparser::{dialect::GenericDialect, parser::Parser},
    TableReference,
};
use sqlparser::ast as sqlast;
use std::{collections::HashMap, sync::Arc};

use super::error::Result;
use crate::schema;

pub fn eval(_schema: &schema::Schema, query: &sqlast::Query) -> Result<()> {
    let schema_provider = SchemaProvider::new();
    let sql_to_rel = SqlToRel::new(&schema_provider);

    let mut ctes = HashMap::new(); // We may eventually want to parse/support these
    let plan = sql_to_rel.query_to_plan(query.clone(), &mut ctes)?;

    eprintln!("Plan: {:?}", plan);

    Ok(())
}

struct SchemaProvider {}

impl SchemaProvider {
    fn new() -> Self {
        Self {}
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

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }
}
