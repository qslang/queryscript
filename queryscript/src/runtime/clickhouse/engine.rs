use sqlparser::ast as sqlast;
use std::fmt;

use clickhouse_rs::{types::SqlType, ClientHandle, Pool};
use std::{collections::HashMap, sync::Arc};

use crate::{
    ast::Ident,
    compile::ConnectionString,
    runtime::{error::rt_unimplemented, Result, SQLEngine, SQLEnginePool, SQLEngineType, SQLParam},
    types::{
        arrow::ArrowRecordBatchRelation, try_fields_to_arrow_fields, value::ArrowRecordBatch,
        ArrowSchema, Field, Relation, Type, Value,
    },
};

use super::value;

pub struct ClickHouseEngine {
    conn: ClientHandle,
}

impl fmt::Debug for ClickHouseEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClickhouseEngine")
    }
}

#[async_trait::async_trait]
impl SQLEnginePool for ClickHouseEngine {
    async fn new(url: Arc<ConnectionString>) -> Result<Box<dyn SQLEngine>> {
        let mut url = url.get_url().clone();
        url.set_scheme("tcp").unwrap();
        let conn = Pool::new(url.as_str()).get_handle().await?;
        Ok(Box::new(ClickHouseEngine { conn }))
    }
}

#[async_trait::async_trait]
impl SQLEngine for ClickHouseEngine {
    async fn eval(
        &mut self,
        query: &sqlast::Statement,
        params: HashMap<Ident, SQLParam>,
    ) -> Result<Arc<dyn Relation>> {
        for (key, param) in params.iter() {
            match &param.value {
                Value::Relation(_) => {
                    return rt_unimplemented!("Relation parameters in ClickHouse");
                }
                Value::Fn(_) => {
                    return rt_unimplemented!("Function parameters in ClickHouse");
                }
                _ => {
                    return rt_unimplemented!("Scalar parameters in ClickHouse");
                }
            }
        }

        let query_string = format!("{}", query);
        eprintln!("QUERY: {}", query_string);
        let result = self.conn.query(query_string).fetch_all().await?;
        eprintln!("RESULT: {:?}", result);

        let mut schema = Vec::new();
        let mut arrays = Vec::new();
        for column in result.columns() {
            let field = Field {
                name: column.name().to_string().into(),
                type_: (&column.sql_type()).try_into()?,
                nullable: matches!(column.sql_type(), SqlType::Nullable(_)),
            };
            schema.push(field);

            // XXX Next step is to batch the rows the same way we do arrow rows
            arrays.push(value::column_to_arrow(column, column.sql_type(), false)?);
        }

        let schema = Arc::new(ArrowSchema::new(try_fields_to_arrow_fields(&schema)?));
        let record_batch = ArrowRecordBatch::try_new(schema.clone(), arrays)?;
        let relation = ArrowRecordBatchRelation::new(schema, Arc::new(vec![record_batch]));
        Ok(relation)
    }

    async fn load(
        &mut self,
        table: &sqlast::ObjectName,
        value: Value,
        type_: Type,
        temporary: bool,
    ) -> Result<()> {
        todo!()
    }

    async fn create(&mut self) -> Result<()> {
        // NOTE: This should probably be a method on the pool, not the engine,
        // since the connection assumes that the database exists.
        Ok(())
    }

    /// Ideally, this gets generalized and we use information schema tables. However, there's
    /// no standard way to tell what database we're currently in. We should generalize this function
    /// eventually.
    async fn table_exists(&mut self, name: &sqlast::ObjectName) -> Result<bool> {
        todo!()
    }

    fn engine_type(&self) -> SQLEngineType {
        SQLEngineType::ClickHouse
    }
}
