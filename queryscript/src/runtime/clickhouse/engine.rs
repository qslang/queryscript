use sqlparser::ast as sqlast;
use std::{borrow::Cow, fmt};

use clickhouse_rs::{types::SqlType, Block, ClientHandle, Pool};
use std::{collections::HashMap, sync::Arc};

use crate::{
    ast::Ident,
    compile::{sql::create_table, ConnectionString},
    runtime::{
        error::rt_unimplemented, normalize::Normalizer, Result, SQLEngine, SQLEnginePool,
        SQLEngineType, SQLParam,
    },
    types::{
        arrow::{ArrowRecordBatchRelation, EmptyRelation},
        try_fields_to_arrow_fields,
        value::ArrowRecordBatch,
        ArrowSchema, Field, Relation, Type, Value,
    },
};

use super::value;

pub struct ClickHouseNormalizer();

impl ClickHouseNormalizer {
    pub fn new() -> ClickHouseNormalizer {
        ClickHouseNormalizer()
    }
}

impl Normalizer for ClickHouseNormalizer {
    fn quote_style(&self) -> Option<char> {
        Some('"')
    }

    fn param(&self, _key: &str) -> Option<&str> {
        None
    }

    fn preprocess<'a>(&self, stmt: &'a sqlast::Statement) -> Cow<'a, sqlast::Statement> {
        match stmt {
            sqlast::Statement::CreateTable { .. } => {
                let mut stmt = stmt.clone();
                match &mut stmt {
                    sqlast::Statement::CreateTable {
                        engine,
                        order_by,
                        temporary,
                        or_replace,
                        if_not_exists,
                        ..
                    } => {
                        if *temporary {
                            if *or_replace {
                                // This is unfortunate and hacky behavior, but ClickHouse does
                                // not support CREATE OR REPLACE TEMPORARY TABLE, so we refocus
                                // the statement to be if_not_exists. In practice, this should be
                                // fine because we only create temporary tables when the table does
                                // not exist.
                                *or_replace = false;
                                *if_not_exists = true;
                            }

                            *engine = Some("Memory".to_string());
                        } else if matches!(engine, None) {
                            // ClickHouse requires an engine to be specified.
                            // TODO: You should be able to configure these
                            *engine = Some("MergeTree".to_string());
                            *order_by = Some(vec![]);
                        }
                    }
                    _ => unreachable!(),
                }
                Cow::Owned(stmt)
            }
            _ => Cow::Borrowed(stmt),
        }
    }
}

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
        let mut conn = Pool::new(url.as_str()).get_handle().await?;
        conn.ping().await?;

        Ok(Box::new(ClickHouseEngine { conn }))
    }

    async fn create(url: Arc<ConnectionString>) -> Result<()> {
        let mut url = url.get_url().clone();
        url.set_scheme("tcp").unwrap();
        let db_name = url.path().to_string();
        let db_name = db_name.strip_prefix("/").unwrap();
        url.set_path("");
        let mut conn = Pool::new(url.as_str()).get_handle().await?;
        conn.ping().await?;

        // ClickHouse fails on CREATE OR REPLACE TABLE commands if running in a Docker
        // container because it uses renameat2()
        conn.execute(format!("DROP DATABASE IF EXISTS \"{}\"", db_name))
            .await?;
        conn.execute(format!("CREATE DATABASE \"{}\"", db_name))
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl SQLEngine for ClickHouseEngine {
    async fn query(
        &mut self,
        query: &sqlast::Statement,
        params: HashMap<Ident, SQLParam>,
    ) -> Result<Arc<dyn Relation>> {
        self.check_params(&params)?;

        let query = ClickHouseNormalizer::new().normalize(query).as_result()?;
        let query_string = format!("{}", query);
        let result = self.conn.query(query_string).fetch_all().await?;

        let mut schema = Vec::new();
        let mut arrays = Vec::new();
        for column in result.columns() {
            let field = Field {
                name: column.name().to_string().into(),
                type_: (&column.sql_type()).try_into()?,
                nullable: matches!(column.sql_type(), SqlType::Nullable(_)),
            };
            schema.push(field);

            arrays.push(value::column_to_arrow(column, column.sql_type(), false)?);
        }

        let schema = Arc::new(ArrowSchema::new(try_fields_to_arrow_fields(&schema)?));
        let record_batch = ArrowRecordBatch::try_new(schema.clone(), arrays)?;
        let relation = ArrowRecordBatchRelation::new(schema, Arc::new(vec![record_batch]));
        Ok(relation)
    }

    async fn exec(
        &mut self,
        stmt: &sqlast::Statement,
        params: HashMap<Ident, SQLParam>,
    ) -> Result<()> {
        self.check_params(&params)?;

        let stmt = ClickHouseNormalizer::new().normalize(stmt).as_result()?;
        let query_string = format!("{}", stmt);
        self.conn.execute(query_string).await?;
        Ok(())
    }

    async fn load(
        &mut self,
        table: &sqlast::ObjectName,
        value: Value,
        type_: Type,
        temporary: bool,
    ) -> Result<()> {
        let fields = match type_ {
            Type::List(r) => match r.as_ref() {
                Type::Record(fields) => fields.clone(),
                _ => {
                    return rt_unimplemented!("Loading non-record lists into ClickHouse");
                }
            },
            _ => {
                return rt_unimplemented!("Loading non-record lists into ClickHouse");
            }
        };

        let create_table_stmt = create_table(table.clone(), &fields, temporary)?;
        self.exec(&create_table_stmt, HashMap::new()).await?;

        let relation = match value {
            Value::Relation(relation) => relation,
            _ => {
                return rt_unimplemented!("Loading non-relation values into ClickHouse");
            }
        };

        let table_name = format!("{}", table);
        for batch_idx in 0..relation.num_batches() {
            let batch = relation.batch(batch_idx).as_arrow_recordbatch();
            let mut block = Block::new();
            for (i, field) in fields.iter().enumerate() {
                let column = batch.column(i);
                block = value::arrow_to_column(block, field.name.as_str(), column.as_ref())?;
            }
            self.conn.insert(&table_name, block).await?;
        }

        Ok(())
    }

    /// Ideally, this gets generalized and we use information schema tables. However, there's
    /// no standard way to tell what database we're currently in. We should generalize this function
    /// eventually.
    async fn table_exists(&mut self, name: &sqlast::ObjectName) -> Result<bool> {
        let ident = if name.0.len() == 1 {
            name.0[0].get().value.clone()
        } else {
            return rt_unimplemented!("Multi-part table names in clickhouse: {}", name);
        };
        let escaped_name = ident.replace("'", "\\'");
        let query = format!(
            // In ClickHouse, temporary tables are in the '' database and present across any session database
            "SELECT name FROM system.tables WHERE name = '{escaped_name}' AND (database=currentDatabase() OR database='')"
        );
        Ok(self.conn.query(query).fetch_all().await?.row_count() > 0)
    }

    fn engine_type(&self) -> SQLEngineType {
        SQLEngineType::ClickHouse
    }
}

impl ClickHouseEngine {
    fn check_params(&self, params: &HashMap<Ident, SQLParam>) -> Result<()> {
        for (name, param) in params.iter() {
            match &param.value {
                Value::Relation(r) if r.as_any().downcast_ref::<EmptyRelation>().is_some() => {
                    continue
                }
                Value::Relation(_) => {
                    return rt_unimplemented!("Relation parameters in ClickHouse ({:?})", name,);
                }
                Value::Fn(_) => {
                    return rt_unimplemented!("Function parameters in ClickHouse ({:?})", name);
                }
                _ => {
                    return rt_unimplemented!("Scalar parameters in ClickHouse ({:?})", name);
                }
            }
        }
        Ok(())
    }
}
