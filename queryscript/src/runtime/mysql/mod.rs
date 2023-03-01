use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use mysql_async::{prelude::*, Conn, Params, Pool, Row};
use sqlparser::ast as sqlast;

use crate::{
    compile::schema::Ident,
    types::{record::VecRow, Relation, Type, Value},
};

use super::{
    error::rt_unimplemented, normalize::Normalizer, Result, SQLEngine, SQLEnginePool,
    SQLEngineType, SQLParam,
};

mod value;

#[derive(Debug)]
pub struct MySQLEngine {
    pool: Pool,
    conn: Conn,
}

#[async_trait::async_trait]
impl SQLEnginePool for MySQLEngine {
    async fn new(url: Arc<crate::compile::ConnectionString>) -> Result<Box<dyn SQLEngine>> {
        let pool = Pool::from_url(url.get_url().as_str())?;
        let conn = pool.get_conn().await?;
        Ok(Box::new(MySQLEngine { pool, conn }))
    }
}

#[async_trait::async_trait]
impl SQLEngine for MySQLEngine {
    async fn eval(
        &mut self,
        query: &sqlast::Statement,
        params: HashMap<Ident, SQLParam>,
    ) -> Result<Arc<dyn Relation>> {
        let mut scalar_params = Vec::new();

        for (key, param) in params.iter() {
            match &param.value {
                Value::Relation(_) => {
                    return rt_unimplemented!("Relation parameters in MySQL");
                }
                Value::Fn(_) => {
                    return rt_unimplemented!("Function parameters in MySQL");
                }
                _ => {
                    scalar_params.push(key.clone());
                }
            }
        }

        let normalizer = MySQLNormalizer::new(&scalar_params);
        let query = normalizer.normalize(&query).as_result()?;

        let query_string = format!("{}", query);
        let mysql_params = if params.len() > 0 {
            Params::Named(
                params
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            normalizer
                                .params
                                .get(k.as_str())
                                .unwrap()
                                .as_bytes()
                                .to_vec(),
                            v.value.into(),
                        )
                    })
                    .collect(),
            )
        } else {
            Params::Empty
        };

        let result: Vec<value::MySQLRow> = self.conn.exec(query_string, mysql_params).await?;

        let empty_schema = Arc::new(Vec::new());
        let relation = Arc::new(value::MySQLRelation {
            rows: result
                .into_iter()
                .map(|r| VecRow::new(empty_schema.clone(), r.0))
                .collect(),
            schema: empty_schema.clone(),
        });
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
        SQLEngineType::MySQL
    }
}

pub struct MySQLNormalizer {
    params: HashMap<String, String>,
}

impl MySQLNormalizer {
    pub fn new(scalar_params: &[Ident]) -> MySQLNormalizer {
        let params: HashMap<String, String> = scalar_params
            .iter()
            .enumerate()
            .map(|(i, s)| (s.to_string(), format!(":{}", s)))
            .collect();

        MySQLNormalizer { params }
    }
}

impl Normalizer for MySQLNormalizer {
    fn quote_style(&self) -> Option<char> {
        Some('`')
    }

    fn param(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(|s| s.as_str())
    }
}
