use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use mysql_async::{Conn, Pool};
use sqlparser::ast as sqlast;

use crate::{
    compile::schema::Ident,
    types::{Relation, Type, Value},
};

use super::{
    error::rt_unimplemented, normalize::Normalizer, Result, SQLEngine, SQLEnginePool,
    SQLEngineType, SQLParam,
};

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

        let mut normalizer = MySQLNormalizer::new(&scalar_params);
        let query = normalizer.normalize(&query).as_result()?;

        let ordered_params = normalizer.into_ordered_params();

        todo!()
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
    param_names: HashSet<String>,
    param_order: RefCell<Vec<String>>,
}

impl MySQLNormalizer {
    pub fn new(scalar_params: &[Ident]) -> MySQLNormalizer {
        MySQLNormalizer {
            param_names: scalar_params.iter().map(|s| s.to_string()).collect(),
            param_order: RefCell::new(Vec::new()),
        }
    }

    pub fn into_ordered_params(&self) -> Vec<String> {
        self.param_order.into_inner()
    }
}

impl Normalizer for MySQLNormalizer {
    fn quote_style(&self) -> Option<char> {
        Some('`')
    }

    fn param(&self, key: &str) -> Option<&str> {
        // This is a really hacky (but somewhat sound) assumption that
        // the order that we traverse the tree is the same as the order
        // we print the operators
        if self.param_names.contains(key) {
            self.param_order.borrow_mut().push(key.to_string());
            Some("?")
        } else {
            None
        }
    }
}
