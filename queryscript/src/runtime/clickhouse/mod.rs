use sqlparser::ast as sqlast;
use std::fmt;

use clickhouse_rs::{ClientHandle, Pool};
use std::{collections::HashMap, sync::Arc};

use crate::{
    ast::Ident,
    compile::ConnectionString,
    types::{record::VecRow, Relation, Type, Value},
};

use super::{error::rt_unimplemented, Result, SQLEngine, SQLEnginePool, SQLEngineType, SQLParam};

pub struct ClickHouseEngine {
    pool: Pool,
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
        let url = url.get_url();
        // NOTE: We could accept an "is_secure" flag in the connection string
        let mut http_url = format!("https://{}", url.host_str().unwrap());
        if let Some(port) = url.port() {
            http_url.push_str(&format!(":{}", port));
        }
        let mut client = Client::default().with_url(http_url.as_str());

        if url.username().len() > 0 {
            client = client.with_user(url.username());
        }

        if let Some(password) = url.password() {
            client = client.with_password(password);
        }

        if let Some(database) = url.path().strip_prefix("/") {
            client = client.with_database(database);
        }

        Ok(Box::new(ClickHouseEngine { conn: client }))
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
        self.conn.query()
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
