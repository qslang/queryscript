use sqlparser::ast as sqlast;
use std::collections::HashMap;

pub mod context;
pub mod datafusion;
pub mod error;
pub mod types;

use types::{Type, Value};

trait SQLRuntime {
    fn eval(query: &sqlast::Query, params: &HashMap<String, SQLParam>);
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
}
