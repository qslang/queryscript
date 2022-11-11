use crate::ast::Type;
use std::collections::BTreeMap;

pub struct Schema {
    pub types: BTreeMap<String, Type>,
}

pub fn from_string(contents: &str) -> Result<Schema> {}
