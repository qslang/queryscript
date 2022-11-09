use crate::ast::*;
use sqlparser::{
    // ast::{ColumnDef, ColumnOptionDef, Statement as SQLStatement, TableConstraint},
    dialect::{GenericDialect},
    // parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};

pub struct Parser {
}

impl Parser {
    pub fn new() -> Parser {
        Parser{}
    }

    pub fn parse_schema(&self, tokens: &[Token]) -> Schema {
        Schema::TODO(tokens.iter().map(|x| x.clone()).collect())
    }
}

pub fn tokenize(text: &str) -> Vec<Token> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, text);

    tokenizer.tokenize().expect("")
}

pub fn parse(text: &str) -> Schema {
    let tokens = tokenize(text);
    let parser = Parser::new();

    parser.parse_schema(&tokens)
}

// pub fn parse_sql(text: &str) {
//     let dialect = &GenericDialect {};
//     DFParser::new_with_dialect(sql, dialect).unwrap()
// }
