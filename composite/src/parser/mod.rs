use crate::ast::*;
use sqlparser::{
    // ast::{ColumnDef, ColumnOptionDef, Statement as SQLStatement, TableConstraint},
    dialect::{GenericDialect},
    // parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};

pub struct Parser {
    tokens: &[Token],
    i: usize,
}

impl Parser {
    pub fn new(tokens: &[Token]) -> Parser {
        let mut r = Parser{
            tokens,
            i: 0,
        }
        r.advance(0);
        return r;
    }

    pub fn advance(&mut self, num: usize) {
        self.i += num;
        while {
            match self.current_token() {
                Token::Whitespace(_) => self.i += 1,
                _ => break;
            }
        }
    }

    pub fn current_token(&mut self) -> &Token {
        &self.tokens[self.i]
    }

    pub fn parse_schema(&mut self) -> Schema {
    }

    pub fn parse_stmt(&mut self) -> Stmt {
        let export = as_word(self.current_token()).expect("Statements must begin with words") == "export";
        if export {
            self.advance(1);
        }

        let word = as_word(self.current_token());
        match w.value.to_uppercase() {
            "import" => { }
            "let" => { }
            _ => { }
            "extern" => { }
            "type" => { }
            "fn" => { }
        }
    }
}

pub fn as_word(token: &Token) -> Option<&str> {
    match token {
        Token::Word(w) => {
            Some(w.value.to_uppercase())
        }
        _ => {
            None
        }
    }
}

pub fn tokenize(text: &str) -> Vec<Token> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, text);

    tokenizer.tokenize().expect("")
}

pub fn parse(text: &str) -> Schema {
    let tokens = tokenize(text);
    let parser = Parser::new(tokens);

    parser.parse_schema()
}

// pub fn parse_sql(text: &str) {
//     let dialect = &GenericDialect {};
//     DFParser::new_with_dialect(sql, dialect).unwrap()
// }
