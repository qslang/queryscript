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
        let mut stmts = Vec::new();
        while !matches!(self.current_token(), Token::EOF) {
            stmts.push(self.parse_stmt());
        }

        Schema {
            stmts,
        }
    }

    pub fn parse_stmt(&mut self) -> Stmt {
        let export = as_word(self.current_token()).expect("Statements must begin with words") == "export";
        if export {
            self.advance(1);
        }

        let mut needs_semicolon = true;
        let word = as_word(self.current_token());
        let mut stmt = match word {
            Some(w) => match w.value.to_lowercase() {
                "import" => {
                    self.parse_import();
                }
                "extern" => {
                    self.advance(1);
                    let mut stmt = self.parse_let();
                    stmt.extern = true;

                    stmt
                }
                "type" => {
                    needs_semicolon = false;
                    panic!("unimplemented");
                }
                "fn" => {
                    self.parse_fn()
                    panic!("unimplemented");
                }
                "let" => {
                    self.advance(1);
                    self.parse_let()
                }
                _ => {
                    if export {
                        self.parse_let()
                    } else {
                        panic!("Unexpected keyword {}", w.value)
                    }
                }
            }
        };

        stmt.export = export;

        if needs_semicolon {
            match self.current_token() {
                Token::EOF => {},
                Token::SemiColon => {
                    self.advance(1);
                },
                _ => {
                    panic!("Unexpected token {}", self.current_token());
                },
            }
        }

        return stmt;
    }

    pub parse_let(&mut self) -> Stmt {
        // Assume the leading keywords have already been consumed
        //
        let name = as_word(self.current_token()).expect("Expected identifier").to_string();
        self.advance(1);

        let type_ = match self.current_token() {
            Token::Eq => {
                None
            }
            _ => {
                Some(self.parse_type())
            }
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
