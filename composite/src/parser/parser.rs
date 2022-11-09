use crate::ast::*;
use sqlparser::{
    // ast::{ColumnDef, ColumnOptionDef, Statement as SQLStatement, TableConstraint},
    dialect::{GenericDialect},
    parser,
    tokenizer::{Token, Tokenizer},
};
use snafu::{OptionExt};
use crate::parser::error::{Result, UnexpectedTokenSnafu};

pub struct Parser<'a> {
    sqlparser: parser::Parser<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: Vec<Token>) -> Parser<'a> {
        let dialect = &GenericDialect {};
        Parser{
            sqlparser: parser::Parser::new(tokens, dialect),
        }
    }

    pub fn next_token(&mut self) -> Token {
        self.sqlparser.next_token()
    }

    pub fn peek_token(&mut self) -> Token {
        self.sqlparser.peek_token()
    }

    pub fn parse_schema(&mut self) -> Result<Schema> {
        let mut stmts = Vec::new();
        while !matches!(self.peek_token(), Token::EOF) {
            stmts.push(self.parse_stmt()?);
        }

        Ok(Schema {
            stmts,
        })
    }

    pub fn parse_stmt(&mut self) -> Result<Stmt> {
        let token = self.peek_token();
        let export = as_word(&token).context(UnexpectedTokenSnafu{
            token: token.clone(),
            msg: "expecting a word",
        })? == "export";
        if export {
            self.next_token();
        }

        let word = as_word(&self.peek_token());
        let body = match word {
            Some(w) => match w.to_lowercase().as_str() {
                "import" | "extern" | "type" | "fn" => {
                    panic!("unimplemented");
                }
                "let" => {
                    self.next_token();
                    self.parse_let()?
                }
                _ => {
                    if export {
                        self.parse_let()?
                    } else {
                        panic!("Unexpected keyword {}", w)
                    }
                }
            }
            None => {
                panic!("Expected keyword");
            }
        };

        match self.peek_token() {
            Token::SemiColon => {
                self.next_token();
            },
            _ => {
                panic!("Unexpected token {}", self.peek_token());
            },
        }

        Ok(Stmt{
            export,
            body,
        })
    }

    pub fn parse_let(&mut self) -> Result<StmtBody> {
        // Assume the leading keywords have already been consumed
        //
        let name = as_word(&self.peek_token()).expect("Expected identifier").to_string();
        self.next_token();

        let type_ = match self.peek_token() {
            Token::Eq => {
                None
            }
            _ => {
                Some(self.parse_type()?)
            }
        };

        let body = match self.peek_token() {
            Token::Eq => {
                self.next_token();
                self.parse_expr()?
            }
            _ => {
                panic!("Expected definition");
            }
        };

        Ok(StmtBody::Let{
            name,
            type_,
            body,
        })
    }

    pub fn parse_type(&mut self) -> Result<Type> {
        let mut tokens = Vec::new();
        loop {
            match self.peek_token() {
                Token::EOF | Token::SemiColon | Token::Eq => {
                    break;
                }
                _ => {
                    tokens.push(self.peek_token().clone());
                }
            }
            self.next_token();
        }

        Ok(Type::TODO(tokens))
    }

    pub fn parse_expr(&mut self) -> Result<Expr> {
        let mut tokens = Vec::new();
        loop {
            match self.peek_token() {
                Token::EOF | Token::SemiColon => {
                    break;
                }
                _ => {
                    tokens.push(self.peek_token().clone());
                }
            }
            self.next_token();
        }

        Ok(Expr::TODO(tokens))
    }
}

pub fn as_word(token: &Token) -> Option<String> {
    match token {
        Token::Word(w) => {
            Some(w.value.clone())
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

pub fn parse(text: &str) -> Result<Schema> {
    let tokens = tokenize(text);
    let mut parser = Parser::new(tokens);

    parser.parse_schema()
}

// pub fn parse_sql(text: &str) {
//     let dialect = &GenericDialect {};
//     DFParser::new_with_dialect(sql, dialect).unwrap()
// }
