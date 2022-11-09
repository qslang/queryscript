use crate::ast::*;
use sqlparser::{
    // ast::{ColumnDef, ColumnOptionDef, Statement as SQLStatement, TableConstraint},
    dialect::{GenericDialect},
    // parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use snafu::{OptionExt};
use crate::parser::error::{Result, UnexpectedTokenSnafu};

pub struct Parser {
    tokens: Vec<Token>,
    i: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Parser {
        println!("{:?}", tokens);
        let mut r = Parser{
            tokens,
            i: 0,
        };
        r.advance(0);
        return r;
    }

    pub fn advance(&mut self, num: usize) {
        self.i += num;
        loop {
            match self.current_token() {
                Token::Whitespace(_) => self.i += 1,
                _ => break,
            };
        }
    }

    pub fn current_token(&mut self) -> &Token {
        if self.i < self.tokens.len() {
            &self.tokens[self.i]
        } else {
            &Token::EOF
        }
    }

    pub fn parse_schema(&mut self) -> Result<Schema> {
        let mut stmts = Vec::new();
        while !matches!(self.current_token(), Token::EOF) {
            stmts.push(self.parse_stmt()?);
        }

        Ok(Schema {
            stmts,
        })
    }

    pub fn parse_stmt(&mut self) -> Result<Stmt> {
        let token = self.current_token();
        let export = as_word(token).context(UnexpectedTokenSnafu{
            token: token.clone(),
            msg: "expecting a word",
        })? == "export";
        if export {
            self.advance(1);
        }

        let word = as_word(self.current_token());
        let body = match word {
            Some(w) => match w.to_lowercase().as_str() {
                "import" => {
                    panic!("unimplemented");
                }
                "extern" => {
                    panic!("unimplemented");
                }
                "type" => {
                    panic!("unimplemented");
                }
                "fn" => {
                    panic!("unimplemented");
                }
                "let" => {
                    self.advance(1);
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

        match self.current_token() {
            Token::SemiColon => {
                self.advance(1);
            },
            _ => {
                panic!("Unexpected token {}", self.current_token());
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
        let name = as_word(self.current_token()).expect("Expected identifier").to_string();
        self.advance(1);

        let type_ = match self.current_token() {
            Token::Eq => {
                None
            }
            _ => {
                Some(self.parse_type()?)
            }
        };

        let body = match self.current_token() {
            Token::Eq => {
                self.advance(1);
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
            match self.current_token() {
                Token::EOF => {
                    break;
                }
                Token::SemiColon => {
                    break;
                }
                Token::Eq => {
                    break;
                }
                _ => {
                    tokens.push(self.current_token().clone());
                }
            }
            self.advance(1);
        }

        Ok(Type::TODO(tokens))
    }

    pub fn parse_expr(&mut self) -> Result<Expr> {
        let mut tokens = Vec::new();
        loop {
            match self.current_token() {
                Token::EOF => {
                    break;
                }
                Token::SemiColon => {
                    break;
                }
                _ => {
                    tokens.push(self.current_token().clone());
                }
            }
            self.advance(1);
        }

        Ok(Expr::TODO(tokens))
    }
}

pub fn as_word(token: &Token) -> Option<&str> {
    match token {
        Token::Word(w) => {
            Some(w.value.as_str())
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
