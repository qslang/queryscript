use crate::ast::*;
use crate::parser::error::{unexpected_token, Result};
use snafu::OptionExt;
use sqlparser::{
    // ast::{ColumnDef, ColumnOptionDef, Statement as SQLStatement, TableConstraint},
    dialect::{keywords::Keyword, GenericDialect},
    parser,
    tokenizer::{Token, Tokenizer, Word},
};

pub struct Parser<'a> {
    sqlparser: parser::Parser<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: Vec<Token>) -> Parser<'a> {
        let dialect = &GenericDialect {};
        Parser {
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

        Ok(Schema { stmts })
    }

    pub fn parse_stmt(&mut self) -> Result<Stmt> {
        let token = self.peek_token();
        let export = as_word(&token)? == "export";
        if export {
            self.next_token();
        }

        let word = as_word(&self.peek_token())?;
        let body = match word.to_lowercase().as_str() {
            "import" | "extern" | "fn" => {
                panic!("unimplemented");
            }
            "let" => {
                self.next_token();
                self.parse_let()?
            }
            "type" => {
                self.next_token();
                self.parse_typedef()?
            }
            _ => {
                if export {
                    self.parse_let()?
                } else {
                    return unexpected_token!(self.peek_token(), "Unexpected keyword");
                }
            }
        };

        match self.peek_token() {
            Token::SemiColon => {
                self.next_token();
            }
            t => {
                return unexpected_token!(t, "Expecting a semi-colon");
            }
        }

        Ok(Stmt { export, body })
    }

    pub fn parse_let(&mut self) -> Result<StmtBody> {
        // Assume the leading keywords have already been consumed
        //
        let name = as_word(&self.peek_token())?.to_string();
        self.next_token();

        let type_ = match self.peek_token() {
            Token::Eq => None,
            _ => Some(self.parse_type()?),
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

        Ok(StmtBody::Let { name, type_, body })
    }

    pub fn parse_typedef(&mut self) -> Result<StmtBody> {
        // Assume the leading keywords have already been consumed
        //
        let name = as_word(&self.peek_token())?.to_string();
        self.next_token();

        let type_ = match self.peek_token() {
            Token::Eq => None,
            _ => Some(self.parse_type()?),
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

        Ok(StmtBody::Let { name, type_, body })
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
        Ok(match self.peek_token() {
            Token::Word(Word {
                value: _,
                quote_style: _,
                keyword: Keyword::SELECT,
            }) => Expr::SQLQuery(self.sqlparser.parse_query()?),
            _ => Expr::SQLExpr(self.sqlparser.parse_expr()?),
        })
    }
}

pub fn as_word(token: &Token) -> Result<String> {
    match token {
        Token::Word(w) => Ok(w.value.clone()),
        _ => unexpected_token!(token, "expecting a word"),
    }
}

pub fn tokenize(text: &str) -> Vec<Token> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, text);

    // XXX Return a better error if the tokenizer fails
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
