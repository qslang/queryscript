use crate::ast::*;
use crate::parser::error::{unexpected_token, Result};
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

    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        self.sqlparser.consume_token(expected)
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
            "import" => {
                panic!("unimplemented");
            }
            "fn" => {
                self.next_token();
                self.parse_fn()?
            }
            "extern" => {
                self.next_token();
                self.parse_extern()?
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

    pub fn parse_ident(&mut self) -> Result<Ident> {
        let ident = as_word(&self.peek_token()).expect("Expected identifier").to_string();
        self.next_token();

        Ok(ident)
    }

    pub fn parse_extern(&mut self) -> Result<StmtBody> {
        // Assume the leading "extern" has already been consumed
        //
        let name = self.parse_ident()?;
        let type_ = self.parse_type()?;

        Ok(StmtBody::Extern{
            name,
            type_,
        })
    }

    pub fn parse_idents(&mut self) -> Result<Vec<Ident>> {
        let mut ret = Vec::new();
        let mut expect_ident = true;
        loop {
            match self.peek_token() {
                Token::Word(w) => {
                    if !expect_ident {
                        break;
                    }

                    ret.push(w.value);
                }
                Token::Comma => {
                    if expect_ident {
                        break;
                    }
                }
                _ => {
                    break;
                }
            }

            expect_ident = !expect_ident;
        }

        Ok(ret)
    }

    pub fn parse_fn(&mut self) -> Result<StmtBody> {
        // Assume the leading "fn" has already been consumed
        //
        let name = self.parse_ident()?;
        let generics = if self.consume_token(&Token::Lt) {
            let list = self.parse_idents()?;
            if !self.consume_token(&Token::Gt) {
                return unexpected_token!(self.peek_token(), "Unexpected token");
            }

            list
        } else {
            Vec::new()
        };

        if !self.consume_token(&Token::LParen) {
            return unexpected_token!(self.peek_token(), "Unexpected token");
        }

        let mut args = Vec::new();
        loop {
            let name = self.parse_ident()?;
            let type_ = match self.peek_token() {
                Token::Comma | Token::RParen => None,
                _ => Some(self.parse_type()?),
            };

            args.push(FnArg{
                name,
                type_,
            });

            match self.next_token() {
                Token::Comma => {},
                Token::RParen => break,
                _ => {
                    return unexpected_token!(self.peek_token(), "Unexpected token");
                }
            }
        }

        let ret = if self.consume_token(&Token::Arrow) {
            Some(self.parse_type()?)
        } else {
            None
        };

        if !self.consume_token(&Token::LBrace) {
            return unexpected_token!(self.peek_token(), "Unexpected token");
        }

        let body = self.parse_expr()?;

        if !self.consume_token(&Token::RBrace) {
            return unexpected_token!(self.peek_token(), "Unexpected token");
        }

        Ok(StmtBody::FnDef {
            name,
            generics,
            args,
            ret,
            body,
        })
    }

    pub fn parse_let(&mut self) -> Result<StmtBody> {
        // Assume the leading "let" or "export" keywords have already been consumed
        //
        let name = self.parse_ident()?;
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
                Token::EOF | Token::SemiColon | Token::Eq | Token::Comma | Token::RParen => {
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
