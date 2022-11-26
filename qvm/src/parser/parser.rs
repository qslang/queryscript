use crate::ast::*;
use crate::parser::error::{unexpected_token, wrap_sql_eof, IncompleteSnafu, Result};
use sqlparser::{
    dialect::{keywords::Keyword, GenericDialect},
    parser,
    tokenizer::{Token, TokenWithLocation, Tokenizer, Word},
};

pub struct Parser<'a> {
    sqlparser: parser::Parser<'a>,
    eof: Location,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: Vec<TokenWithLocation>, eof: Location) -> Parser<'a> {
        let dialect = &GenericDialect {};
        Parser {
            sqlparser: parser::Parser::new_with_locations(tokens, dialect),
            eof,
        }
    }

    pub fn next_token(&mut self) -> TokenWithLocation {
        self.sqlparser.next_token()
    }

    pub fn peek_token(&mut self) -> TokenWithLocation {
        self.sqlparser.peek_token()
    }

    pub fn reset(&mut self) {
        self.sqlparser.index = 0;
    }

    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        self.sqlparser.consume_token(expected)
    }

    pub fn expect_token(&mut self, expected: &Token) -> Result<()> {
        Ok(self.sqlparser.expect_token(expected)?)
    }

    // This returns a slightly different error code that indicates that we were
    // expecting the statement to end, so we can handle it in the REPL.
    pub fn expect_eos(&mut self) -> Result<()> {
        match self.expect_token(&Token::SemiColon) {
            Ok(_) => Ok(()),
            Err(e) => IncompleteSnafu {
                original: Box::new(e),
            }
            .fail(),
        }
    }

    pub fn peek_keyword(&mut self, expected: &str) -> bool {
        match self.peek_token().token {
            Token::Word(w) => {
                if w.value.to_lowercase() == expected.to_lowercase() {
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }
    pub fn consume_keyword(&mut self, expected: &str) -> bool {
        if self.peek_keyword(expected) {
            self.next_token();
            return true;
        }
        return false;
    }

    pub fn expect_keyword(&mut self, expected: &str) -> Result<()> {
        if self.consume_keyword(expected) {
            Ok(())
        } else {
            let token = self.peek_token();
            Ok(self.sqlparser.expected::<()>(expected, token)?)
        }
    }

    #[must_use]
    fn maybe_parse<T, F>(&mut self, mut f: F) -> Option<T>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T>,
    {
        let index = self.sqlparser.index;
        if let Ok(t) = f(self) {
            Some(t)
        } else {
            self.sqlparser.index = index;
            None
        }
    }

    fn start_of_current(&mut self) -> Location {
        self.peek_token().location
    }

    fn end_of_current(&mut self) -> Location {
        let i = self.sqlparser.index;
        let mut end = self
            .sqlparser
            .next_token_no_skip()
            .unwrap_or(&TokenWithLocation {
                token: Token::EOF,
                location: self.eof.clone(),
            })
            .location
            .clone();
        self.sqlparser.index = i;

        // It's always safe to assume the token immediately following a non-whitespace token is
        // after the start of the line, since newlines count as tokens.
        //
        end.column -= 1;

        return end;
    }

    pub fn parse_schema(&mut self) -> Result<Schema> {
        let mut stmts = Vec::new();
        while !matches!(self.peek_token().token, Token::EOF) {
            stmts.push(self.parse_stmt()?);
        }

        Ok(Schema { stmts })
    }

    pub fn parse_stmt(&mut self) -> Result<Stmt> {
        let start = self.start_of_current();
        if self.consume_token(&Token::SemiColon) {
            return Ok(Stmt {
                export: false,
                body: StmtBody::Noop,
                start: start.clone(),
                end: start.clone(),
            });
        }

        let export = self.consume_keyword("export");
        let body = if self.consume_keyword("fn") {
            self.parse_fn()?
        } else if self.consume_keyword("extern") {
            self.parse_extern()?
        } else if self.consume_keyword("let") {
            self.parse_let()?
        } else if self.consume_keyword("type") {
            self.parse_typedef()?
        } else if self.consume_keyword("import") || export {
            self.parse_import()?
        } else if self.peek_keyword("select") {
            self.parse_query()?
        } else {
            self.parse_expr_stmt()?
        };

        let end = self.end_of_current();

        Ok(Stmt {
            export,
            body,
            start,
            end,
        })
    }

    pub fn parse_ident(&mut self) -> Result<Ident> {
        let token = self.next_token();
        let location = token.location;
        match token.token {
            Token::Word(w) => Ok(w.value),
            Token::DoubleQuotedString(s) => Ok(s),
            token => unexpected_token!(
                TokenWithLocation { token, location },
                "Expected: WORD | DOUBLE_QUOTED_STRING"
            ),
        }
    }

    pub fn parse_path(&mut self) -> Result<Path> {
        let mut path = Vec::new();
        loop {
            path.push(self.parse_ident()?);
            match self.peek_token().token {
                Token::Period => {
                    self.next_token();
                }
                _ => {
                    break;
                }
            }
        }
        Ok(path)
    }

    pub fn parse_extern(&mut self) -> Result<StmtBody> {
        // Assume the leading "extern" has already been consumed
        //
        let name = self.parse_ident()?;
        let type_ = self.parse_type()?;
        self.expect_eos()?;

        Ok(StmtBody::Extern { name, type_ })
    }

    pub fn parse_idents(&mut self) -> Result<Vec<Ident>> {
        let mut ret = Vec::new();
        let mut expect_ident = true;
        loop {
            if expect_ident {
                if let Ok(ident) = self.parse_ident() {
                    ret.push(ident);
                } else {
                    break;
                }
            } else {
                match self.peek_token().token {
                    Token::Comma => {}
                    _ => break,
                }

                self.next_token();
            }

            expect_ident = !expect_ident;
        }

        Ok(ret)
    }

    pub fn parse_simple_import(&mut self) -> Result<StmtBody> {
        let path = self.parse_path()?;
        self.expect_eos()?;
        Ok(StmtBody::Import {
            path,
            list: ImportList::None,
            args: None,
        })
    }

    pub fn parse_import(&mut self) -> Result<StmtBody> {
        if let Some(stmt) = self.maybe_parse(Parser::parse_simple_import) {
            return Ok(stmt);
        }

        let list = if self.consume_token(&Token::Mul) {
            ImportList::Star
        } else {
            ImportList::Items(
                self.parse_idents()?
                    .iter()
                    .map(|x| vec![x.clone()])
                    .collect(),
            )
        };
        self.expect_keyword("from")?;

        let path = self.parse_path()?;
        let args = if self.consume_token(&Token::LBrace) {
            let mut args = Vec::new();
            loop {
                let name = self.parse_ident()?;
                let expr = if self.consume_token(&Token::Colon) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };

                args.push(NameAndExpr { name, expr });

                if !self.consume_token(&Token::Comma) {
                    self.expect_token(&Token::RBrace)?;
                    break;
                }
                if self.consume_token(&Token::RBrace) {
                    break;
                }
            }

            Some(args)
        } else {
            None
        };

        self.expect_eos()?;

        Ok(StmtBody::Import { path, list, args })
    }

    pub fn parse_fn(&mut self) -> Result<StmtBody> {
        // Assume the leading "fn" has already been consumed
        //
        let name = self.parse_ident()?;
        let generics = if self.consume_token(&Token::Lt) {
            let list = self.parse_idents()?;
            self.expect_token(&Token::Gt)?;

            list
        } else {
            Vec::new()
        };

        self.expect_token(&Token::LParen)?;

        let args = if self.consume_token(&Token::RParen) {
            Vec::new()
        } else {
            let mut args = Vec::new();
            loop {
                let name = self.parse_ident()?;
                let type_ = self.parse_type()?;

                args.push(FnArg { name, type_ });

                match self.next_token().token {
                    Token::Comma => {}
                    Token::RParen => break args,
                    _ => {
                        return unexpected_token!(self.peek_token(), "Expected: ',' | ')'");
                    }
                }
            }
        };

        let ret = if self.consume_token(&Token::Arrow) {
            Some(self.parse_type()?)
        } else {
            None
        };

        self.expect_token(&Token::LBrace)?;

        let body = self.parse_expr()?;

        self.expect_token(&Token::RBrace)?;

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
        let type_ = match self.peek_token().token {
            Token::Eq => None,
            _ => Some(self.parse_type()?),
        };

        let body = match self.peek_token().token {
            Token::Eq => {
                self.next_token();
                self.parse_expr()?
            }
            _ => {
                return unexpected_token!(self.peek_token(), "Expected definition");
            }
        };

        self.expect_eos()?;

        Ok(StmtBody::Let { name, type_, body })
    }

    pub fn parse_query(&mut self) -> Result<StmtBody> {
        let query = wrap_sql_eof(self.sqlparser.parse_query())?;
        self.expect_eos()?;

        Ok(StmtBody::Expr(Expr::SQLQuery(query)))
    }

    pub fn parse_expr_stmt(&mut self) -> Result<StmtBody> {
        let expr = wrap_sql_eof(self.sqlparser.parse_expr())?;
        self.expect_eos()?;

        Ok(StmtBody::Expr(Expr::SQLExpr(expr)))
    }

    pub fn parse_typedef(&mut self) -> Result<StmtBody> {
        // Assume the leading keywords have already been consumed
        //
        let name = self.parse_ident()?;
        let def = self.parse_type()?;
        match def {
            Type::Struct(_) => {}
            _ => self.expect_eos()?,
        }
        Ok(StmtBody::TypeDef(NameAndType { name, def }))
    }

    pub fn parse_type(&mut self) -> Result<Type> {
        let mut type_ = if self.consume_token(&Token::LBracket) {
            let inner = self.parse_type()?;
            self.expect_token(&Token::RBracket)?;
            Type::List(Box::new(inner))
        } else if self.peek_token().token == Token::LBrace {
            self.parse_struct()?
        } else {
            Type::Reference(self.parse_path()?)
        };

        if self.consume_keyword("exclude") {
            let excluded = self.parse_idents()?;
            type_ = Type::Exclude {
                inner: Box::new(type_),
                excluded,
            };
        }

        Ok(type_)
    }

    pub fn parse_struct(&mut self) -> Result<Type> {
        self.expect_token(&Token::LBrace)?;
        let mut struct_ = Vec::new();
        let mut needs_comma = false;
        loop {
            match self.peek_token().token {
                Token::RBrace => {
                    self.next_token();
                    break;
                }
                Token::Comma => {
                    if needs_comma {
                        needs_comma = false;
                    } else {
                        return unexpected_token!(self.peek_token(), "Two consecutive commas");
                    }
                    self.next_token();
                }
                Token::Period => {
                    for _ in 0..3 {
                        if !matches!(self.peek_token().token, Token::Period) {
                            return unexpected_token!(self.peek_token(), "Three periods");
                        }
                        self.next_token();
                    }
                    struct_.push(StructEntry::Include(self.parse_path()?));
                    needs_comma = true;
                }
                _ => {
                    if needs_comma {
                        return unexpected_token!(
                            self.peek_token(),
                            "Expected a comma before the next type declaration"
                        );
                    }
                    let name = self.parse_ident()?;
                    let def = self.parse_type()?;
                    struct_.push(StructEntry::NameAndType(NameAndType { name, def }));
                    needs_comma = true;
                }
            }
        }
        Ok(Type::Struct(struct_))
    }

    pub fn parse_expr(&mut self) -> Result<Expr> {
        Ok(match self.peek_token().token {
            Token::Word(Word {
                value: _,
                quote_style: _,
                keyword: Keyword::SELECT | Keyword::WITH,
            }) => Expr::SQLQuery(self.sqlparser.parse_query()?),
            _ => Expr::SQLExpr(self.sqlparser.parse_expr()?),
        })
    }
}

pub fn tokenize(text: &str) -> Result<(Vec<TokenWithLocation>, Location)> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, text);

    Ok(tokenizer.tokenize_with_location()?)
}

pub fn parse_schema(text: &str) -> Result<Schema> {
    let (tokens, eof) = tokenize(text)?;
    let mut parser = Parser::new(tokens, eof);

    parser.parse_schema()
}
