use crate::ast::*;
use crate::parser::error::{unexpected_token, Result};
use sqlparser::{
    ast as sqlast,
    dialect::{keywords::Keyword, GenericDialect},
    parser,
    tokenizer::{TokenWithLocation, Tokenizer},
};

pub use sqlparser::tokenizer::Location;
pub use sqlparser::tokenizer::Token;
pub use sqlparser::tokenizer::Word;

// In order to communicate semantic context to the autocompleter, we encode different kinds of
// information into the `quote_style` field of `Word`.  These special placeholders indicate various
// special tokens.
//
pub const AUTOCOMPLETE_UNKNOWN: char = 'u'; // Unknown
pub const AUTOCOMPLETE_KEYWORD: char = 'k'; // A keyword
pub const AUTOCOMPLETE_ALIAS: char = 'v'; // A new name, which shouldn't be autocompleted
pub const AUTOCOMPLETE_VARIABLE: char = 'v'; // A variable name
pub const AUTOCOMPLETE_TYPE: char = 't'; // A type name
pub const AUTOCOMPLETE_SCHEMA: char = 's'; // A schema

pub struct Parser<'a> {
    sqlparser: parser::Parser<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: Vec<TokenWithLocation>, eof: Location) -> Parser<'a> {
        let dialect = &GenericDialect {};
        Parser {
            sqlparser: parser::Parser::new_with_locations(tokens, eof, dialect),
        }
    }

    pub fn next_token(&mut self) -> TokenWithLocation {
        self.sqlparser.next_token()
    }

    pub fn peek_token(&self) -> TokenWithLocation {
        self.sqlparser.peek_token()
    }

    pub fn peek_start_location(&self) -> Location {
        self.sqlparser.peek_start_location()
    }

    pub fn peek_end_location(&self) -> Location {
        self.sqlparser.peek_end_location()
    }

    pub fn is_eof(&self) -> bool {
        self.peek_token().token == Token::EOF
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
        self.expect_token(&Token::SemiColon)
    }

    pub fn peek_keyword(&mut self, expected: &str) -> bool {
        self.sqlparser
            .autocomplete_tokens(&[Token::make_word(expected, Some(AUTOCOMPLETE_KEYWORD))]);
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
        let i = self.sqlparser.current_index();
        if let Ok(t) = f(self) {
            Some(t)
        } else {
            self.sqlparser.reset(i);
            None
        }
    }

    pub fn autocomplete_tokens(&mut self, tokens: &[Token]) {
        self.sqlparser.autocomplete_tokens(tokens);
    }

    pub fn get_autocomplete(&self, loc: Location) -> (TokenWithLocation, Vec<Token>) {
        self.sqlparser.get_autocomplete(loc)
    }

    pub fn get_autocompletes(&self) -> Vec<Vec<Token>> {
        self.sqlparser.get_autocompletes()
    }
    pub fn parse_schema(&mut self) -> Result<Schema> {
        let mut stmts = Vec::new();
        while !matches!(self.peek_token().token, Token::EOF) {
            stmts.push(self.parse_stmt()?);
        }

        Ok(Schema { stmts })
    }

    pub fn parse_stmt(&mut self) -> Result<Stmt> {
        let start = self.peek_start_location();
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

        let end = self.peek_end_location();

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

    pub fn parse_path(&mut self, kind: char) -> Result<Path> {
        let mut path = Vec::<Ident>::new();
        loop {
            self.autocomplete_tokens(&[Token::make_word(
                sqlast::ObjectName(
                    path.iter()
                        .map(|p| sqlast::Ident {
                            value: p.clone(),
                            quote_style: Some('\"'),
                        })
                        .collect(),
                )
                .to_string()
                .as_str(),
                Some(kind),
            )]);
            path.push(self.parse_ident()?);
            self.autocomplete_tokens(&[Token::Period]);
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
                self.autocomplete_tokens(&[Token::make_ident("")]);
                if let Ok(ident) = self.parse_ident() {
                    ret.push(ident);
                } else {
                    break;
                }
            } else {
                self.autocomplete_tokens(&[Token::Comma]);
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
        let path = self.parse_path(AUTOCOMPLETE_SCHEMA)?;
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

        let path = self.parse_path(AUTOCOMPLETE_SCHEMA)?;
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

                self.autocomplete_tokens(&[Token::Comma, Token::RParen]);
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

        let body = if self.consume_token(&Token::Eq) {
            if self.consume_keyword("native") {
                FnBody::Native
            } else if self.consume_keyword("sql") {
                FnBody::SQL
            } else {
                return unexpected_token!(self.peek_token(), "Expected: native | sql");
            }
        } else {
            self.expect_token(&Token::LBrace)?;

            let body = self.parse_expr()?;

            self.expect_token(&Token::RBrace)?;
            FnBody::Expr(body)
        };

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
        self.autocomplete_tokens(&[Token::Eq]);
        let type_ = match self.peek_token().token {
            Token::Eq => None,
            _ => Some(self.parse_type()?),
        };

        self.autocomplete_tokens(&[Token::Eq]);
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
        let start = self.peek_start_location();
        let query = self.sqlparser.parse_query()?;
        self.expect_eos()?;
        let end = self.peek_end_location();

        Ok(StmtBody::Expr(Expr {
            body: ExprBody::SQLQuery(query),
            start,
            end,
        }))
    }

    pub fn parse_expr_stmt(&mut self) -> Result<StmtBody> {
        let start = self.peek_start_location();
        let expr = self.sqlparser.parse_expr()?;
        self.expect_eos()?;
        let end = self.peek_end_location();

        Ok(StmtBody::Expr(Expr {
            body: ExprBody::SQLExpr(expr),
            start,
            end,
        }))
    }

    pub fn parse_typedef(&mut self) -> Result<StmtBody> {
        // Assume the leading keywords have already been consumed
        //
        let name = self.parse_ident()?;
        let def = self.parse_type()?;
        match def.body {
            TypeBody::Struct(_) => {}
            _ => self.expect_eos()?,
        }
        Ok(StmtBody::TypeDef(NameAndType { name, def }))
    }

    pub fn parse_type(&mut self) -> Result<Type> {
        let start = self.peek_start_location();
        self.autocomplete_tokens(&[Token::LBrace]);
        let mut body = if self.consume_token(&Token::LBracket) {
            let inner = self.parse_type()?;
            self.expect_token(&Token::RBracket)?;
            TypeBody::List(Box::new(inner))
        } else if self.peek_token().token == Token::LBrace {
            self.parse_struct()?
        } else {
            TypeBody::Reference(self.parse_path(AUTOCOMPLETE_TYPE)?)
        };

        if self.consume_keyword("exclude") {
            let excluded = self.parse_idents()?;
            let end = self.peek_end_location();
            body = TypeBody::Exclude {
                inner: Box::new(Type {
                    body,
                    start: start.clone(),
                    end,
                }),
                excluded,
            };
        }

        let end = self.peek_end_location();

        Ok(Type { body, start, end })
    }

    pub fn parse_struct(&mut self) -> Result<TypeBody> {
        self.expect_token(&Token::LBrace)?;
        let mut struct_ = Vec::new();
        let mut needs_comma = false;
        loop {
            self.autocomplete_tokens(&[Token::RBrace, Token::Comma, Token::Period]);
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
                        self.autocomplete_tokens(&[Token::Period]);
                        if !matches!(self.peek_token().token, Token::Period) {
                            return unexpected_token!(self.peek_token(), "Three periods");
                        }
                        self.next_token();
                    }
                    struct_.push(StructEntry::Include(self.parse_path(AUTOCOMPLETE_TYPE)?));
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
        Ok(TypeBody::Struct(struct_))
    }

    pub fn parse_expr(&mut self) -> Result<Expr> {
        let start = self.peek_start_location();
        self.sqlparser
            .autocomplete_tokens(&[Token::make_keyword("SELECT"), Token::make_keyword("WITH")]);
        Ok(match self.peek_token().token {
            Token::Word(Word {
                value: _,
                quote_style: _,
                keyword: Keyword::SELECT | Keyword::WITH,
            }) => {
                let query = self.sqlparser.parse_query()?;
                let end = self.peek_end_location();
                Expr {
                    body: ExprBody::SQLQuery(query),
                    start,
                    end,
                }
            }
            _ => {
                let expr = self.sqlparser.parse_expr()?;
                let end = self.peek_end_location();
                Expr {
                    body: ExprBody::SQLExpr(expr),
                    start,
                    end,
                }
            }
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

pub fn parse_path(text: &str) -> Result<Path> {
    let (tokens, eof) = tokenize(text)?;
    let mut parser = Parser::new(tokens, eof);

    parser.parse_path(AUTOCOMPLETE_UNKNOWN)
}
