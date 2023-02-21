use crate::ast::*;
use crate::c_try;
use crate::error::MultiResult;
use crate::parser::error::{
    unexpected_token, ErrorLocation, ParserError, Result, SQLParserSnafu, TokenizerSnafu,
};
use snafu::prelude::*;
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

type ParseResult<T> = MultiResult<T, ParserError>;

pub struct Parser<'a> {
    file: String,
    sqlparser: parser::Parser<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(file: &str, tokens: Vec<TokenWithLocation>, eof: Location) -> Parser<'a> {
        let dialect = &GenericDialect {};
        Parser {
            file: file.to_string(),
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

    pub fn prev_end_location(&self) -> Location {
        self.sqlparser.prev_end_location()
    }

    pub fn is_eof(&self) -> bool {
        self.peek_token().token == Token::EOF
    }

    pub fn may_have_more(&mut self) -> bool {
        if !self.is_eof() {
            return false;
        }

        self.sqlparser.prev_token();
        return self.sqlparser.next_token() != Token::SemiColon;
    }

    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        self.sqlparser.consume_token(expected)
    }

    pub fn token_location(&self) -> ErrorLocation {
        self.range_location(self.peek_start_location())
    }

    pub fn range_location(&self, start: Location) -> ErrorLocation {
        let end = self.peek_end_location();

        ErrorLocation::Range(self.file.clone(), Range { start, end })
    }

    pub fn token_context(&self) -> SQLParserSnafu<ErrorLocation> {
        SQLParserSnafu {
            loc: self.token_location(),
        }
    }

    pub fn range_context(&self, start: &Location) -> SQLParserSnafu<ErrorLocation> {
        SQLParserSnafu {
            loc: ErrorLocation::Range(
                self.file.clone(),
                Range {
                    start: start.clone(),
                    end: self.prev_end_location(),
                },
            ),
        }
    }

    pub fn expect_token(&mut self, expected: &Token) -> Result<()> {
        Ok(self
            .sqlparser
            .expect_token(expected)
            .context(self.token_context())?)
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

    pub fn consume_ellipse(&mut self) -> bool {
        match (
            self.sqlparser.peek_nth_token(0).1.token,
            self.sqlparser.peek_nth_token(1).1.token,
            self.sqlparser.peek_nth_token(2).1.token,
        ) {
            (Token::Period, Token::Period, Token::Period) => {
                for _ in 0..3 {
                    let consumed = self.consume_token(&Token::Period);
                    assert!(consumed);
                }
                true
            }
            _ => false,
        }
    }

    pub fn expect_keyword(&mut self, expected: &str) -> Result<()> {
        if self.consume_keyword(expected) {
            Ok(())
        } else {
            let token = self.peek_token();
            Ok(self
                .sqlparser
                .expected::<()>(expected, token)
                .context(self.token_context())?)
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
    pub fn parse_schema(&mut self) -> ParseResult<Schema> {
        let mut stmts = Vec::new();
        let mut errors = Vec::new();
        let mut idx: usize = 0;
        while !matches!(self.peek_token().token, Token::EOF) {
            let ParseResult {
                result: stmt,
                errors: stmt_errors,
            } = self.parse_stmt(idx);

            errors.extend(stmt_errors);

            if matches!(stmt.body, StmtBody::Unparsed)
                && matches!(
                    stmts.last().map(|s: &Stmt| &s.body),
                    Some(&StmtBody::Unparsed)
                )
            {
                let len = stmts.len();
                stmts[len - 1].end = stmt.end;
            } else {
                idx += 1;
                stmts.push(stmt);
            }
        }

        let mut result = ParseResult::new(Schema { stmts });
        for (idx, err) in errors {
            result.add_error(idx, err);
        }
        result
    }

    pub fn parse_stmt(&mut self, idx: usize) -> ParseResult<Stmt> {
        let start = self.peek_start_location();
        let mut result = ParseResult::<Stmt>::new(Stmt {
            export: false,
            body: StmtBody::Noop,
            start: start.clone(),
            end: start.clone(),
        });
        if self.consume_token(&Token::SemiColon) {
            return result;
        }

        let export = self.consume_keyword("export");
        let body = if self.consume_keyword("fn") {
            self.parse_fn()
        } else if self.consume_keyword("extern") {
            self.parse_extern()
        } else if self.consume_keyword("let") {
            self.parse_let(false)
        } else if self.consume_keyword("mat") {
            self.parse_let(true)
        } else if self.consume_keyword("type") {
            self.parse_typedef()
        } else if self.consume_keyword("import") || export {
            self.parse_import()
        } else {
            self.parse_expr_stmt()
        };

        match body {
            Ok(body) => {
                let end = self.prev_end_location();
                result.set_result(Stmt {
                    export,
                    body,
                    start,
                    end,
                });
            }
            Err(err) => {
                // Attempt to skip ahead until the next time we encounter something that looks like
                // the start or end of a statement.
                //
                while !self.peek_keyword("export")
                    && !self.peek_keyword("fn")
                    && !self.peek_keyword("extern")
                    && !self.peek_keyword("let")
                    && !self.peek_keyword("mat")
                    && !self.peek_keyword("type")
                    && !self.peek_keyword("import")
                    && !self.peek_keyword("select")
                    && !self.peek_keyword("with")
                    && !self.peek_keyword("unsafe")
                    && self.peek_token().token != Token::SemiColon
                    && self.peek_token().token != Token::EOF
                {
                    self.next_token();
                }
                if self.peek_token().token == Token::SemiColon {
                    self.next_token();
                }
                let end = self.prev_end_location();

                result.set_result(Stmt {
                    export: false,
                    body: StmtBody::Unparsed,
                    start,
                    end,
                });

                result.add_error(Some(idx), err);
            }
        }

        result
    }

    pub fn parse_ident(&mut self) -> Result<Located<Ident>> {
        let start = self.peek_start_location();
        let end = self.peek_end_location();

        let ident = self.sqlparser.parse_identifier().context(SQLParserSnafu {
            loc: SourceLocation::Range(self.file.clone(), Range { start, end }),
        })?;

        let loc = SourceLocation::from_file_range(self.file.clone(), ident.location().clone());

        Ok(Ident::from_sqlident(loc, ident.into_inner()))
    }

    pub fn parse_path(&mut self, kind: char) -> Result<Path> {
        let mut path = Vec::<Located<Ident>>::new();
        loop {
            self.autocomplete_tokens(&[Token::make_word(
                sqlast::ObjectName(path.iter().map(ToSqlIdent::to_sqlident).collect())
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

    pub fn parse_idents(&mut self) -> Result<Vec<Located<Ident>>> {
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

        let mut variadic_arg = None;
        let args = if self.consume_token(&Token::RParen) {
            Vec::new()
        } else {
            let mut args = Vec::new();
            loop {
                let name = self.parse_ident()?;
                let is_variadic = self.consume_ellipse();
                let type_ = self.parse_type()?;

                if is_variadic {
                    if variadic_arg.is_some() {
                        return Err(ParserError::invalid(
                            SourceLocation::Range(
                                self.file.clone(),
                                Range {
                                    start: type_.start,
                                    end: type_.end,
                                },
                            ),
                            "Only one variadic argument is allowed",
                        ));
                    }
                    variadic_arg = Some(FnArg { name, type_ });
                } else {
                    args.push(FnArg { name, type_ });
                }

                self.autocomplete_tokens(&[Token::Comma, Token::RParen]);
                let next_token = self.next_token();
                match &next_token.token {
                    Token::Comma => {}
                    Token::RParen => break args,
                    _ => {
                        return unexpected_token!(
                            self.file.clone(),
                            &next_token,
                            "Expected: ',' | ')'"
                        );
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
                return unexpected_token!(
                    self.file.clone(),
                    self.peek_token(),
                    "Expected: native | sql"
                );
            }
        } else {
            self.expect_token(&Token::LBrace)?;

            let body = self.parse_expr()?;

            self.expect_token(&Token::RBrace)?;
            FnBody::Expr(body)
        };

        Ok(StmtBody::FnDef(FnDef {
            name,
            generics,
            args,
            variadic_arg,
            ret,
            body,
        }))
    }

    pub fn parse_let(&mut self, materialize: bool) -> Result<StmtBody> {
        let materialize = if materialize {
            let mut args = Vec::new();
            if self.peek_token() == Token::LParen {
                let _ = self.consume_token(&Token::LParen);
                args = self
                    .sqlparser
                    .parse_optional_args()
                    .context(self.token_context())?;
            }

            if args.len() > 1 {
                return Err(ParserError::unimplemented(
                    self.token_location(),
                    "mat supports up to 1 argument (db)",
                ));
            }
            let mut db = None;
            for (i, arg) in args.into_iter().enumerate() {
                match arg {
                    sqlast::FunctionArg::Named { name, arg } => {
                        if Into::<Ident>::into(name.get()) == Into::<Ident>::into("db") {
                            db = Some(arg);
                        }
                    }
                    sqlast::FunctionArg::Unnamed(arg) => {
                        if i == 0 {
                            db = Some(arg);
                        }
                    }
                }
            }
            Some(MaterializeArgs {
                db: match db {
                    Some(sqlast::FunctionArgExpr::Expr(e)) => Some(Expr {
                        body: ExprBody::SQLExpr(e),
                        start: self.peek_start_location().clone(),
                        end: self.peek_start_location().clone(),
                        is_unsafe: false,
                    }),
                    Some(_) => {
                        return Err(ParserError::unimplemented(
                            self.token_location(),
                            "mat conn argument must be an expression",
                        ));
                    }
                    None => None,
                },
            })
        } else {
            None
        };

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
                return unexpected_token!(
                    self.file.clone(),
                    self.peek_token(),
                    "Expected definition"
                );
            }
        };

        self.expect_eos()?;

        Ok(StmtBody::Let {
            name,
            type_,
            body,
            materialize,
        })
    }

    pub fn parse_expr_stmt(&mut self) -> Result<StmtBody> {
        let expr = self.parse_expr()?;
        self.expect_eos()?;

        Ok(StmtBody::Expr(expr))
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
            let type_name = self.parse_path(AUTOCOMPLETE_TYPE)?;
            if self.consume_token(&Token::Lt) {
                let mut args = Vec::new();
                loop {
                    args.push(self.parse_type()?);
                    self.autocomplete_tokens(&[Token::Comma, Token::Gt]);

                    match self.next_token().token {
                        Token::Comma => {}
                        Token::Gt => break,
                        _ => {
                            return unexpected_token!(
                                self.file.clone(),
                                self.peek_token(),
                                "Expected: ',' | '>'"
                            );
                        }
                    }
                }
                TypeBody::Generic(type_name, args)
            } else if self.consume_token(&Token::Neq) {
                TypeBody::Generic(type_name, Vec::new())
            } else {
                TypeBody::Reference(type_name)
            }
        };

        if self.consume_keyword("exclude") {
            let excluded = self.parse_idents()?;
            let end = self.prev_end_location();
            body = TypeBody::Exclude {
                inner: Box::new(Type {
                    body,
                    start: start.clone(),
                    end,
                }),
                excluded,
            };
        }

        let end = self.prev_end_location();

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
                        return unexpected_token!(
                            self.file.clone(),
                            self.peek_token(),
                            "Two consecutive commas"
                        );
                    }
                    self.next_token();
                }
                Token::Period => {
                    for _ in 0..3 {
                        self.autocomplete_tokens(&[Token::Period]);
                        if !matches!(self.peek_token().token, Token::Period) {
                            return unexpected_token!(
                                self.file.clone(),
                                self.peek_token(),
                                "Three periods"
                            );
                        }
                        self.next_token();
                    }
                    struct_.push(StructEntry::Include(self.parse_path(AUTOCOMPLETE_TYPE)?));
                    needs_comma = true;
                }
                _ => {
                    if needs_comma {
                        return unexpected_token!(
                            self.file.clone(),
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

        let is_unsafe = self.consume_keyword("unsafe");

        let body = match self.peek_token().token {
            Token::Word(Word {
                value: _,
                quote_style: _,
                keyword: Keyword::SELECT | Keyword::WITH,
            }) => {
                let query = self
                    .sqlparser
                    .parse_query()
                    .context(self.range_context(&start))?;
                ExprBody::SQLQuery(query)
            }
            _ => {
                let expr = self
                    .sqlparser
                    .parse_expr()
                    .context(self.range_context(&start))?;
                ExprBody::SQLExpr(expr)
            }
        };

        Ok(Expr {
            body,
            start,
            end: self.prev_end_location(),
            is_unsafe,
        })
    }
}

pub fn tokenize(file: &str, text: &str) -> Result<(Vec<TokenWithLocation>, Location)> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, text);

    Ok(tokenizer
        .tokenize_with_location()
        .context(TokenizerSnafu { file })?)
}

pub fn parse_schema(file: &str, text: &str) -> ParseResult<Schema> {
    let mut result = ParseResult::new(Schema { stmts: Vec::new() });
    let (tokens, eof) = c_try!(result, tokenize(file.clone(), text));
    let mut parser = Parser::new(file, tokens, eof);

    parser.parse_schema()
}

pub fn parse_path(file: &str, text: &str) -> Result<Path> {
    let (tokens, eof) = tokenize(file.clone(), text)?;
    let mut parser = Parser::new(file, tokens, eof);

    parser.parse_path(AUTOCOMPLETE_UNKNOWN)
}
