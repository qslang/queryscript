use sqlparser::{ast as sqlast, ast::Located};
use std::borrow::Cow;
use std::cell::RefCell;

use super::error::RuntimeError;
use crate::compile::traverse::{SQLVisitor, VisitSQL};
use crate::error::MultiResult;

pub trait Normalizer {
    fn quote_style(&self) -> Option<char>;
    fn should_quote(&self, ident: &sqlast::Ident) -> bool {
        match ident.value.as_str() {
            "grouping" => false, // SQL Parsers (DuckDB and Postgres) expect grouping _not_ to be quoted (it isn't parsed as a function)
            _ => true,
        }
    }
    fn param(&self, key: &str) -> Option<&str>;

    fn preprocess<'a>(&self, stmt: &'a sqlast::Statement) -> Cow<'a, sqlast::Statement> {
        Cow::Borrowed(stmt)
    }

    fn normalize<'s>(
        &'s self,
        stmt: &sqlast::Statement,
    ) -> MultiResult<sqlast::Statement, RuntimeError> {
        let visitor = NormalizerVisitor::<'s, Self> {
            normalizer: &self,
            errors: RefCell::new(Vec::new()),
        };

        let stmt = self.preprocess(stmt);
        let mut result = MultiResult::new(stmt.visit_sql(&visitor));
        for e in visitor.errors.into_inner() {
            result.add_error(None, e);
        }

        result
    }
}

pub struct NormalizerVisitor<'n, N>
where
    N: Normalizer + 'n + ?Sized,
{
    normalizer: &'n N,
    errors: RefCell<Vec<RuntimeError>>,
}

impl<'n, N> SQLVisitor for NormalizerVisitor<'n, N>
where
    N: Normalizer + 'n + ?Sized,
{
    fn visit_sqlpath(
        &self,
        path: &Vec<Located<sqlast::Ident>>,
    ) -> Option<Vec<Located<sqlast::Ident>>> {
        // If we encounter any format strings in the idents, then it's a bug (we should
        // have resolved these in the compiler)
        for p in path {
            if matches!(p.get().quote_style, Some('f')) {
                self.errors.borrow_mut().push(RuntimeError::new(&format!(
                    "unresolved format string: {:?}",
                    p.get()
                )))
            }
        }

        if path.len() == 1 {
            let ident = &path[0];
            if let Some(name) = self.normalizer.param(&ident.value) {
                return Some(vec![Located::new(
                    sqlast::Ident {
                        value: name.to_owned(),
                        quote_style: None,
                    },
                    ident.location().clone(),
                )]);
            }
        }

        Some(
            path.iter()
                .map(|ident| {
                    Located::new(
                        sqlast::Ident {
                            value: ident.value.clone(),
                            quote_style: if self.normalizer.should_quote(&ident) {
                                self.normalizer.quote_style()
                            } else {
                                None
                            },
                        },
                        ident.location().clone(),
                    )
                })
                .collect(),
        )
    }
}
