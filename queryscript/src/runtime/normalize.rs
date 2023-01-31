use crate::compile::traverse::{SQLVisitor, VisitSQL};
use sqlparser::{ast as sqlast, ast::Located};
use std::collections::HashMap;

pub trait Normalizer {
    fn quote_style(&self) -> Option<char>;
    fn params(&self) -> &HashMap<String, String>;

    fn normalize<'s>(&'s self, stmt: &sqlast::Statement) -> sqlast::Statement {
        let visitor = NormalizerVisitor::<'s, Self> { normalizer: &self };
        stmt.visit_sql(&visitor)
    }
}

pub struct NormalizerVisitor<'n, N>
where
    N: Normalizer + 'n + ?Sized,
{
    normalizer: &'n N,
}

impl<'n, N> SQLVisitor for NormalizerVisitor<'n, N>
where
    N: Normalizer + 'n + ?Sized,
{
    fn visit_sqlpath(
        &self,
        path: &Vec<Located<sqlast::Ident>>,
    ) -> Option<Vec<Located<sqlast::Ident>>> {
        let params = self.normalizer.params();
        if path.len() == 1 {
            let ident = &path[0];
            if let Some(name) = params.get(&ident.value) {
                return Some(vec![Located::new(
                    sqlast::Ident {
                        value: name.clone(),
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
                            quote_style: self.normalizer.quote_style(),
                        },
                        ident.location().clone(),
                    )
                })
                .collect(),
        )
    }
}
