use crate::compile::traverse::{Visit, Visitor};
use sqlparser::ast as sqlast;
use std::collections::HashMap;

pub trait Normalizer {
    fn quote_style(&self) -> Option<char>;
    fn params(&self) -> &HashMap<String, String>;

    fn normalize<'s>(&'s self, query: &sqlast::Query) -> sqlast::Query {
        let visitor = NormalizerVisitor::<'s, Self> { normalizer: &self };
        query.visit(&visitor)
    }
}

pub struct NormalizerVisitor<'n, N>
where
    N: Normalizer + 'n + ?Sized,
{
    normalizer: &'n N,
}

impl<'n, N> Visitor for NormalizerVisitor<'n, N>
where
    N: Normalizer + 'n + ?Sized,
{
    fn visit_ident(&self, ident: &sqlast::Ident) -> Option<sqlast::Ident> {
        let params = self.normalizer.params();
        Some(match params.get(&ident.value) {
            Some(name) => sqlast::Ident {
                value: name.clone(),
                quote_style: None,
            },
            None => sqlast::Ident {
                value: ident.value.clone(),
                quote_style: self.normalizer.quote_style(),
            },
        })
    }
}
