use snafu::prelude::*;
use std::collections::BTreeMap;

use crate::ast::SourceLocation;

use super::error::*;
use super::schema::*;
use super::{compile_reference, Compiler, Schema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    Literal(String),
    Format(String),
}

#[derive(Debug, Clone)]
pub struct StringFormatter {
    pub loc: SourceLocation,
    pub segments: Vec<Segment>,
}

impl StringFormatter {
    pub fn build(s: String, loc: SourceLocation) -> StringFormatter {
        let mut segments = Vec::new();
        let mut in_format = false;
        let mut curr = "".to_string();
        let mut iter = s.chars().peekable();
        loop {
            let tok = match iter.next() {
                Some(c) => c,
                None => break,
            };
            if tok == '{' && matches!(iter.peek(), Some('{')) {
                curr.push(iter.next().unwrap());
                continue;
            } else if tok == '}' && matches!(iter.peek(), Some('}')) {
                curr.push(iter.next().unwrap());
                continue;
            }
            if tok == '{' && !in_format {
                in_format = true;
                if curr.len() > 0 {
                    segments.push(Segment::Literal(curr.drain(..).collect()));
                }
            } else if tok == '}' && in_format {
                in_format = false;
                if curr.len() > 0 {
                    // NOTE: Eventually we could support positional arguments with {}
                    segments.push(Segment::Format(curr.drain(..).collect()));
                }
            } else {
                curr.push(tok);
            }
        }
        if curr.len() > 0 {
            if in_format {
                curr = "{".to_string() + &curr;
            }
            segments.push(Segment::Literal(curr))
        }
        StringFormatter {
            loc,
            segments: segments,
        }
    }

    pub fn format(self, names: BTreeMap<String, String>) -> String {
        self.segments
            .into_iter()
            .map(|s| match s {
                Segment::Literal(s) => s,
                Segment::Format(s) => names
                    .get(&s)
                    .cloned()
                    .expect(&format!("Unknown format specifier: {}", s)),
            })
            .collect()
    }

    pub fn resolve_format_string(
        self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        loc: &SourceLocation,
    ) -> Result<String> {
        let mut names = BTreeMap::new();
        for segment in self.segments.iter() {
            let param_name = match segment {
                Segment::Literal(_) => continue,
                Segment::Format(f) => f,
            };

            if names.contains_key(param_name) {
                continue;
            }

            let names_ident: Located<Ident> =
                Located::new(param_name.clone().into(), SourceLocation::Unknown);
            let expr = compile_reference(
                compiler.clone(),
                schema.clone(),
                // TODO We should parse the innards of a format string as a path, instead of
                // a single ident
                &vec![names_ident],
            )?;

            names.insert(param_name.clone(), expr);
        }

        let mut ctx = crate::runtime::Context::new(
            schema.read()?.folder.clone(),
            crate::runtime::SQLEngineType::DuckDB,
        );

        let mut value_names = BTreeMap::new();
        for (name, expr) in names {
            let te = expr
                .to_runtime_type()
                .context(RuntimeSnafu { loc: loc.clone() })?;
            let result = futures::executor::block_on(async {
                crate::runtime::eval(&mut ctx, &te)
                    .await
                    .context(RuntimeSnafu { loc: loc.clone() })
            })?;
            value_names.insert(name, result.to_string());
        }

        Ok(self.format(value_names))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_format_strings() {
        let cases = vec![
            ("foo", vec![Segment::Literal("foo".to_string())]),
            (
                "foo{bar}",
                vec![
                    Segment::Literal("foo".to_string()),
                    Segment::Format("bar".to_string()),
                ],
            ),
            (
                "{foo}bar",
                vec![
                    Segment::Format("foo".to_string()),
                    Segment::Literal("bar".to_string()),
                ],
            ),
            (
                "foo{bar}baz",
                vec![
                    Segment::Literal("foo".to_string()),
                    Segment::Format("bar".to_string()),
                    Segment::Literal("baz".to_string()),
                ],
            ),
            (
                "foo{bar}baz{qux}",
                vec![
                    Segment::Literal("foo".to_string()),
                    Segment::Format("bar".to_string()),
                    Segment::Literal("baz".to_string()),
                    Segment::Format("qux".to_string()),
                ],
            ),
            (
                "foo{bar } baz }",
                vec![
                    Segment::Literal("foo".to_string()),
                    Segment::Format("bar ".to_string()),
                    Segment::Literal(" baz }".to_string()),
                ],
            ),
            (
                "foo{bar { baz }",
                vec![
                    Segment::Literal("foo".to_string()),
                    Segment::Format("bar { baz ".to_string()),
                ],
            ),
            (
                "foo{bar { baz ",
                vec![
                    Segment::Literal("foo".to_string()),
                    Segment::Literal("{bar { baz ".to_string()),
                ],
            ),
            (
                "foo{}bar",
                vec![
                    Segment::Literal("foo".to_string()),
                    Segment::Literal("bar".to_string()),
                ],
            ),
            ("foo{{}}bar", vec![Segment::Literal("foo{}bar".to_string())]),
        ];

        for case in cases {
            let (input, expected) = case;
            let formatter = StringFormatter::build(input.to_string(), SourceLocation::Unknown);
            assert_eq!(expected, formatter.segments);
        }
    }
}
