use rustyline::{completion::*, highlight::*, hint::*, validate::*, Context, Helper, Result};

use qvm::compile::schema;
use qvm::parser;

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
pub struct ReadlineStats {
    tried: u64,
    completed: u64,
    msg: String,
}

impl ReadlineStats {
    pub fn new() -> ReadlineStats {
        ReadlineStats {
            tried: 0,
            completed: 0,
            msg: String::new(),
        }
    }
}

impl fmt::Display for ReadlineStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:?}", self))?;
        Ok(())
    }
}

pub struct ReadlineHelper {
    pub schema: Arc<RwLock<schema::Schema>>,
    pub curr_buffer: Rc<RefCell<String>>,
    pub stats: Rc<RefCell<ReadlineStats>>,
    pub debug: bool,
}

impl ReadlineHelper {
    pub fn new(
        schema: Arc<RwLock<schema::Schema>>,
        curr_buffer: Rc<RefCell<String>>,
    ) -> ReadlineHelper {
        ReadlineHelper {
            schema,
            curr_buffer,
            stats: Rc::new(RefCell::new(ReadlineStats::new())),
            debug: false, // Switch this to true to get diagnostics as you type
        }
    }
}

fn pos_to_loc(text: &str, pos: usize) -> parser::Location {
    let line: u64 = (text[..pos]
        .as_bytes()
        .iter()
        .filter(|&&c| c == b'\n')
        .count()
        + 1) as u64;
    let column: u64 = (pos - text[..pos].rfind('\n').unwrap_or(0) + 1) as u64;
    parser::Location { line, column }
}

fn loc_to_pos(text: &str, loc: parser::Location) -> usize {
    text.split('\n').collect::<Vec<_>>()[..(loc.line - 1) as usize]
        .iter()
        .map(|l| l.len() + 1)
        .sum::<usize>()
        + loc.column as usize
        - 1
}

impl Completer for ReadlineHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Self::Candidate>)> {
        (&mut *self.stats.borrow_mut()).tried += 1;

        let mut full = self.curr_buffer.borrow().clone();
        let start_pos = full.len();
        full.push_str(&line);

        let full_pos = start_pos + pos;
        let full_loc = pos_to_loc(full.as_str(), full_pos);

        let (tokens, eof) = match parser::tokenize(&full) {
            Ok(r) => r,
            Err(e) => {
                (&mut *self.stats.borrow_mut()).msg = format!("{}", e);
                return Ok((0, Vec::new()));
            }
        };
        let mut parser = parser::Parser::new(tokens, eof);
        parser.parse_schema().ok();

        let (tok, suggestions) = parser.get_autocomplete(full_loc);
        let suggestion_loc = tok.location.clone();
        let suggestion_pos = loc_to_pos(full.as_str(), suggestion_loc);

        if suggestion_pos < start_pos {
            (&mut *self.stats.borrow_mut()).msg = format!("failed before");
            return Ok((0, Vec::new()));
        }

        let expects_ident = suggestions
            .iter()
            .filter(|t| {
                matches!(
                    t,
                    parser::Token::Word(parser::Word {
                        quote_style: Some('\"'),
                        ..
                    })
                )
            })
            .count()
            > 0;

        let all: Vec<String> = if expects_ident {
            let schema = match self.schema.read() {
                Ok(s) => s,
                Err(e) => {
                    (&mut *self.stats.borrow_mut()).msg = format!("{}", e);
                    return Ok((0, Vec::new()));
                }
            };
            schema
                .decls
                .iter()
                .filter_map(|(k, v)| match v.value {
                    schema::SchemaEntry::Expr(_) | schema::SchemaEntry::Schema(_) => {
                        Some(k.clone())
                    }
                    _ => None,
                })
                .collect()
        } else {
            suggestions
                .into_iter()
                .filter_map(|t| match t {
                    parser::Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
                    _ => None,
                })
                .collect()
        };

        let filtered = all.iter().collect::<Vec<&String>>();
        (&mut *self.stats.borrow_mut()).msg = format!("{} {:?}", suggestion_pos - start_pos, all);
        let pairs = filtered
            .into_iter()
            .map(|s| Pair {
                display: s.clone(),
                replacement: s.clone(),
            })
            .collect();

        (&mut *self.stats.borrow_mut()).completed += 1;

        Ok((suggestion_pos - start_pos, pairs))
    }
}

impl Hinter for ReadlineHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        if self.debug {
            Some(format!("         {}", self.stats.borrow()))
        } else {
            None
        }
    }
}

impl Highlighter for ReadlineHelper {}
impl Validator for ReadlineHelper {}
impl Helper for ReadlineHelper {}
