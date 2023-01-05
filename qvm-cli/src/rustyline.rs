use rustyline::{completion::*, highlight::*, hint::*, validate::*, Context, Helper, Result};

use qvm::compile;
use qvm::compile::autocomplete::AutoCompleter;
use qvm::compile::schema;

use std::cell::RefCell;
use std::rc::Rc;

pub struct RustylineHelper(AutoCompleter);

impl RustylineHelper {
    pub fn new(
        compiler: compile::Compiler,
        schema: schema::Ref<schema::Schema>,
        curr_buffer: Rc<RefCell<String>>,
    ) -> RustylineHelper {
        RustylineHelper(AutoCompleter::new(compiler, schema, curr_buffer))
    }
}

impl Completer for RustylineHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Self::Candidate>)> {
        let (pos, suggestions) = self.0.auto_complete(line, pos);
        Ok((
            pos,
            suggestions
                .into_iter()
                .map(|s| Pair {
                    display: s.to_string(),
                    replacement: s.to_string(),
                })
                .collect(),
        ))
    }
}

impl Hinter for RustylineHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        if self.0.debug {
            Some(format!("         {}", self.0.stats.borrow()))
        } else {
            None
        }
    }
}

impl Highlighter for RustylineHelper {}
impl Validator for RustylineHelper {}
impl Helper for RustylineHelper {}
