use rustyline::{completion::*, highlight::*, hint::*, validate::*, Context, Helper, Result};

use qvm::compile;
use qvm::compile::schema;
use qvm::parser;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fmt;
use std::fs::read_dir;
use std::path::Path;
use std::rc::Rc;

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
    pub compiler: compile::Compiler,
    pub schema: schema::Ref<schema::Schema>,
    pub curr_buffer: Rc<RefCell<String>>,
    pub stats: Rc<RefCell<ReadlineStats>>,
    pub debug: bool,
}

impl ReadlineHelper {
    pub fn new(
        compiler: compile::Compiler,
        schema: schema::Ref<schema::Schema>,
        curr_buffer: Rc<RefCell<String>>,
    ) -> ReadlineHelper {
        ReadlineHelper {
            compiler,
            schema,
            curr_buffer,
            stats: Rc::new(RefCell::new(ReadlineStats::new())),
            debug: true, // Switch this to true to get diagnostics as you type
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

fn parse_longest_path(texts: &Vec<String>) -> Vec<String> {
    texts
        .iter()
        .fold::<Vec<String>, _>(Vec::new(), |acc, item| {
            let parsed = if item.is_empty() {
                Vec::new()
            } else {
                match parser::parse_path(item.as_str()) {
                    Ok(path) => path,
                    Err(_) => Vec::new(),
                }
            };
            if acc.len() < parsed.len() {
                parsed
            } else {
                acc
            }
        })
}

fn get_imported_decls<F: FnMut(&schema::SchemaEntry) -> bool>(
    compiler: compile::Compiler,
    schema: schema::Ref<schema::Schema>,
    path: &Vec<String>,
    mut f: F,
) -> compile::Result<Vec<String>> {
    let (schema, _, remainder) = compile::lookup_path(compiler, schema.clone(), path, true, true)?;
    if remainder.len() > 0 {
        return Ok(Vec::new());
    }
    return Ok(schema
        .read()?
        .decls
        .iter()
        .filter_map(move |(k, v)| if f(&v.value) { Some(k.clone()) } else { None })
        .collect::<Vec<String>>());
}

fn get_schema_paths(
    schema: schema::Ref<schema::Schema>,
    path: &Vec<String>,
) -> compile::Result<Vec<String>> {
    if let Some(folder) = schema.read()?.folder.clone() {
        let mut folder = Path::new(&folder).to_path_buf();
        folder.extend(path.iter());
        let files = read_dir(folder)?;
        let mut ret = Vec::new();
        for f in files {
            if let Ok(f) = f {
                let file = f.path();
                let extension = file.extension().and_then(OsStr::to_str).unwrap_or("");
                if schema::SCHEMA_EXTENSIONS.contains(&extension) {
                    if let Some(fp) = file.file_stem().and_then(OsStr::to_str) {
                        ret.push(fp.to_string());
                    }
                }
                if file.is_dir() {
                    if let Some(fp) = file.file_name().and_then(OsStr::to_str) {
                        ret.push(fp.to_string());
                    }
                }
            }
        }
        return Ok(ret);
    }
    Ok(Vec::new())
}

fn get_record_fields(
    compiler: compile::Compiler,
    schema: schema::Ref<schema::Schema>,
    path: &Vec<String>,
) -> compile::Result<Vec<String>> {
    let expr = compile::compile_reference(compiler.clone(), schema.clone(), path)?;
    let type_ = expr.type_.must()?.read()?.clone();

    match type_ {
        schema::MType::Record(fields) => {
            return Ok(fields.iter().map(|f| f.name.clone()).collect());
        }
        _ => {}
    }

    Ok(Vec::new())
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
        let partial = match tok.token {
            parser::Token::Word(w) => w.value,
            _ => "".to_string(),
        };
        let suggestion_loc = tok.location.clone();
        let suggestion_pos = loc_to_pos(full.as_str(), suggestion_loc);

        if suggestion_pos < start_pos {
            (&mut *self.stats.borrow_mut()).msg = format!("failed before");
            return Ok((0, Vec::new()));
        }

        let mut ident_types = BTreeMap::<char, Vec<String>>::new();
        for s in suggestions.clone() {
            match s {
                parser::Token::Word(w) => {
                    let style = match w.quote_style {
                        None | Some(parser::AUTOCOMPLETE_KEYWORD) => parser::AUTOCOMPLETE_KEYWORD,
                        Some('\"') | Some(parser::AUTOCOMPLETE_VARIABLE) => {
                            parser::AUTOCOMPLETE_VARIABLE
                        }
                        Some(c) => c,
                    };
                    ident_types
                        .entry(style)
                        .or_insert_with(Vec::new)
                        .push(w.value);
                }
                _ => {}
            }
        }

        let vars = ident_types
            .get(&parser::AUTOCOMPLETE_VARIABLE)
            .map(parse_longest_path)
            .map_or(Vec::new(), |path| {
                if let Ok(choices) =
                    get_imported_decls(self.compiler.clone(), self.schema.clone(), &path, |se| {
                        matches!(se, schema::SchemaEntry::Expr(_))
                    })
                {
                    return choices;
                }
                if let Ok(choices) =
                    get_record_fields(self.compiler.clone(), self.schema.clone(), &path)
                {
                    return choices;
                }
                Vec::new()
            })
            .clone();

        let types = ident_types
            .get(&parser::AUTOCOMPLETE_TYPE)
            .map(parse_longest_path)
            .map_or(Vec::new(), |path| {
                if let Ok(choices) =
                    get_imported_decls(self.compiler.clone(), self.schema.clone(), &path, |se| {
                        matches!(se, schema::SchemaEntry::Type(_))
                            || matches!(se, schema::SchemaEntry::Schema(_))
                    })
                {
                    return choices;
                }
                Vec::new()
            });

        let schemas = ident_types
            .get(&parser::AUTOCOMPLETE_SCHEMA)
            .map(parse_longest_path)
            .map_or(Vec::new(), |path| {
                if let Ok(choices) = get_schema_paths(self.schema.clone(), &path) {
                    return choices;
                }
                Vec::new()
            });

        let mut keywords = ident_types
            .get(&parser::AUTOCOMPLETE_KEYWORD)
            .unwrap_or(&Vec::new())
            .clone();

        if match partial.chars().next() {
            Some(c) => c.is_lowercase(),
            None => false,
        } {
            keywords = keywords.iter().map(|k| k.to_lowercase()).collect();
        }

        let all = vec![vars, types, schemas, keywords].concat();
        let filtered = all
            .iter()
            .filter(|a| a.starts_with(partial.as_str()))
            .collect::<Vec<_>>();

        (&mut *self.stats.borrow_mut()).msg = format!(
            "{} {:?} {:?}",
            suggestion_pos - start_pos,
            partial,
            filtered,
        );
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
