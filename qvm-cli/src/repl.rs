use rustyline::{error::ReadlineError, Editor};
use snafu::prelude::*;

use crate::rustyline::RustylineHelper;
use qvm::compile;
use qvm::compile::schema;
use qvm::error::*;
use qvm::parser;
use qvm::parser::error::PrettyError;
use qvm::runtime;

use std::cell::RefCell;
use std::rc::Rc;

pub fn run(rt: &runtime::Runtime, engine_type: qvm::runtime::SQLEngineType) {
    let cwd = std::env::current_dir()
        .expect("current working directory")
        .display()
        .to_string();
    let repl_compiler = compile::Compiler::new().unwrap();
    let file = "<repl>".to_string();
    let repl_schema = schema::Schema::new(file.clone(), Some(cwd));
    let curr_buffer = Rc::new(RefCell::new(String::new()));
    let helper = RustylineHelper::new(
        repl_compiler.clone(),
        repl_schema.clone(),
        curr_buffer.clone(),
    );

    let mut rl = Editor::with_config(
        rustyline::Config::builder()
            // .completion_type(rustyline::config::CompletionType::List)
            .build(),
    )
    .expect("readline library failed");
    rl.set_helper(Some(helper));

    let qvm_dir = get_qvm_dir();
    let qvm_history = match &qvm_dir {
        Some(p) => {
            std::fs::create_dir_all(p).expect("failed to create qvm dir");
            Some(p.join("history.txt").display().to_string())
        }
        None => None,
    };

    if let Some(history_file) = &qvm_history {
        // This function returns an error when the history file does not exist,
        // which is ok.
        match rl.load_history(history_file) {
            Ok(_) => {}
            Err(_) => {}
        }
    }

    loop {
        let readline = rl.readline(if curr_buffer.borrow().len() == 0 {
            "qvm> "
        } else {
            "...> "
        });

        match readline {
            Ok(line) => {
                if curr_buffer.borrow().len() == 0 {
                    *curr_buffer.borrow_mut() = line;
                } else {
                    curr_buffer.borrow_mut().push_str(&format!("\n{}", line));
                }
                match curr_buffer
                    .borrow()
                    .trim()
                    .to_lowercase()
                    .trim_end_matches(';')
                {
                    "exit" | "quit" => {
                        rl.add_history_entry(curr_buffer.borrow().as_str());
                        println!("Goodbye!");
                        break;
                    }
                    _ => {}
                };

                let result = run_command(
                    rt,
                    repl_compiler.clone(),
                    repl_schema.clone(),
                    &*curr_buffer.borrow(),
                    engine_type,
                );
                match result {
                    Ok(RunCommandResult::Done) => {
                        // Reset the buffer
                        rl.add_history_entry(curr_buffer.borrow().as_str());
                        curr_buffer.borrow_mut().clear();
                    }
                    Ok(RunCommandResult::More) => {
                        // Allow the loop to run again (and parse more)
                    }
                    Err(e) => {
                        repl_compiler
                            .set_file_contents(file.clone(), curr_buffer.borrow().clone())
                            .unwrap();
                        let code = repl_compiler.file_contents().unwrap();
                        eprintln!("{}", e.pretty_with_code(&code.files));
                        rl.add_history_entry(curr_buffer.borrow().as_str());
                        curr_buffer.borrow_mut().clear();
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                curr_buffer.borrow_mut().clear();
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                println!("I/O error: {:?}", err);
                break;
            }
        }
    }

    if let Some(history_file) = &qvm_history {
        rl.save_history(history_file)
            .expect("failed to save history");
    }
}

enum RunCommandResult {
    Done,
    More,
}

fn run_command(
    rt: &runtime::Runtime,
    compiler: compile::Compiler,
    repl_schema: schema::SchemaRef,
    cmd: &str,
    engine_type: qvm::runtime::SQLEngineType,
) -> Result<RunCommandResult, QVMError> {
    let file = "<repl>";
    let (tokens, eof) = parser::tokenize(file, &cmd)?;
    let mut parser = parser::Parser::new(file, tokens, eof);

    if parser.consume_token(&parser::Token::Placeholder("?".to_string())) {
        parser.parse_schema();
        let loc = parser.peek_token().location;
        eprintln!("{:#?}", parser.get_autocomplete(loc));
        return Ok(RunCommandResult::Done);
    }

    match parser.parse_schema().as_result() {
        Ok(ast) => {
            let num_exprs = repl_schema.read()?.exprs.len();

            compiler
                .compile_schema_ast(repl_schema.clone(), &ast)
                .as_result()?;

            let compiled = {
                let locked_schema = repl_schema.read()?;
                if locked_schema.exprs.len() > num_exprs {
                    Some(locked_schema.exprs.last().unwrap().clone())
                } else {
                    None
                }
            };

            if let Some(compiled) = compiled {
                let ctx = qvm::runtime::Context::new(&repl_schema, engine_type);
                let expr = compiled.to_runtime_type().context(RuntimeSnafu {
                    file: file.to_string(),
                })?;
                let value = rt
                    .block_on(async move { runtime::eval(&ctx, &expr).await })
                    .context(RuntimeSnafu {
                        file: file.to_string(),
                    })?;
                println!("{}", value);
            }
            Ok(RunCommandResult::Done)
        }
        Err(_) if parser.is_eof() => Ok(RunCommandResult::More),
        Err(e) => Err(e.into()),
    }
}

fn get_qvm_dir() -> Option<std::path::PathBuf> {
    home::home_dir().map(|p| p.join(".qvm"))
}
