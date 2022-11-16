use rustyline::{error::ReadlineError, Editor};
use snafu::{prelude::*, Whatever};

use qvm::compile;
use qvm::compile::schema;
use qvm::parser;
use qvm::runtime;

pub fn run() {
    let cwd = std::env::current_dir()
        .expect("current working directory")
        .display()
        .to_string();
    let repl_schema = schema::Schema::new(Some(cwd));

    let mut rl = Editor::<()>::new().expect("readline library failed");

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
        let readline = rl.readline("qvm> ");

        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                match line.trim().to_lowercase().trim_end_matches(';') {
                    "exit" | "quit" => {
                        println!("Goodbye!");
                        break;
                    }
                    _ => {}
                };

                match run_command(repl_schema.clone(), &line) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Error: {}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted...");
                break;
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

fn run_command(repl_schema: schema::SchemaRef, cmd: &str) -> Result<(), Whatever> {
    let tokens = parser::tokenize(&cmd).with_whatever_context(|e| format!("{}", e))?;
    let mut parser = parser::Parser::new(tokens);

    match parser.parse_schema() {
        Ok(ast) => {
            compile::compile_schema_entries(repl_schema.clone(), &ast)
                .with_whatever_context(|e| format!("{}", e))?;
            return Ok(());
        }
        Err(_) => {
            // TODO: Currently falls back to parsing as an expression in this case.
            // We should really distinguish between whether this _is_ a schema vs.
            // an error parsing a schema.
            parser.reset();
        }
    };

    match parser.parse_expr() {
        Ok(ast) => {
            let compiled = compile::compile_expr(repl_schema.clone(), &ast)
                .with_whatever_context(|e| format!("{}", e))?;
            let expr = compiled
                .to_runtime_type()
                .with_whatever_context(|e| format!("{}", e))?;
            let value = runtime::eval(repl_schema.clone(), &expr)
                .with_whatever_context(|e| format!("{}", e))?;
            println!("{:?}", value);
        }
        Err(e) => {
            whatever!("{}", e);
        }
    };

    return Ok(());
}

fn get_qvm_dir() -> Option<std::path::PathBuf> {
    home::home_dir().map(|p| p.join(".qvm"))
}
