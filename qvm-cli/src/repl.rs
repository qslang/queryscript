use rustyline::{error::ReadlineError, Editor};
use snafu::{prelude::*, Whatever};

use qvm::compile;
use qvm::compile::schema;
use qvm::parser;
use qvm::runtime;

pub fn run(rt: &runtime::Runtime) {
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

    let mut curr_buffer = String::new();
    loop {
        let readline = rl.readline(if curr_buffer.len() == 0 {
            "qvm> "
        } else {
            "...> "
        });

        match readline {
            Ok(line) => {
                if curr_buffer.len() == 0 {
                    curr_buffer = line;
                } else {
                    curr_buffer.push_str(&format!("\n{}", line));
                }
                match curr_buffer.trim().to_lowercase().trim_end_matches(';') {
                    "exit" | "quit" => {
                        rl.add_history_entry(curr_buffer.as_str());
                        println!("Goodbye!");
                        break;
                    }
                    _ => {}
                };

                match run_command(rt, repl_schema.clone(), &curr_buffer) {
                    Ok(RunCommandResult::Done) => {
                        // Reset the buffer
                        rl.add_history_entry(curr_buffer.as_str());
                        curr_buffer.clear();
                    }
                    Ok(RunCommandResult::More) => {
                        // Allow the loop to run again (and parse more)
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        rl.add_history_entry(curr_buffer.as_str());
                        curr_buffer.clear();
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

enum RunCommandResult {
    Done,
    More,
}

fn run_command(
    rt: &runtime::Runtime,
    repl_schema: schema::SchemaRef,
    cmd: &str,
) -> Result<RunCommandResult, Whatever> {
    let tokens = parser::tokenize(&cmd).with_whatever_context(|e| format!("{}", e))?;
    let mut parser = parser::Parser::new(tokens);

    match parser.parse_schema() {
        Ok(ast) => {
            let cloned_schema = repl_schema.clone();
            let locked_schema = compile::read_whatever(&cloned_schema)?;
            let num_exprs = locked_schema.exprs.len();

            let compiler = compile::Compiler::new().with_whatever_context(|e| format!("{}", e))?;
            compile::compile_schema_ast(compiler.clone(), repl_schema.clone(), &ast)
                .with_whatever_context(|e| format!("{}", e))?;

            if locked_schema.exprs.len() > num_exprs {
                let compiled = locked_schema.exprs.last().unwrap().clone();
                let expr = compiled
                    .to_runtime_type()
                    .with_whatever_context(|e| format!("{}", e))?;
                let value = rt.block_on(async move {
                    runtime::eval(repl_schema.clone(), &expr)
                        .await
                        .with_whatever_context(|e| format!("{}", e))
                })?;
                println!("{:?}", value);
            }
            Ok(RunCommandResult::Done)
        }
        Err(parser::ParserError::Incomplete { .. }) => Ok(RunCommandResult::More),
        Err(e) => whatever!("{}", e),
    }
}

fn get_qvm_dir() -> Option<std::path::PathBuf> {
    home::home_dir().map(|p| p.join(".qvm"))
}
