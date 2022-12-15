use clap::Parser;
use snafu::{prelude::*, whatever};
use std::fs;
use std::path::Path;

use qvm::compile;
use qvm::error::*;
use qvm::parser;
use qvm::parser::error::PrettyError;
use qvm::runtime;

mod repl;
mod rustyline;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    file: Option<String>,

    #[arg(short, long, default_value_t = false)]
    compile: bool,

    #[arg(short, long, default_value_t = false)]
    parse: bool,

    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    #[arg(long, default_value_t = String::from("duckdb"))]
    engine: String,

    #[arg(short, long)]
    execute: Option<String>,
}

enum Mode {
    Execute,
    Compile,
    Parse,
}

fn main() {
    match main_result() {
        Ok(()) => {}
        Err(e) => {
            eprintln!("{}", e);
        }
    }
}

fn main_result() -> Result<(), QVMError> {
    let cli = Cli::parse();
    if cli.verbose {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    if cli.compile && cli.parse {
        whatever!("Cannot run with --compile and --parse");
    }

    let mode = if cli.compile {
        Mode::Compile
    } else if cli.parse {
        Mode::Parse
    } else {
        Mode::Execute
    };

    let engine_type = qvm::runtime::SQLEngineType::from_name(&cli.engine).unwrap();

    match cli.file {
        Some(file) => {
            let rt = runtime::build().context(RuntimeSnafu {
                file: file.to_string(),
            })?;

            let compiler = compile::Compiler::new()?;
            match run_file(compiler.clone(), &rt, engine_type, &file, mode, cli.execute) {
                Err(err) => {
                    let errs = if cli.verbose {
                        err.format_backtrace()
                    } else {
                        err.format_without_backtrace()
                    };
                    let contents = compiler.file_contents()?;
                    let err_strs = errs
                        .iter()
                        .map(|e| e.pretty_with_code(&contents))
                        .collect::<Vec<_>>();
                    whatever!("{}", err_strs.join("\n"))
                }
                Ok(()) => Ok(()),
            }
        }
        None => {
            if cli.compile {
                whatever!("Cannot run with compile-only mode (--compile) in the repl");
            }
            if cli.parse {
                whatever!("Cannot run with parse-only mode (--parse) in the repl");
            }
            if cli.execute.is_some() {
                whatever!("Cannot run single expression (--execute) in the repl");
            }
            let rt = runtime::build().context(RuntimeSnafu {
                file: "<repl>".to_string(),
            })?;

            crate::repl::run(&rt, engine_type);
            Ok(())
        }
    }
}

fn run_file(
    compiler: compile::Compiler,
    rt: &runtime::Runtime,
    engine_type: qvm::runtime::SQLEngineType,
    file: &str,
    mode: Mode,
    execute: Option<String>,
) -> Result<(), QVMError> {
    let path = Path::new(&file);
    if !path.exists() {
        whatever!("Path {:?} does not exist", path);
    }

    if matches!(mode, Mode::Parse) {
        let file = path.to_str().unwrap();
        let contents = match fs::read_to_string(path) {
            Ok(p) => p,
            Err(e) => whatever!("{}", e),
        };
        let (tokens, eof) = parser::tokenize(file, &contents)?;
        let mut parser = parser::Parser::new(file, tokens, eof);
        let schema = parser.parse_schema()?;
        println!("{:#?}", schema);
        return Ok(());
    }

    let schema = compiler
        .compile_schema_from_file(&Path::new(&file))
        .as_result()?
        .unwrap();

    if let Some(execute) = &execute {
        // Add a semicolon on so that it's not required in the last expression within the argument
        // to --execute
        //
        let execute = execute.clone() + ";";

        // If a string was provided to execute against the schema, then clear out any other
        // expressions to replace them with the provided one.
        //
        // XXX: If this fails, the error locations still point at the schema file, which makes the
        // highlighted context incorrect.
        //
        schema.write()?.exprs = vec![];
        compiler
            .compile_string(schema.clone(), execute.as_str())
            .as_result()?;
    }

    if matches!(mode, Mode::Compile) {
        if execute.is_none() {
            println!("{:#?}", schema);
        } else {
            println!("{:#?}", schema.read()?.exprs.first().unwrap());
        }
        return Ok(());
    }

    let ctx = qvm::runtime::build_context(&schema, engine_type);
    let locked_schema = schema.read()?;
    for expr in locked_schema.exprs.iter() {
        let expr = expr.to_runtime_type().context(RuntimeSnafu {
            file: file.to_string(),
        })?;
        let value = rt
            .block_on(async { runtime::eval(&ctx, &expr).await })
            .context(RuntimeSnafu {
                file: file.to_string(),
            })?;
        println!("{}", value);
    }

    Ok(())
}
