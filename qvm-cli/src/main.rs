use clap::Parser;
use snafu::ErrorCompat;
use snafu::{prelude::*, Whatever};
use std::path::Path;

use qvm::compile;
use qvm::runtime;

mod repl;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    file: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    match cli.file {
        Some(file) => match run_file(&file) {
            Err(err) => {
                eprintln!("{}", err);
                if let Some(bt) = ErrorCompat::backtrace(&err) {
                    eprintln!("{}", bt);
                }
            }
            Ok(()) => {}
        },
        None => {
            crate::repl::run();
        }
    }
}

fn run_file(file: &str) -> Result<(), Whatever> {
    let schema = compile::compile_schema_from_file(&Path::new(&file))
        .with_whatever_context(|e| format!("{}", e))?;

    for expr in schema.borrow().exprs.iter() {
        let expr = expr
            .to_runtime_type()
            .with_whatever_context(|e| format!("{}", e))?;
        let value =
            runtime::eval(schema.clone(), &expr).with_whatever_context(|e| format!("{}", e))?;
        println!("{:?}", value);
    }

    Ok(())
}
