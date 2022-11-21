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

    #[arg(short, long, default_value_t = false)]
    compile: bool,
}

fn main() {
    let cli = Cli::parse();
    let rt = runtime::build().expect("Failed to build runtime");

    match cli.file {
        Some(file) => match run_file(&rt, &file, cli.compile) {
            Err(err) => {
                eprintln!("{}", err);
                if let Some(bt) = ErrorCompat::backtrace(&err) {
                    eprintln!("{}", bt);
                }
            }
            Ok(()) => {}
        },
        None => {
            if cli.compile {
                eprintln!("Cannot run with compile-only mode (--compile) in the repl");
                return;
            }
            crate::repl::run(&rt);
        }
    }
}

fn run_file(rt: &runtime::Runtime, file: &str, compile_only: bool) -> Result<(), Whatever> {
    let schema = compile::compile_schema_from_file(&Path::new(&file))
        .with_whatever_context(|e| format!("{}", e))?;

    if compile_only {
        println!("{:#?}", schema);
        return Ok(());
    }

    for expr in schema.borrow().exprs.iter() {
        let expr = expr
            .to_runtime_type()
            .with_whatever_context(|e| format!("{}", e))?;
        let value = rt.block_on(async {
            runtime::eval(schema.clone(), &expr)
                .await
                .with_whatever_context(|e| format!("{}", e))
        })?;
        println!("{:?}", value);
    }

    Ok(())
}
