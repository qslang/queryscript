use clap::Parser;
use snafu::whatever;
use snafu::ErrorCompat;
use std::path::Path;

use qvm::compile;
use qvm::runtime;
use qvm::QVMError;

mod repl;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    file: Option<String>,

    #[arg(short, long, default_value_t = false)]
    compile: bool,

    #[arg(short, long, default_value_t = false)]
    verbose: bool,
}

fn main() {
    let cli = Cli::parse();
    if cli.verbose {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    let rt = runtime::build().expect("Failed to build runtime");

    match cli.file {
        Some(file) => match run_file(&rt, &file, cli.compile) {
            Err(err) => {
                eprintln!("{}", err);
                if cli.verbose {
                    if let Some(bt) = ErrorCompat::backtrace(&err) {
                        eprintln!("{:?}", bt);
                    }
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

fn run_file(rt: &runtime::Runtime, file: &str, compile_only: bool) -> Result<(), QVMError> {
    let path = Path::new(&file);
    if !path.exists() {
        whatever!("Path {:?} does not exist", path);
    }

    let schema = compile::compile_schema_from_file(&Path::new(&file))?;

    if compile_only {
        println!("{:#?}", schema);
        return Ok(());
    }

    let ctx = (&schema).into();
    let locked_schema = schema.read()?;
    for expr in locked_schema.exprs.iter() {
        let expr = expr.to_runtime_type()?;
        let value = rt.block_on(async { runtime::eval(&ctx, &expr).await })?;
        println!("{:?}", value);
    }

    Ok(())
}
