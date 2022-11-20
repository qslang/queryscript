use clap::Parser;
use snafu::ErrorCompat;
use std::path::Path;

use qvm::compile;

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
        Some(file) => match compile::compile_schema_from_file(&Path::new(&file)) {
            Err(err) => {
                eprintln!("{}", err);
                if let Some(bt) = ErrorCompat::backtrace(&err) {
                    eprintln!("{}", bt);
                }
            }
            Ok(schema) => eprintln!("{:#?}", schema),
        },
        None => {
            crate::repl::run();
        }
    }
}
