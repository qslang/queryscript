use clap::{Parser, Subcommand};
use snafu::ErrorCompat;
use std::fs;
use std::path::Path;

use qvm::compile;
use qvm::parser;

mod repl;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Parse { file: String },
    Compile { file: String },
    Repl,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Parse { file } => {
            let contents = fs::read_to_string(file).expect("Unable to read file");
            eprintln!(
                "{:#?}",
                parser::parse_schema(&contents).expect("Parser failed")
            );
        }
        Commands::Compile { file } => match compile::compile_schema_from_file(&Path::new(&file)) {
            Err(err) => {
                eprintln!("{}", err);
                if let Some(bt) = ErrorCompat::backtrace(&err) {
                    eprintln!("{}", bt);
                }
            }
            Ok(schema) => eprintln!("{:#?}", schema),
        },
        Commands::Repl => {
            crate::repl::run();
        }
    }
}
