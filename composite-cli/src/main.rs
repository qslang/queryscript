use clap::{Parser, Subcommand};
use std::fs;

use composite::compile;
use composite::parser;

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
        Commands::Compile { file } => {
            let schema = compile::compile_schema_from_file(&file).expect("Compilation error");
            eprintln!("{:#?}", schema);
        }
    }
}
