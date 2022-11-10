use clap::{Parser, Subcommand};
use std::fs;

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
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Parse { file } => {
            let contents = fs::read_to_string(file).expect("Unable to read file");
            eprintln!("{:#?}", parser::parse(&contents).expect("Parser failed"));
        }
    }
}
