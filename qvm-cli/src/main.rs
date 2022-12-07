use clap::Parser;
use snafu::whatever;
use snafu::ErrorCompat;
use std::fs;
use std::path::Path;

use qvm::compile;
use qvm::parser;
use qvm::runtime;
use qvm::QVMError;

mod readline_helper;
mod repl;

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
}

enum Mode {
    Execute,
    Compile,
    Parse,
}

fn main() {
    let cli = Cli::parse();
    if cli.verbose {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    let rt = runtime::build().expect("Failed to build runtime");

    if cli.compile && cli.parse {
        eprintln!("Cannot run with --compile and --parse");
        return;
    }

    let mode = if cli.compile {
        Mode::Compile
    } else if cli.parse {
        Mode::Parse
    } else {
        Mode::Execute
    };

    match cli.file {
        Some(file) => match run_file(&rt, &file, mode) {
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
            if cli.parse {
                eprintln!("Cannot run with parse-only mode (--parse) in the repl");
                return;
            }
            crate::repl::run(&rt);
        }
    }
}

fn run_file(rt: &runtime::Runtime, file: &str, mode: Mode) -> Result<(), QVMError> {
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

    let compiler = compile::Compiler::new()?;
    let schema = compiler
        .compile_schema_from_file(&Path::new(&file))
        .as_result()?
        .unwrap();

    if matches!(mode, Mode::Compile) {
        println!("{:#?}", schema);
        return Ok(());
    }

    let ctx = (&schema).into();
    let locked_schema = schema.read()?;
    for expr in locked_schema.exprs.iter() {
        let expr = expr.to_runtime_type()?;
        let value = rt.block_on(async { runtime::eval(&ctx, &expr).await })?;
        println!("{}", value);
    }

    Ok(())
}
