use composite::parser;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let text = &args[1];

    println!("{:#?}", parser::parse(text).expect("Parser failed"));
}
