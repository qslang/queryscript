use lazy_static::lazy_static;
use std::collections::{BTreeMap, BTreeSet};

use crate::ast;
use crate::compile::compile::{compile_schema_ast, Compiler};
use crate::compile::inference::mkcref;
use crate::compile::schema::{
    Decl, Expr, MField, MFnType, MType, Ref, SType, STypedExpr, Schema, SchemaEntry,
};
use crate::parser;
use crate::types::{AtomicType, TimeUnit};

// TODO: Add support for types with arguments
type BuiltinType = (&'static str, AtomicType);
const BUILTIN_TYPES: &'static [BuiltinType] = &[
    //
    // Numbers
    //
    ("number", AtomicType::Float64),
    ("tinyint", AtomicType::Int8),
    ("smallint", AtomicType::Int16),
    ("int", AtomicType::Int32),
    ("bigint", AtomicType::Int64),
    ("float", AtomicType::Float32),
    ("double", AtomicType::Float64),
    //
    // Strings
    //
    ("string", AtomicType::Utf8),
    ("text", AtomicType::Utf8),
    ("varchar", AtomicType::Utf8),
    //
    // Date/Time:
    // - The parser in rust arrow expects Date32 to be a date (no time) and
    //   Date64 to be a date and time (w/ optional timezone)
    //   https://github.com/apache/arrow-rs/blob/27.0.0/arrow-cast/src/parse.rs#L224
    // - The time types are defaulted to microsecond precision, but we should make
    //   this configurable.
    // - The timestamp has no timezone, but we should make this configurable.
    //
    ("date", AtomicType::Date32),
    ("time", AtomicType::Time64(TimeUnit::Microsecond)),
    ("datetime", AtomicType::Date64),
    (
        "timestamp",
        AtomicType::Timestamp(TimeUnit::Microsecond, None),
    ),
    //
    // Other
    //
    ("bool", AtomicType::Boolean),
    ("null", AtomicType::Null),
];

macro_rules! arg {
    ($name: literal, $type_: expr) => {
        MField {
            name: $name.to_string(),
            type_: mkcref($type_),
            nullable: false,
        }
    };
    ($name: literal, $type_: expr, $nullable: literal) => {
        MField {
            name: $name.to_string(),
            type_: mkcref($type_),
            nullable: $nullable,
        }
    };
}

type BuiltinFunction = (String, Vec<String>, Vec<MField>, MType);
pub const NATIVE_FN_NAME: &'static str = "__native";

/*
pub fn parse_fn_decl(decl: &'static str) -> BuiltinFunction {
    let (tokens, eof) = parser::tokenize(decl).expect("failed to tokenize builtin function");
    let mut parser = parser::Parser::new(tokens, eof);
    let fndef = match parser.parse_fn().expect("failed to parse function") {
        ast::StmtBody::FnDef {
            name,
            generics,
            args,
            ret,
            ..
        } => (name, generics, args, ret),
        o => panic!("Non-function {:?}", o),
    };

    BuiltinFunction(name, generics, args.map(|a| MField {
        name,
        type_.into()
    }
}
*/

lazy_static! {
    // A function is a name, generic variables, args, and a return type
    static ref BUILTIN_FUNCTIONS: Vec<BuiltinFunction> = vec![
        ("load".to_string(), vec!["R".to_string()], vec![
            arg!("file", MType::Atom(AtomicType::Utf8)),
            arg!("format", MType::Atom(AtomicType::Utf8), true)
        ], MType::List(mkcref(MType::Name("R".to_string())))),
    ];

    static ref BUILTIN_TYPE_DECLS : Vec<(String, Decl)> = BUILTIN_TYPES
        .iter()
        .map(|(name, type_)| (
            name.to_string(),
            Decl {
                public: true,
                extern_: false,
                name: name.to_string(),
                value: SchemaEntry::Type(mkcref(MType::Atom(type_.clone()))),
            },
        ))
        .collect()
        ;

    pub static ref GLOBAL_COMPILER: Compiler = Compiler::new().unwrap();
    pub static ref GLOBAL_SCHEMA: Ref<Schema> = {
        let ret = Schema::new(None);
        ret.write().unwrap().decls = BTreeMap::from_iter(BUILTIN_TYPE_DECLS.clone().into_iter());

        eprintln!("INITIALIZING GLOBAL SCHEMA");
        let builtin_compiler = Compiler::new_with_builtins(ret.clone()).unwrap();
        eprintln!("INITIALIZED GLOBAL COMPILER");

        let (tokens, eof) = parser::tokenize("fn load<R>(file varchar, format varchar) -> [R] { __native('load') }").expect("failed to tokenize builtin function");
        let mut parser = parser::Parser::new(tokens, eof);
        let schema_ast = parser.parse_schema().expect("Failed to parse builtin function defs");
        eprintln!("Time to compile: {:#?}", schema_ast);
        builtin_compiler.compile_schema_ast(ret.clone(), &schema_ast).expect("Failed to compile builtin function defs");

        eprintln!("Compiled {:#?}", ret);
        ret
    };

    /*
        .chain(
            BUILTIN_FUNCTIONS.iter().map(
                |(name, variables, args, ret)|
            (
                name.to_string(),
                Decl {
                    public: true,
                    extern_: false,
                    name: name.to_string(),
                    value: SchemaEntry::Expr(mkcref(STypedExpr {
                        type_: mkcref(SType {
                            variables: BTreeSet::from_iter(variables.iter().map(|s| s.to_string())),
                            body: mkcref(MType::Fn(MFnType {
                                args: args.clone(),
                                ret: mkcref(ret.clone()),
                            })),
                        }),
                        expr: mkcref(Expr::NativeFn(name.to_string())),
                    })),
                },
            ))
        )
        .collect();
    */
}
