use lazy_static::lazy_static;
use std::collections::BTreeMap;

use crate::ast::{Ident, SourceLocation};
use crate::compile::compile::Compiler;
use crate::compile::inference::mkcref;
use crate::compile::schema::{Decl, MType, Ref, Schema, SchemaEntry};
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

const BUILTIN_FUNCTIONS: &'static str = "
fn load<R>(file varchar, format varchar) -> [R] = native;
fn min<R>(value R) -> R = sql;
fn max<R>(value R) -> R = sql;
fn count<R>(value R) -> bigint = sql;
fn sum<R>(value R) -> R = sql;
";

lazy_static! {
    static ref BUILTIN_TYPE_DECLS: Vec<(String, Decl)> = BUILTIN_TYPES
        .iter()
        .map(|(name, type_)| (
            name.to_string(),
            Decl {
                public: true,
                extern_: false,
                name: Ident::without_location(name.to_string()),
                value: SchemaEntry::Type(mkcref(MType::Atom(
                    SourceLocation::File("<builtin>".to_string()),
                    type_.clone()
                ))),
            },
        ))
        .collect();
    pub static ref GLOBAL_SCHEMA: Ref<Schema> = {
        let ret = Schema::new("<builtin>".to_string(), None);
        ret.write().unwrap().decls = BTreeMap::from_iter(BUILTIN_TYPE_DECLS.clone().into_iter());

        let builtin_compiler = Compiler::new_with_builtins(ret.clone(), true).unwrap();
        builtin_compiler
            .compile_string(ret.clone(), BUILTIN_FUNCTIONS)
            .expect("Failed to compile builtin function defs");

        ret
    };
}
