use lazy_static::lazy_static;
use std::collections::BTreeMap;

use crate::ast::{Ident, SourceLocation};
use crate::compile::compile::{Compiler, CompilerConfig};
use crate::compile::inference::mkcref;
use crate::compile::schema::{Decl, Located, MType, Ref, Schema, TypeEntry};
use crate::types::{AtomicType, TimeUnit};

pub use crate::compile::generics::GLOBAL_GENERICS;

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
    ("hugeint", AtomicType::Decimal128(38, 0)), // XXX This is a hack because DuckDB's sum returns this
    ("float", AtomicType::Float32),
    ("double", AtomicType::Float64),
    //
    // Strings
    //
    ("string", AtomicType::Utf8),
    ("text", AtomicType::Utf8),
    ("varchar", AtomicType::Utf8),
    ("blob", AtomicType::Binary),
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
    ("datetime", AtomicType::Timestamp(TimeUnit::Second, None)),
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
fn load<R>(file varchar, format varchar) -> External<[R]> = native;
fn __native_identity<T>(value T) -> T = native;

-- Functions
fn abs<R>(value R) -> R = sql;
fn concat(args ...string) -> string = sql;
fn current_date() -> timestamp = sql;
fn date_trunc(unit string, value timestamp) -> timestamp = sql;
fn ifnull<R>(value R, default R) -> R = sql;
fn json_extract_string(value string, path string) -> string = sql;
fn strptime(value text, fmt string) -> timestamp = sql;

-- Aggs
fn min<R>(value R) -> R = sql;
fn max<R>(value R) -> R = sql;
fn count<R>(value R) -> bigint = sql;
fn sum<R>(value R) -> SumAgg<R> = sql;
fn avg<R>(value R) -> double = sql;
fn grouping<R>(value R) -> bigint = sql;

-- Window functions
fn row_number<R>() -> bigint = sql;
fn lead<R, N>(expr R, offset N, default R) -> R = sql;
fn lag<R, N>(expr R, offset N, default R) -> R = sql;
";

const AGG_NAME_STRS: &'static [&'static str] = &["min", "max", "count", "sum", "avg", "grouping"];

lazy_static! {
    pub static ref BUILTIN_LOC: SourceLocation = SourceLocation::File("<builtin>".to_string());
    // There is definitely a better way to do this (e.g. store this info in the compiler)
    // or somehow in the SQL tree
    pub static ref AGG_NAMES: Vec<Ident> = AGG_NAME_STRS
        .iter()
        .map(|name| Ident::from(name.to_string()))
        .collect();
    static ref BUILTIN_TYPE_DECLS: Vec<(Ident, Decl<TypeEntry>)> = BUILTIN_TYPES
        .iter()
        .map(|(name, type_)| (
            name.to_string().into(),
            Decl {
                public: true,
                extern_: false,
                is_arg: false,
                name: Ident::with_location(BUILTIN_LOC.clone(), name.to_string()),
                value: mkcref(MType::Atom(Located::new(
                    type_.clone(),
                    BUILTIN_LOC.clone()
                ))),
            },
        ))
        .collect();
    pub static ref GLOBAL_SCHEMA: Ref<Schema> = {
        let ret = Schema::new("<builtin>".to_string(), None);
        ret.write().unwrap().type_decls = BTreeMap::from_iter(
            BUILTIN_TYPE_DECLS
                .clone()
                .into_iter()
                .map(|(name, decl)| (name, Located::new(decl, SourceLocation::Unknown))),
        );

        let builtin_compiler = Compiler::new_with_builtins(
            ret.clone(),
            CompilerConfig {
                allow_native: true,
                ..Default::default()
            },
        )
        .unwrap();
        builtin_compiler
            .compile_string(ret.clone(), BUILTIN_FUNCTIONS)
            .expect("Failed to compile builtin function defs");

        ret
    };
}
