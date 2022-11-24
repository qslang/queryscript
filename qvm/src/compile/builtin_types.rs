use lazy_static::lazy_static;
use std::collections::{BTreeMap, BTreeSet};

use crate::compile::inference::mkcref;
use crate::compile::schema::{
    Decl, Expr, MField, MFnType, MType, Ref, SType, STypedExpr, Schema, SchemaEntry,
};
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

type BuiltinFunction = (&'static str, &'static [&'static str], Vec<MField>, MType);

lazy_static! {
    // A function is a name, generic variables, args, and a return type
    static ref BUILTIN_FUNCTIONS: Vec<BuiltinFunction> = vec![
        ("load", &["R"], vec![
            arg!("file", MType::Atom(AtomicType::Utf8)),
            arg!("format", MType::Atom(AtomicType::Utf8), true)
        ], MType::List(mkcref(MType::Name("R".to_string())))),
    ];

    static ref BUILTIN_DECLS: Vec<(String, Decl)> = BUILTIN_TYPES
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
    pub static ref GLOBAL_SCHEMA: Ref<Schema> = {
        let ret = Schema::new(None);
        ret.write().unwrap().decls = BTreeMap::from_iter(BUILTIN_DECLS.clone().into_iter());

        ret
    };
}
