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

// XXX
// NEXT STEPS
// Refactor function interface below (maybe make a macro?)
// Implement load_csv() (or maybe a load function which can load either?)
lazy_static! {
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
            vec![(
                "load".to_string(),
                Decl {
                    public: true,
                    extern_: false,
                    name: "load".to_string(),
                    value: SchemaEntry::Expr(mkcref(STypedExpr {
                        type_: mkcref(SType {
                            variables: BTreeSet::from(["R".to_string()]),
                            body: mkcref(MType::Fn(MFnType {
                                args: vec![
                                    MField {
                                        name: "file".to_string(),
                                        type_: mkcref(MType::Atom(AtomicType::Utf8)),
                                        nullable: false,
                                    },
                                    MField {
                                        name: "format".to_string(),
                                        type_: mkcref(MType::Atom(AtomicType::Utf8)),
                                        nullable: true,
                                    }
                                ],
                                ret: mkcref(MType::List(mkcref(MType::Name("R".to_string())))),
                            })),
                        }),
                        expr: mkcref(Expr::NativeFn("load".to_string())),
                    })),
                },
            )]
            .into_iter()
        )
        .collect();
    pub static ref GLOBAL_SCHEMA: Ref<Schema> = {
        let ret = Schema::new(None);
        ret.write().unwrap().decls = BTreeMap::from_iter(BUILTIN_DECLS.clone().into_iter());

        ret
    };
}
