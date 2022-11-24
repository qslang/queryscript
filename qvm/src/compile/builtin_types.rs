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
    // Date/Time. Time precision is defaulted to microseconds
    // but we should make this configurable.
    //
    ("date", AtomicType::Date64),
    ("time", AtomicType::Time64(TimeUnit::Microsecond)),
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
                "load_json".to_string(),
                Decl {
                    public: true,
                    extern_: false,
                    name: "load_json".to_string(),
                    value: SchemaEntry::Expr(mkcref(STypedExpr {
                        type_: mkcref(SType {
                            variables: BTreeSet::from(["R".to_string()]),
                            body: mkcref(MType::Fn(MFnType {
                                args: vec![MField {
                                    name: "file".to_string(),
                                    type_: mkcref(MType::Atom(AtomicType::Utf8)),
                                    nullable: false,
                                }],
                                ret: mkcref(MType::List(mkcref(MType::Name("R".to_string())))),
                            })),
                        }),
                        expr: mkcref(Expr::NativeFn("load_json".to_string())),
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
