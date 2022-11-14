use datafusion::arrow::datatypes::{DataType as ArrowDataType, IntervalUnit, TimeUnit};
use std::cell::RefCell;
use std::rc::Rc;

use super::error::{ts_unimplemented, Result};
use crate::ast;

pub type Ident = ast::Ident;
pub type TypeRef = Rc<RefCell<Type>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Type {
    Unknown,
    Atom(AtomicType),
    Struct(Vec<Field>),
    List(Box<Type>),
    Exclude {
        inner: Box<Type>,
        excluded: Vec<Ident>,
    },
    Fn(FnType),
    Ref(TypeRef),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FnType {
    pub args: Vec<Field>,
    pub ret: Box<Type>,
}

// These types are borrowed from Apache Arrow, which we closely integrate with but do not want
// to take a 100% dependency on. We expect to extend these types over time to include more
// QVM specific logic.

// From arrow-schema/src/field.rs
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Field {
    pub name: String,
    pub type_: Type,
    pub nullable: bool,
}

impl Field {
    pub fn new_nullable(name: String, type_: Type) -> Field {
        Field {
            name,
            type_,
            nullable: true,
        }
    }
}

// From arrow-schema/src/datatype.rs
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AtomicType {
    /// Null type
    Null,
    /// A boolean datatype representing the values `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// A timestamp with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a 64-bit integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    ///
    /// Timestamps with a non-empty timezone
    /// ------------------------------------
    ///
    /// If a Timestamp column has a non-empty timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in the *UTC* timezone
    /// (the Unix epoch), regardless of the Timestamp's own timezone.
    ///
    /// Therefore, timestamp values with a non-empty timezone correspond to
    /// physical points in time together with some additional information about
    /// how the data was obtained and/or how to display it (the timezone).
    ///
    ///   For example, the timestamp value 0 with the timezone string "Europe/Paris"
    ///   corresponds to "January 1st 1970, 00h00" in the UTC timezone, but the
    ///   application may prefer to display it as "January 1st 1970, 01h00" in
    ///   the Europe/Paris timezone (which is the same physical point in time).
    ///
    /// One consequence is that timestamp values with a non-empty timezone
    /// can be compared and ordered directly, since they all share the same
    /// well-known point of reference (the Unix epoch).
    ///
    /// Timestamps with an unset / empty timezone
    /// -----------------------------------------
    ///
    /// If a Timestamp column has no timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone.
    ///
    /// Therefore, timestamp values without a timezone cannot be meaningfully
    /// interpreted as physical points in time, but only as calendar / clock
    /// indications ("wall clock time") in an unspecified timezone.
    ///
    ///   For example, the timestamp value 0 with an empty timezone string
    ///   corresponds to "January 1st 1970, 00h00" in an unknown timezone: there
    ///   is not enough information to interpret it as a well-defined physical
    ///   point in time.
    ///
    /// One consequence is that timestamp values without a timezone cannot
    /// be reliably compared or ordered, since they may have different points of
    /// reference.  In particular, it is *not* possible to interpret an unset
    /// or empty timezone as the same as "UTC".
    ///
    /// Conversion between timezones
    /// ----------------------------
    ///
    /// If a Timestamp column has a non-empty timezone, changing the timezone
    /// to a different non-empty value is a metadata-only operation:
    /// the timestamp values need not change as their point of reference remains
    /// the same (the Unix epoch).
    ///
    /// However, if a Timestamp column has no timezone value, changing it to a
    /// non-empty value requires to think about the desired semantics.
    /// One possibility is to assume that the original timestamp values are
    /// relative to the epoch of the timezone being set; timestamp values should
    /// then adjusted to the Unix epoch (for example, changing the timezone from
    /// empty to "Europe/Paris" would require converting the timestamp values
    /// from "Europe/Paris" to "UTC", which seems counter-intuitive but is
    /// nevertheless correct).
    Timestamp(TimeUnit, Option<String>),
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits).
    Date32,
    /// A 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds (64 bits). Values are evenly divisible by 86400000.
    Date64,
    /// A 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    Time32(TimeUnit),
    /// A 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    Time64(TimeUnit),
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    /// A "calendar" interval which models types that don't necessarily
    /// have a precise duration without the context of a base timestamp (e.g.
    /// days can differ in length during day light savings time transitions).
    Interval(IntervalUnit),
    /// Opaque binary data of variable length.
    Binary,
    /// Opaque binary data of fixed size.
    /// Enum parameter specifies the number of bytes per value.
    FixedSizeBinary(i32),
    /// Opaque binary data of variable length and 64-bit offsets.
    LargeBinary,
    /// A variable-length string in Unicode with UTF-8 encoding.
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    LargeUtf8,
    /// Exact 128-bit width decimal value with precision and scale
    ///
    /// * precision is the total number of digits
    /// * scale is the number of digits past the decimal
    ///
    /// For example the number 123.45 has precision 5 and scale 2.
    Decimal128(u8, u8),
    /// Exact 256-bit width decimal value with precision and scale
    ///
    /// * precision is the total number of digits
    /// * scale is the number of digits past the decimal
    ///
    /// For example the number 123.45 has precision 5 and scale 2.
    Decimal256(u8, u8),
    //
    // The Arrow DataType struct contains several composite types, which we break out
    // this enum. Several of them we exclude entirely.
    // List and Struct are in the top-level type field
    // FixedSizeList, LargeList, and Map are ignored (because I think they are just
    // implementation-specific representations of List and Struct)
    // TODO Union and Dictionary are two useful types that we should eventually implement.
    //
    /*
    /// A list of some logical data type with variable length.
    List(Box<Field>),
    /// A list of some logical data type with fixed length.
    FixedSizeList(Box<Field>, i32),
    /// A list of some logical data type with variable length and 64-bit offsets.
    LargeList(Box<Field>),
    /// A nested datatype that contains a number of sub-fields.
    Struct(Vec<Field>),
    /// A nested datatype that can represent slots of differing types. Components:
    ///
    /// 1. [`Field`] for each possible child type the Union can hold
    /// 2. The corresponding `type_id` used to identify which Field
    /// 3. The type of union (Sparse or Dense)
    Union(Vec<Field>, Vec<i8>, UnionMode),
    /// A dictionary encoded array (`key_type`, `value_type`), where
    /// each array element is an index of `key_type` into an
    /// associated dictionary of `value_type`.
    ///
    /// Dictionary arrays are used to store columns of `value_type`
    /// that contain many repeated values using less memory, but with
    /// a higher CPU overhead for some operations.
    ///
    /// This type mostly used to represent low cardinality string
    /// arrays or a limited set of primitive types as integers.
    Dictionary(Box<DataType>, Box<DataType>),
    /// A Map is a logical nested type that is represented as
    ///
    /// `List<entries: Struct<key: K, value: V>>`
    ///
    /// The keys and values are each respectively contiguous.
    /// The key and value types are not constrained, but keys should be
    /// hashable and unique.
    /// Whether the keys are sorted can be set in the `bool` after the `Field`.
    ///
    /// In a field with Map type, the field has a child Struct field, which then
    /// has two children: key type and the second the value type. The names of the
    /// child fields may be respectively "entries", "key", and "value", but this is
    /// not enforced.
    Map(Box<Field>, bool),
    */
}

impl TryFrom<&ArrowDataType> for Type {
    type Error = super::error::TypesystemError;

    fn try_from(t: &ArrowDataType) -> Result<Self> {
        use ArrowDataType::*;
        Ok(match t {
            Null => Type::Atom(AtomicType::Null),
            Boolean => Type::Atom(AtomicType::Boolean),
            Int8 => Type::Atom(AtomicType::Int8),
            Int16 => Type::Atom(AtomicType::Int16),
            Int32 => Type::Atom(AtomicType::Int32),
            Int64 => Type::Atom(AtomicType::Int64),
            UInt8 => Type::Atom(AtomicType::UInt8),
            UInt16 => Type::Atom(AtomicType::UInt16),
            UInt32 => Type::Atom(AtomicType::UInt32),
            UInt64 => Type::Atom(AtomicType::UInt64),
            Float16 => Type::Atom(AtomicType::Float16),
            Float32 => Type::Atom(AtomicType::Float32),
            Float64 => Type::Atom(AtomicType::Float64),
            Timestamp(u, s) => Type::Atom(AtomicType::Timestamp(u.clone(), s.clone())),
            Date32 => Type::Atom(AtomicType::Date32),
            Date64 => Type::Atom(AtomicType::Date64),
            Time32(u) => Type::Atom(AtomicType::Time32(u.clone())),
            Time64(u) => Type::Atom(AtomicType::Time64(u.clone())),
            Duration(u) => Type::Atom(AtomicType::Duration(u.clone())),
            Interval(u) => Type::Atom(AtomicType::Interval(u.clone())),
            Binary => Type::Atom(AtomicType::Binary),
            FixedSizeBinary(l) => Type::Atom(AtomicType::FixedSizeBinary(*l)),
            LargeBinary => Type::Atom(AtomicType::LargeBinary),
            Utf8 => Type::Atom(AtomicType::Utf8),
            LargeUtf8 => Type::Atom(AtomicType::LargeUtf8),
            Decimal128(p, s) => Type::Atom(AtomicType::Decimal128(*p, *s)),
            Decimal256(p, s) => Type::Atom(AtomicType::Decimal256(*p, *s)),
            List(f) | LargeList(f) | FixedSizeList(f, _) => {
                Type::List(Box::new(f.data_type().try_into()?))
            }
            Struct(fields) => Type::Struct(
                fields
                    .iter()
                    .map(|f| {
                        Ok(Field {
                            name: f.name().clone(),
                            type_: f.data_type().try_into()?,
                            nullable: f.is_nullable(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),

            Union(..) | Dictionary(..) | Map(..) => return ts_unimplemented!("union type"),
        })
    }
}
