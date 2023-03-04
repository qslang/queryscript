pub use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, IntervalUnit as ArrowIntervalUnit,
    Schema as ArrowSchema, TimeUnit as ArrowTimeUnit, DECIMAL128_MAX_PRECISION,
    DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
};

use sqlparser::ast::{
    DataType as ParserDataType, ExactNumberInfo as ParserNumberInfo, TimezoneInfo as ParserTz,
};

use super::error::{ts_fail, ts_unimplemented, Result};
use crate::ast::Ident;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub enum Type {
    Atom(AtomicType),
    Record(Vec<Field>),
    List(Box<Type>),
    Fn(FnType),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub struct FnType {
    pub args: Vec<Field>,
    pub ret: Box<Type>,
}

// These types are borrowed from Apache Arrow, which we closely integrate with but do not want
// to take a 100% dependency on. We expect to extend these types over time to include more
// QueryScript specific logic.

// From arrow-schema/src/field.rs
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub struct Field {
    #[cfg_attr(feature = "ts", ts(type = "String"))]
    pub name: Ident,
    pub type_: Type,
    pub nullable: bool,
}

impl Field {
    pub fn new_nullable(name: Ident, type_: Type) -> Field {
        Field {
            name,
            type_,
            nullable: true,
        }
    }
}

// From arrow-schema/src/datatype.rs
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
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
    // This is in the Arrow typesystem but not implemented in the ScalarValue enum in DataFusion
    /*
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    */
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
    Decimal128(u8, i8),
    /// Exact 256-bit width decimal value with precision and scale
    ///
    /// * precision is the total number of digits
    /// * scale is the number of digits past the decimal
    ///
    /// For example the number 123.45 has precision 5 and scale 2.
    Decimal256(u8, i8),
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

// TimeUnit and IntervalUnit are copied from Arrow
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub enum IntervalUnit {
    YearMonth,
    DayTime,
    MonthDayNano,
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(t: &ArrowTimeUnit) -> Self {
        match t {
            ArrowTimeUnit::Second => TimeUnit::Second,
            ArrowTimeUnit::Millisecond => TimeUnit::Millisecond,
            ArrowTimeUnit::Microsecond => TimeUnit::Microsecond,
            ArrowTimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl Into<ArrowTimeUnit> for &TimeUnit {
    fn into(self) -> ArrowTimeUnit {
        match self {
            TimeUnit::Second => ArrowTimeUnit::Second,
            TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
            TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
            TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
        }
    }
}

impl From<&ArrowIntervalUnit> for IntervalUnit {
    fn from(t: &ArrowIntervalUnit) -> Self {
        match t {
            ArrowIntervalUnit::YearMonth => IntervalUnit::YearMonth,
            ArrowIntervalUnit::DayTime => IntervalUnit::DayTime,
            ArrowIntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        }
    }
}

impl Into<ArrowIntervalUnit> for &IntervalUnit {
    fn into(self) -> ArrowIntervalUnit {
        match self {
            IntervalUnit::YearMonth => ArrowIntervalUnit::YearMonth,
            IntervalUnit::DayTime => ArrowIntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => ArrowIntervalUnit::MonthDayNano,
        }
    }
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
            Timestamp(u, s) => Type::Atom(AtomicType::Timestamp(u.into(), s.clone())),
            Date32 => Type::Atom(AtomicType::Date32),
            Date64 => Type::Atom(AtomicType::Date64),
            Time64(u) => Type::Atom(AtomicType::Time64(u.into())),
            Interval(u) => Type::Atom(AtomicType::Interval(u.into())),
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
            Struct(fields) => fields.try_into()?,
            Union(..) | Dictionary(..) | Map(..) | Time32(..) | Duration(..) => {
                return ts_unimplemented!("type {:?}", &t)
            }
        })
    }
}

impl TryInto<ArrowDataType> for &Type {
    type Error = super::error::TypesystemError;

    fn try_into(self) -> Result<ArrowDataType> {
        use AtomicType::*;
        use Type::*;
        Ok(match self {
            Atom(Null) => ArrowDataType::Null,
            Atom(Boolean) => ArrowDataType::Boolean,
            Atom(Int8) => ArrowDataType::Int8,
            Atom(Int16) => ArrowDataType::Int16,
            Atom(Int32) => ArrowDataType::Int32,
            Atom(Int64) => ArrowDataType::Int64,
            Atom(UInt8) => ArrowDataType::UInt8,
            Atom(UInt16) => ArrowDataType::UInt16,
            Atom(UInt32) => ArrowDataType::UInt32,
            Atom(UInt64) => ArrowDataType::UInt64,
            Atom(Float16) => ArrowDataType::Float16,
            Atom(Float32) => ArrowDataType::Float32,
            Atom(Float64) => ArrowDataType::Float64,
            Atom(Timestamp(tu, tz)) => ArrowDataType::Timestamp(tu.into(), tz.clone()),
            Atom(Date32) => ArrowDataType::Date32,
            Atom(Date64) => ArrowDataType::Date64,
            Atom(Time32(tu)) => ArrowDataType::Time32(tu.into()),
            Atom(Time64(tu)) => ArrowDataType::Time64(tu.into()),
            Atom(Interval(iu)) => ArrowDataType::Interval(iu.into()),
            Atom(Binary) => ArrowDataType::Binary,
            Atom(FixedSizeBinary(len)) => ArrowDataType::FixedSizeBinary(*len),
            Atom(LargeBinary) => ArrowDataType::LargeBinary,
            Atom(Utf8) => ArrowDataType::Utf8,
            Atom(LargeUtf8) => ArrowDataType::LargeUtf8,
            Atom(Decimal128(p, s)) => ArrowDataType::Decimal128(*p, *s),
            Atom(Decimal256(p, s)) => ArrowDataType::Decimal256(*p, *s),
            Record(fields) => ArrowDataType::Struct(
                fields
                    .iter()
                    .map(|x| Ok(x.try_into()?))
                    .collect::<Result<Vec<ArrowField>>>()?,
            ),
            List(data_type) => ArrowDataType::List(Box::new(ArrowField::new(
                "",
                data_type.as_ref().try_into()?,
                true,
            ))),
            Fn(_) => return ts_fail!("Arrow does not support function types"),
        })
    }
}

fn time_unit_precision(tu: &TimeUnit) -> u64 {
    match tu {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

impl TryInto<ParserDataType> for &Type {
    type Error = super::error::TypesystemError;

    fn try_into(self) -> Result<ParserDataType> {
        use AtomicType::*;
        use Type::*;
        Ok(match self {
            Atom(Null) => return ts_fail!("Parser does not support NULL type"),
            Atom(Boolean) => ParserDataType::Boolean,
            Atom(Int8) => ParserDataType::TinyInt(None),
            Atom(Int16) => ParserDataType::SmallInt(None),
            Atom(Int32) => ParserDataType::Int(None),
            Atom(Int64) => ParserDataType::BigInt(None),
            Atom(UInt8) => ParserDataType::UnsignedTinyInt(None),
            Atom(UInt16) => ParserDataType::UnsignedSmallInt(None),
            Atom(UInt32) => ParserDataType::UnsignedInt(None),
            Atom(UInt64) => ParserDataType::UnsignedBigInt(None),

            // The parser data types do not support Float16
            Atom(Float16) => ParserDataType::Float(None),
            Atom(Float32) => ParserDataType::Float(None),
            Atom(Float64) => ParserDataType::Double,
            Atom(Timestamp(tu, tz)) => ParserDataType::Timestamp(
                Some(time_unit_precision(tu)),
                tz.as_ref().map_or(ParserTz::None, |_| ParserTz::Tz),
            ),
            Atom(Date32) => ParserDataType::Date,
            Atom(Date64) => ParserDataType::Datetime(None),
            Atom(Time32(u)) => ParserDataType::Time(Some(time_unit_precision(u)), ParserTz::None),
            Atom(Time64(u)) => ParserDataType::Time(Some(time_unit_precision(u)), ParserTz::None),
            Atom(Interval(_)) => ParserDataType::Interval,
            Atom(Binary) => ParserDataType::Varbinary(None),
            Atom(FixedSizeBinary(len)) => ParserDataType::Binary(Some(*len as u64)),
            Atom(LargeBinary) => ParserDataType::Varbinary(None),
            Atom(Utf8) => ParserDataType::String,
            Atom(LargeUtf8) => ParserDataType::String,
            Atom(Decimal128(p, s)) => {
                ParserDataType::Decimal(ParserNumberInfo::PrecisionAndScale(*p as u64, *s as u64))
            }
            Atom(Decimal256(p, s)) => {
                ParserDataType::Decimal(ParserNumberInfo::PrecisionAndScale(*p as u64, *s as u64))
            }
            List(_data_type) => return ts_fail!("Parser does not support list types"),
            Record(_) => return ts_fail!("Parser does not support record types"),
            Fn(_) => return ts_fail!("Parser does not support function types"),
        })
    }
}

pub fn try_arrow_fields_to_fields(fields: &Vec<ArrowField>) -> Result<Vec<Field>> {
    Ok(fields
        .iter()
        .map(|f| {
            Ok(Field {
                name: f.name().clone().into(),
                type_: f.data_type().try_into()?,
                nullable: f.is_nullable(),
            })
        })
        .collect::<Result<Vec<_>>>()?)
}

impl TryFrom<&Vec<ArrowField>> for Type {
    type Error = super::error::TypesystemError;
    fn try_from(fields: &Vec<ArrowField>) -> Result<Self> {
        Ok(Type::Record(try_arrow_fields_to_fields(fields)?))
    }
}

impl TryFrom<&ArrowSchema> for Type {
    type Error = super::error::TypesystemError;
    fn try_from(schema: &ArrowSchema) -> Result<Self> {
        (&schema.fields).try_into()
    }
}

impl TryInto<ArrowField> for &Field {
    type Error = super::error::TypesystemError;
    fn try_into(self) -> Result<ArrowField> {
        Ok(ArrowField::new(
            self.name.as_str(),
            (&self.type_).try_into()?,
            self.nullable,
        ))
    }
}

pub fn try_fields_to_arrow_fields(fields: &Vec<Field>) -> Result<Vec<ArrowField>> {
    fields
        .iter()
        .map(|f| f.try_into())
        .collect::<Result<Vec<ArrowField>>>()
}

impl TryInto<ArrowSchema> for &Type {
    type Error = super::error::TypesystemError;
    fn try_into(self) -> Result<ArrowSchema> {
        match self {
            Type::Record(s) => Ok(ArrowSchema::new(
                s.iter()
                    .map(|f| Ok(f.try_into()?))
                    .collect::<Result<Vec<ArrowField>>>()?,
            )),
            Type::List(dt) => dt.as_ref().try_into(),
            t => ts_fail!("Cannot convert type {:?} to record", t),
        }
    }
}

impl TryFrom<&sqlparser::ast::DataType> for Type {
    type Error = super::error::TypesystemError;

    fn try_from(t: &sqlparser::ast::DataType) -> Result<Self> {
        use sqlparser::ast::DataType::*;
        Ok(match t {
            Character(_)
            | Char(_)
            | CharacterVarying(_)
            | CharVarying(_)
            | Varchar(_)
            | Nvarchar(_)
            | Uuid
            | CharacterLargeObject(_)
            | CharLargeObject(_)
            | Clob(_)
            | Text
            | String => Type::Atom(AtomicType::Utf8), // Our / Arrow's typesystem does not support fixed size strings?

            Binary(_) | Varbinary(_) | Blob(_) | Bytea => Type::Atom(AtomicType::Binary),

            Numeric(ni) | Decimal(ni) | Dec(ni) | BigNumeric(ni) | BigDecimal(ni) => {
                use sqlparser::ast::ExactNumberInfo::*;
                let (p, s) = match ni {
                    None => (DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE),
                    Precision(p) => (*p as u8, DECIMAL128_MAX_SCALE),
                    PrecisionAndScale(p, s) => (*p as u8, *s as i8),
                };
                Type::Atom(AtomicType::Decimal128(p, s))
            }

            Float(p) => match p {
                None => Type::Atom(AtomicType::Float64),
                Some(i) if *i <= 24 => Type::Atom(AtomicType::Float32),
                Some(i) if *i > 24 && *i <= 53 => Type::Atom(AtomicType::Float64),
                Some(_) => return ts_fail!("Unsupported float precision"),
            },

            Real => Type::Atom(AtomicType::Float32),
            Double | DoublePrecision => Type::Atom(AtomicType::Float64),

            TinyInt(_) => Type::Atom(AtomicType::Int8),
            UnsignedTinyInt(_) => Type::Atom(AtomicType::UInt8),
            SmallInt(_) => Type::Atom(AtomicType::Int16),
            UnsignedSmallInt(_) => Type::Atom(AtomicType::UInt16),
            MediumInt(_) => Type::Atom(AtomicType::Int32), // We don't support Int24
            UnsignedMediumInt(_) => Type::Atom(AtomicType::UInt32), // We don't support Int24
            Int(_) | Integer(_) => Type::Atom(AtomicType::Int32),
            UnsignedInt(_) | UnsignedInteger(_) => Type::Atom(AtomicType::UInt32),
            BigInt(_) => Type::Atom(AtomicType::Int64),
            UnsignedBigInt(_) => Type::Atom(AtomicType::UInt64),

            Boolean => Type::Atom(AtomicType::Boolean),

            // TODO: Match this more closely
            Time(..) => Type::Atom(AtomicType::Time64(TimeUnit::Microsecond)),
            Date => Type::Atom(AtomicType::Date32),
            Timestamp(..) => Type::Atom(AtomicType::Timestamp(TimeUnit::Microsecond, None)),
            Datetime(..) => Type::Atom(AtomicType::Timestamp(TimeUnit::Second, None)),

            Interval => Type::Atom(AtomicType::Interval(IntervalUnit::YearMonth)), // Unclear what unit is implied from the parser
            Array(inner_type) => Type::List(Box::new(match inner_type {
                Some(inner_type) => inner_type.as_ref().try_into()?,
                None => return ts_unimplemented!("Array of unknown type"),
            })),

            Regclass => return ts_unimplemented!("Regclass"),
            Custom(..) => return ts_unimplemented!("Custom types"),
            Enum(..) => return ts_unimplemented!("Enum types"),
            Set(..) => return ts_unimplemented!("Set types"),
            JSON => return ts_unimplemented!("JSON types"),
        })
    }
}
