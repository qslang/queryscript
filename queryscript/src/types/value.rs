pub use arrow::{
    array::Array as ArrowArray,
    array::ArrayRef as ArrowArrayRef,
    datatypes::{
        Date32Type as ArrowDate32Type, Date64Type as ArrowDate64Type, DECIMAL128_MAX_PRECISION,
        DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    },
    record_batch::RecordBatch as ArrowRecordBatch,
};
pub use arrow_buffer::i256;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use dyn_clone::{clone_trait_object, DynClone};
use futures::future::BoxFuture;
pub use std::any::Any;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
pub use std::sync::Arc;
use tabled::builder::Builder as TableBuilder;

use super::error::Result;
use super::types::*;

#[derive(fmt::Debug, Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float16(half::f16),
    Float32(f32),
    Float64(f64),

    Decimal128(i128),
    Decimal256(i256),

    Utf8(String),
    LargeUtf8(String),
    Binary(Vec<u8>),
    FixedSizeBinary(i32, Vec<u8>),
    LargeBinary(Vec<u8>),

    // We only pull over one timestamp type (nanosecods)
    // and aim to represent all timestamps this way. We can have
    // other representations down the line as needed
    TimestampSecond(i64, Option<String>),
    TimestampMillisecond(i64, Option<String>),
    TimestampMicrosecond(i64, Option<String>),
    TimestampNanosecond(i64, Option<String>),

    Date32(i32),
    Date64(i64),

    Time32Second(i32),
    Time32Millisecond(i32),
    Time64Microsecond(i64),
    Time64Nanosecond(i64),

    IntervalYearMonth(i32),
    IntervalDayTime(i64),
    IntervalMonthDayNano(i128),

    // These are the structural types in QueryScript that deviate from DataFusion
    Record(Arc<dyn Record>),
    Relation(Arc<dyn Relation>),
    List(Arc<dyn List>),
    Fn(Arc<dyn FnValue>),
}

pub trait Record: fmt::Debug + Send + Sync {
    fn schema(&self) -> Vec<Field>;
    fn as_any(&self) -> &dyn Any;
    fn column(&self, index: usize) -> &Value;
}

pub trait List: fmt::Debug + Send + Sync {
    fn data_type(&self) -> Type;
    fn as_any(&self) -> &dyn Any;

    // TODO: This should eventually be changed to have the standard
    // array-like methods (indexing, etc.)
    fn as_vec(&self) -> Vec<Value>;
}

#[async_trait]
pub trait FnValue: fmt::Debug + DynClone + Send + Sync {
    // NOTE: We may eventually need to tease apart this trait into a pure
    // runtime trait if we separate the runtime crate.
    fn execute<'a>(
        &'a self,
        ctx: &'a mut crate::runtime::Context,
        args: Vec<Value>,
    ) -> BoxFuture<'a, crate::runtime::Result<Value>>;
    fn fn_type(&self) -> FnType;
    fn as_any(&self) -> &dyn Any;
}

pub trait Relation: fmt::Debug + Send + Sync {
    fn schema(&self) -> Vec<Field>;
    fn as_any(&self) -> &dyn Any;

    fn num_batches(&self) -> usize;
    fn batch(&self, index: usize) -> &dyn RecordBatch;

    fn records(&self) -> Vec<Arc<dyn Record>> {
        (0..self.num_batches())
            .map(|i| self.batch(i).records())
            .flatten()
            .collect()
    }

    fn try_cast(&self, target_schema: &Vec<Field>) -> Result<Arc<dyn Relation>>;
}

pub trait RecordBatch: fmt::Debug + Send + Sync {
    fn schema(&self) -> Vec<Field>;
    fn as_any(&self) -> &dyn Any;

    fn records(&self) -> Vec<Arc<dyn Record>>;
    fn as_arrow_recordbatch(&self) -> &ArrowRecordBatch;
}

clone_trait_object!(FnValue);

impl Value {
    pub fn type_(&self) -> Type {
        match self {
            Self::Null => Type::Atom(AtomicType::Null),
            Self::Boolean(_) => Type::Atom(AtomicType::Boolean),
            Self::Int8(_) => Type::Atom(AtomicType::Int8),
            Self::Int16(_) => Type::Atom(AtomicType::Int16),
            Self::Int32(_) => Type::Atom(AtomicType::Int32),
            Self::Int64(_) => Type::Atom(AtomicType::Int64),
            Self::UInt8(_) => Type::Atom(AtomicType::UInt8),
            Self::UInt16(_) => Type::Atom(AtomicType::UInt16),
            Self::UInt32(_) => Type::Atom(AtomicType::UInt32),
            Self::UInt64(_) => Type::Atom(AtomicType::UInt64),
            Self::Float16(_) => Type::Atom(AtomicType::Float16),
            Self::Float32(_) => Type::Atom(AtomicType::Float32),
            Self::Float64(_) => Type::Atom(AtomicType::Float64),
            Self::Decimal128(_) => Type::Atom(AtomicType::Decimal128(
                DECIMAL128_MAX_PRECISION,
                DECIMAL128_MAX_SCALE,
            )),
            Self::Decimal256(_) => Type::Atom(AtomicType::Decimal256(
                DECIMAL256_MAX_PRECISION,
                DECIMAL256_MAX_SCALE,
            )),
            Self::Utf8(_) => Type::Atom(AtomicType::Utf8),
            Self::LargeUtf8(_) => Type::Atom(AtomicType::LargeUtf8),
            Self::Binary(_) => Type::Atom(AtomicType::Binary),
            Self::FixedSizeBinary(size, _) => Type::Atom(AtomicType::FixedSizeBinary(*size)),
            Self::LargeBinary(_) => Type::Atom(AtomicType::Utf8),
            Self::TimestampSecond(_, tz) => {
                Type::Atom(AtomicType::Timestamp(TimeUnit::Second, tz.clone()))
            }
            Self::TimestampMillisecond(_, tz) => {
                Type::Atom(AtomicType::Timestamp(TimeUnit::Millisecond, tz.clone()))
            }
            Self::TimestampMicrosecond(_, tz) => {
                Type::Atom(AtomicType::Timestamp(TimeUnit::Microsecond, tz.clone()))
            }
            Self::TimestampNanosecond(_, tz) => {
                Type::Atom(AtomicType::Timestamp(TimeUnit::Nanosecond, tz.clone()))
            }
            Self::Date32(..) => Type::Atom(AtomicType::Date32),
            Self::Date64(..) => Type::Atom(AtomicType::Date64),

            Self::Time32Second(..) => Type::Atom(AtomicType::Time32(TimeUnit::Second)),
            Self::Time32Millisecond(..) => Type::Atom(AtomicType::Time32(TimeUnit::Millisecond)),
            Self::Time64Microsecond(..) => Type::Atom(AtomicType::Time64(TimeUnit::Nanosecond)),
            Self::Time64Nanosecond(..) => Type::Atom(AtomicType::Time64(TimeUnit::Nanosecond)),

            Self::IntervalYearMonth(..) => {
                Type::Atom(AtomicType::Interval(IntervalUnit::YearMonth))
            }
            Self::IntervalDayTime(..) => Type::Atom(AtomicType::Interval(IntervalUnit::DayTime)),
            Self::IntervalMonthDayNano(..) => {
                Type::Atom(AtomicType::Interval(IntervalUnit::MonthDayNano))
            }

            Self::Record(r) => Type::Record(r.schema()),
            Self::Relation(r) => Type::List(Box::new(Type::Record(r.schema()))),
            Self::List(l) => Type::List(Box::new(l.data_type())),
            Self::Fn(f) => Type::Fn(f.fn_type()),
        }
    }

    #[allow(unused)]
    fn as_any(&self) -> &dyn Any {
        match self {
            Self::Null => self as &dyn Any,
            Self::Boolean(x) => x as &dyn Any,
            Self::Int8(x) => x as &dyn Any,
            Self::Int16(x) => x as &dyn Any,
            Self::Int32(x) => x as &dyn Any,
            Self::Int64(x) => x as &dyn Any,
            Self::UInt8(x) => x as &dyn Any,
            Self::UInt16(x) => x as &dyn Any,
            Self::UInt32(x) => x as &dyn Any,
            Self::UInt64(x) => x as &dyn Any,
            Self::Float16(x) => x as &dyn Any,
            Self::Float32(x) => x as &dyn Any,
            Self::Float64(x) => x as &dyn Any,
            Self::Decimal128(x) => x as &dyn Any,
            Self::Decimal256(x) => x as &dyn Any,
            Self::Utf8(x) => x as &dyn Any,
            Self::LargeUtf8(x) => x as &dyn Any,
            Self::Binary(x) => x as &dyn Any,
            Self::FixedSizeBinary(..) => self as &dyn Any,
            Self::LargeBinary(x) => x as &dyn Any,
            Self::TimestampSecond(..) => self as &dyn Any,
            Self::TimestampMillisecond(..) => self as &dyn Any,
            Self::TimestampMicrosecond(..) => self as &dyn Any,
            Self::TimestampNanosecond(..) => self as &dyn Any,
            Self::Date32(x) => x as &dyn Any,
            Self::Date64(x) => x as &dyn Any,
            Self::Time32Second(x) => x as &dyn Any,
            Self::Time32Millisecond(x) => x as &dyn Any,
            Self::Time64Microsecond(x) => x as &dyn Any,
            Self::Time64Nanosecond(x) => x as &dyn Any,
            Self::IntervalYearMonth(x) => x as &dyn Any,
            Self::IntervalDayTime(x) => x as &dyn Any,
            Self::IntervalMonthDayNano(x) => x as &dyn Any,

            Self::Record(r) => r.as_any(),
            Self::Relation(r) => r.as_any(),
            Self::List(l) => l.as_any(),
            Self::Fn(f) => f.as_any(),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Boolean(x) => write!(f, "{}", x),
            Self::Int8(x) => write!(f, "{}", x),
            Self::Int16(x) => write!(f, "{}", x),
            Self::Int32(x) => write!(f, "{}", x),
            Self::Int64(x) => write!(f, "{}", x),
            Self::UInt8(x) => write!(f, "{}", x),
            Self::UInt16(x) => write!(f, "{}", x),
            Self::UInt32(x) => write!(f, "{}", x),
            Self::UInt64(x) => write!(f, "{}", x),

            // Float types print integer values for exact numbers with {},
            // e.g. 1.0 will print 1. This is confusing because you don't know
            // if it's a float or not. The debug version prints 1.0 though...
            Self::Float16(x) => write!(f, "{:?}", x),
            Self::Float32(x) => write!(f, "{:?}", x),
            Self::Float64(x) => write!(f, "{:?}", x),

            Self::Decimal128(x) => write!(f, "{}", x),
            Self::Decimal256(x) => write!(f, "{}", x),
            Self::Utf8(x) => write!(f, "{}", x),
            Self::LargeUtf8(x) => write!(f, "{}", x),

            // TODO binary strings are printed as their "debug" version (for now)
            Self::Binary(x) => write!(f, "{:?}", x),
            Self::FixedSizeBinary(_len, buf) => write!(f, "{:?}", buf),
            Self::LargeBinary(x) => write!(f, "{:?}", x),

            Self::TimestampSecond(x, _tz)
            | Self::TimestampMillisecond(x, _tz)
            | Self::TimestampMicrosecond(x, _tz)
            | Self::TimestampNanosecond(x, _tz) => {
                let (seconds, nanos) = match &self {
                    Self::TimestampSecond(..) => (*x, 0),
                    Self::TimestampMillisecond(..) => (*x / 1000, (*x % 1000) * 1_000_000),
                    Self::TimestampMicrosecond(..) => (*x / 1_000_000, (*x % 1_000_000) * 1_000),
                    Self::TimestampNanosecond(..) => (*x / 1_000_000_000, *x % 1_000_000_000),
                    _ => panic!("unreachable"),
                };
                write!(
                    f,
                    "{}",
                    Utc.timestamp_opt(seconds, nanos as u32)
                        .unwrap()
                        .to_rfc3339()
                )
            }

            Self::Date32(x) => write!(
                f,
                "{}",
                ArrowDate32Type::to_naive_date(*x)
                    .format("%Y-%m-%d")
                    .to_string()
            ),
            Self::Date64(x) => write!(
                f,
                "{}",
                ArrowDate64Type::to_naive_date(*x)
                    .format("%Y-%m-%d")
                    .to_string()
            ),

            Self::Time32Second(x) => write!(f, "Time(s) {:?}", x),
            Self::Time32Millisecond(x) => write!(f, "Time(ms) {:?}", x),
            Self::Time64Microsecond(x) => write!(f, "Time(μs) {:?}", x),
            Self::Time64Nanosecond(x) => write!(f, "Time(ns) {:?}", x),

            Self::IntervalYearMonth(x) => write!(f, "YearMonth {:?}", x),
            Self::IntervalDayTime(x) => write!(f, "DayTime {:?}", x),
            Self::IntervalMonthDayNano(x) => write!(f, "DayNano {:?}", x),

            // TODO: Implement records without Debug
            // We don't use the table builder for an individual record just yet, because
            // we might want to do something more horizontally space efficient.
            Self::Record(r) => {
                let schema = r.schema();
                let values = BTreeMap::from_iter(
                    (0..schema.len()).map(|i| ((schema[i].name.clone(), r.column(i)))),
                );
                write!(f, "{:?}", values)
            }

            Self::Relation(r) => {
                let schema = r.schema();
                let ncols = schema.len();
                let mut builder = TableBuilder::default();
                builder.set_columns(schema.iter().map(|f| Cow::Borrowed(f.name.as_str())));
                for idx in 0..r.num_batches() {
                    for record in r.batch(idx).records().into_iter() {
                        builder.add_record(
                            (0..ncols)
                                .map(|col_idx| Cow::Owned(format!("{}", record.column(col_idx)))),
                        );
                    }
                }

                let mut table = builder.build();
                table.with(tabled::style::Style::markdown());
                write!(f, "{}", table)
            }

            // TODO: Implement list without Debug
            Self::List(l) => {
                write!(f, "[")?;
                let mut delim = "";
                for t in l.as_vec() {
                    write!(f, "{delim}")?;
                    delim = ", ";
                    write!(f, "{t}")?;
                }
                write!(f, "]")
            }

            // TODO: Implement functions without Debug
            Self::Fn(v) => {
                write!(f, "{:?}", v.as_ref())
            }
        }
    }
}
