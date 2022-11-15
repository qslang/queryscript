pub use datafusion::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use dyn_clone::{clone_trait_object, DynClone};
pub use std::any::Any;
use std::fmt;
pub use std::sync::Arc;

use super::types::*;

#[derive(Debug, Clone)]
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
    Float16(f32),
    Float32(f32),
    Float64(f64),

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

    // DataFusion does not seem to have a Time32 implementation
    Time64(i64),

    IntervalYearMonth(i32),
    IntervalDayTime(i64),
    IntervalMonthDayNano(i128),

    // These are the structural types in QVM that deviate from DataFusion
    Record(Arc<dyn Record>),
    Relation(Arc<dyn Relation>),
    List(Arc<dyn List>),
    Fn(Arc<dyn FnValue>),
}

pub trait Record: fmt::Debug + DynClone + Send + Sync {
    fn schema(&self) -> Vec<Field>;
    fn as_any(&self) -> &dyn Any;
}

pub trait List: fmt::Debug + DynClone + Send + Sync {
    fn data_type(&self) -> Type;
    fn as_any(&self) -> &dyn Any;
}

pub trait FnValue: fmt::Debug + DynClone + Send + Sync {
    fn execute(&self, args: Vec<Value>) -> crate::runtime::error::Result<Value>;
    fn fn_type(&self) -> FnType;
    fn as_any(&self) -> &dyn Any;
}

pub trait Relation: fmt::Debug + DynClone + Send + Sync {
    fn schema(&self) -> Vec<Field>;
    fn as_any(&self) -> &dyn Any;

    fn row(&self, index: usize) -> &dyn Record;
    fn column(&self, index: usize) -> &dyn List;

    fn as_arrow_recordbatch(self: Arc<Self>) -> Arc<Vec<ArrowRecordBatch>>;
}

clone_trait_object!(Record);
clone_trait_object!(Relation);
clone_trait_object!(List);
clone_trait_object!(FnValue);

impl Value {
    #[allow(unused)]
    fn type_(&self) -> Type {
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

            // DataFusion does not seem to have a Time32 implementation
            // The Time64 type is nanonseconds according to a comment in ScalarValue
            Self::Time64(..) => Type::Atom(AtomicType::Time64(TimeUnit::Nanosecond)),

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
            Self::Time64(x) => x as &dyn Any,
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
