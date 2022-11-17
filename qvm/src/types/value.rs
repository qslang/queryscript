pub use arrow_buffer::i256;
pub use datafusion::arrow::{
    array::Array as ArrowArray,
    array::ArrayRef as ArrowArrayRef,
    datatypes::{
        DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION,
        DECIMAL256_MAX_SCALE,
    },
    record_batch::RecordBatch as ArrowRecordBatch,
};
use datafusion::common::ScalarValue as DFScalarValue;
use dyn_clone::{clone_trait_object, DynClone};
pub use std::any::Any;
use std::fmt::Debug;
pub use std::sync::Arc;

use super::error::{ts_fail, Result};
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

    // These are the structural types in QVM that deviate from DataFusion
    Record(Arc<dyn Record>),
    Relation(Arc<dyn Relation>),
    List(Arc<dyn List>),
    Fn(Arc<dyn FnValue>),
}

pub trait Record: Debug + Send + Sync {
    fn schema(&self) -> Vec<Field>;
    fn as_any(&self) -> &dyn Any;
    fn column(&self, index: usize) -> &Value;
}

pub trait List: Debug + Send + Sync {
    fn data_type(&self) -> Type;
    fn as_any(&self) -> &dyn Any;

    // TODO: This should eventually be changed to have the standard
    // array-like methods (indexing, etc.)
    fn as_vec(&self) -> Vec<Value>;
}

pub trait FnValue: Debug + DynClone + Send + Sync {
    fn execute(&self, args: Vec<Value>) -> crate::runtime::error::Result<Value>;
    fn fn_type(&self) -> FnType;
    fn as_any(&self) -> &dyn Any;
}

pub trait Relation: Debug + Send + Sync {
    fn schema(&self) -> Vec<Field>;
    fn as_any(&self) -> &dyn Any;

    fn num_batches(&self) -> usize;
    fn batch(&self, index: usize) -> &dyn RecordBatch;

    // We expect to have one such function for each in-memory execution
    // format we support. To start, this is just Apache Arrow (columnar).
    fn as_arrow_recordbatch(self: Arc<Self>) -> Arc<Vec<ArrowRecordBatch>>;
}

pub trait RecordBatch: Debug + Send + Sync {
    fn schema(&self) -> Vec<Field>;
    fn as_any(&self) -> &dyn Any;

    fn records(&self) -> Vec<Arc<dyn Record>>;
}

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
            Self::Decimal128(_) => Type::Atom(AtomicType::Decimal128(
                DECIMAL128_MAX_PRECISION,
                DECIMAL128_MAX_SCALE,
            )),
            Self::Decimal256(_) => Type::Atom(AtomicType::Decimal128(
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

impl TryInto<DFScalarValue> for Value {
    type Error = super::error::TypesystemError;

    fn try_into(self) -> Result<DFScalarValue> {
        Ok(match self {
            Self::Null => DFScalarValue::Null,
            Self::Boolean(x) => DFScalarValue::Boolean(Some(x)),
            Self::Int8(x) => DFScalarValue::Int8(Some(x)),
            Self::Int16(x) => DFScalarValue::Int16(Some(x)),
            Self::Int32(x) => DFScalarValue::Int32(Some(x)),
            Self::Int64(x) => DFScalarValue::Int64(Some(x)),
            Self::UInt8(x) => DFScalarValue::UInt8(Some(x)),
            Self::UInt16(x) => DFScalarValue::UInt16(Some(x)),
            Self::UInt32(x) => DFScalarValue::UInt32(Some(x)),
            Self::UInt64(x) => DFScalarValue::UInt64(Some(x)),
            Self::Float16(x) => DFScalarValue::Float32(Some(x.into())),
            Self::Float32(x) => DFScalarValue::Float32(Some(x)),
            Self::Float64(x) => DFScalarValue::Float64(Some(x)),
            Self::Decimal128(x) => {
                DFScalarValue::Decimal128(Some(x), DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE)
            }
            Self::Utf8(x) => DFScalarValue::Utf8(Some(x)),
            Self::LargeUtf8(x) => DFScalarValue::LargeUtf8(Some(x)),
            Self::Binary(x) => DFScalarValue::Binary(Some(x)),
            Self::FixedSizeBinary(len, buf) => DFScalarValue::FixedSizeBinary(len, Some(buf)),
            Self::LargeBinary(x) => DFScalarValue::LargeBinary(Some(x)),

            Self::TimestampSecond(x, tz) => DFScalarValue::TimestampSecond(Some(x), tz),
            Self::TimestampMillisecond(x, tz) => DFScalarValue::TimestampMillisecond(Some(x), tz),
            Self::TimestampMicrosecond(x, tz) => DFScalarValue::TimestampMicrosecond(Some(x), tz),
            Self::TimestampNanosecond(x, tz) => DFScalarValue::TimestampNanosecond(Some(x), tz),

            Self::Date32(x) => DFScalarValue::Date32(Some(x)),
            Self::Date64(x) => DFScalarValue::Date64(Some(x)),

            Self::IntervalYearMonth(x) => DFScalarValue::IntervalYearMonth(Some(x)),
            Self::IntervalDayTime(x) => DFScalarValue::IntervalDayTime(Some(x)),
            Self::IntervalMonthDayNano(x) => DFScalarValue::IntervalMonthDayNano(Some(x)),

            Self::Record(r) => {
                let schema = r.schema();
                let values = (0..schema.len())
                    .map(|i| r.column(i).clone().try_into())
                    .collect::<Result<Vec<_>>>()?;
                DFScalarValue::Struct(Some(values), Box::new(try_fields_to_arrow_fields(&schema)?))
            }
            Self::List(l) => {
                let data_type = l.data_type();
                DFScalarValue::List(
                    Some(
                        l.as_vec()
                            .iter()
                            .map(|v| (v.clone()).try_into())
                            .collect::<Result<Vec<DFScalarValue>>>()?,
                    ),
                    Box::new(ArrowField::new("", (&data_type).try_into()?, true)),
                )
            }

            Self::Decimal256(..)
            | Self::Fn(..)
            | Self::Time32Second(..)
            | Self::Time32Millisecond(..)
            | Self::Time64Microsecond(..)
            | Self::Time64Nanosecond(..)
            | Self::Relation(..) => return ts_fail!("Unsupported value {:?} in data fusion", self),
        })
    }
}
