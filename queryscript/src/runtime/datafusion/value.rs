// NOTE: This is currently dead code, since we've removed support for datafusion

use crate::runtime::error::{fail, Result, RuntimeError};
use crate::types::{types::try_fields_to_arrow_fields, ArrowField, Value};
use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE};

use datafusion::common::ScalarValue as DFScalarValue;

impl TryInto<DFScalarValue> for Value {
    type Error = RuntimeError;

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
            | Self::Relation(..) => return fail!("Unsupported value {:?} in data fusion", self),
        })
    }
}
