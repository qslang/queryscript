use crate::types::Value;

use duckdb::{
    types::{TimeUnit as DuckTimeUnit, ToSqlOutput, Value as DuckValue},
    ToSql,
};

fn unimplemented(what: &str) -> duckdb::Result<ToSqlOutput<'_>> {
    return Err(duckdb::Error::ToSqlConversionFailure(Box::new(
        crate::error::UnimplementedSnafu {
            what: what.to_string(),
        }
        .build(),
    )));
}

impl ToSql for Value {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Self::Null => duckdb::types::Null.into(),
            Self::Boolean(x) => (*x).into(),
            Self::Int8(x) => (*x).into(),
            Self::Int16(x) => (*x).into(),
            Self::Int32(x) => (*x).into(),
            Self::Int64(x) => (*x).into(),
            Self::UInt8(x) => (*x).into(),
            Self::UInt16(x) => (*x).into(),
            Self::UInt32(x) => (*x).into(),
            Self::UInt64(x) => (*x).into(),

            Self::Float16(x) => f32::from(*x).into(),
            Self::Float32(x) => (*x).into(),
            Self::Float64(x) => (*x).into(),

            Self::Decimal128(x) => (*x).into(),
            Self::Decimal256(_x) => return unimplemented("256-bit integers"),
            Self::Utf8(x) => x.to_sql()?,
            Self::LargeUtf8(x) => x.to_sql()?,

            Self::Binary(x) => x.to_sql()?,
            Self::FixedSizeBinary(_len, buf) => buf.to_sql()?,
            Self::LargeBinary(x) => x.to_sql()?,

            Self::TimestampSecond(x, _tz) => {
                ToSqlOutput::Owned(DuckValue::Timestamp(DuckTimeUnit::Second, *x))
            }
            Self::TimestampMillisecond(x, _tz) => {
                ToSqlOutput::Owned(DuckValue::Timestamp(DuckTimeUnit::Millisecond, *x))
            }
            Self::TimestampMicrosecond(x, _tz) => {
                ToSqlOutput::Owned(DuckValue::Timestamp(DuckTimeUnit::Microsecond, *x))
            }
            Self::TimestampNanosecond(x, _tz) => {
                ToSqlOutput::Owned(DuckValue::Timestamp(DuckTimeUnit::Nanosecond, *x))
            }

            Self::Time32Second(x) => {
                ToSqlOutput::Owned(DuckValue::Time64(DuckTimeUnit::Second, i64::from(*x)))
            }
            Self::Time32Millisecond(x) => {
                ToSqlOutput::Owned(DuckValue::Time64(DuckTimeUnit::Millisecond, i64::from(*x)))
            }
            Self::Time64Microsecond(x) => {
                ToSqlOutput::Owned(DuckValue::Time64(DuckTimeUnit::Microsecond, i64::from(*x)))
            }
            Self::Time64Nanosecond(x) => {
                ToSqlOutput::Owned(DuckValue::Time64(DuckTimeUnit::Nanosecond, i64::from(*x)))
            }

            Self::Date32(x) => ToSqlOutput::Owned(DuckValue::Date32(*x)),
            Self::Date64(_) => return unimplemented("Datetime"),

            Self::IntervalYearMonth(_)
            | Self::IntervalDayTime(_)
            | Self::IntervalMonthDayNano(_) => return unimplemented("Date/Time Intervals"),

            Self::Record(_) => return unimplemented("Records"),
            Self::List(_) => return unimplemented("Lists"),
            Self::Fn(_) => return unimplemented("Function values"),

            Self::Relation(_) => {
                panic!("Relations should have been factored into relation scanner")
            }
        })
    }
}
