use arrow::array::Array as ArrowArray;
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use std::sync::Arc;

use clickhouse_rs::{
    types::{Column, Complex, DateTimeType, SqlType},
    Block,
};

use crate::types::{
    arrow::EPOCH_DAYS_FROM_CE,
    error::{ts_unimplemented, TypesystemError},
    precision_time_unit, AtomicType, TimeUnit, Type,
};

impl TryFrom<&SqlType> for Type {
    type Error = TypesystemError;
    fn try_from(st: &SqlType) -> crate::types::error::Result<Self> {
        use SqlType::*;
        Ok(match st {
            UInt8 => Type::Atom(AtomicType::UInt8),
            UInt16 => Type::Atom(AtomicType::UInt16),
            UInt32 => Type::Atom(AtomicType::UInt32),
            UInt64 => Type::Atom(AtomicType::UInt64),
            Int8 => Type::Atom(AtomicType::Int8),
            Int16 => Type::Atom(AtomicType::Int16),
            Int32 => Type::Atom(AtomicType::Int32),
            Int64 => Type::Atom(AtomicType::Int64),
            String | FixedString(_) => Type::Atom(AtomicType::Utf8),
            Float32 => Type::Atom(AtomicType::Float32),
            Float64 => Type::Atom(AtomicType::Float64),
            Date => Type::Atom(AtomicType::Date32),
            DateTime(dt) => {
                Type::Atom(match dt {
                    DateTimeType::DateTime32 => AtomicType::Timestamp(TimeUnit::Second, None),
                    // TODO: We ignore the timestamp here altogether :(
                    // https://github.com/qscl/queryscript/issues/88
                    DateTimeType::DateTime64(p, _) => {
                        AtomicType::Timestamp(precision_time_unit(*p as u64)?, None)
                    }
                    DateTimeType::Chrono => AtomicType::Timestamp(TimeUnit::Microsecond, None),
                })
            }
            Nullable(inner) => (*inner).try_into()?,
            Array(inner) => Type::List(Box::new((*inner).try_into()?)),
            Decimal(p, s) => Type::Atom(AtomicType::Decimal128(*p, *s as i8)),
            Ipv4 | Ipv6 | Uuid | Enum8(..) | Enum16(..) => {
                return ts_unimplemented!("ClickHouse type: {:?}", st);
            }
        })
    }
}

pub fn column_to_arrow(
    column: &Column<Complex>,
    st: SqlType,
    nullable: bool,
) -> crate::types::error::Result<Arc<dyn ArrowArray>> {
    use arrow::array::*;
    use SqlType::*;

    match st {
        UInt8 => UInt8Array::to_arrow(column, nullable),
        UInt16 => UInt16Array::to_arrow(column, nullable),
        UInt32 => UInt32Array::to_arrow(column, nullable),
        UInt64 => UInt64Array::to_arrow(column, nullable),
        Int8 => Int8Array::to_arrow(column, nullable),
        Int16 => Int16Array::to_arrow(column, nullable),
        Int32 => Int32Array::to_arrow(column, nullable),
        Int64 => Int64Array::to_arrow(column, nullable),
        String | FixedString(_) => StringArray::to_arrow(column, nullable),
        Float32 => Float32Array::to_arrow(column, nullable),
        Float64 => Float64Array::to_arrow(column, nullable),
        Date => Date32Array::to_arrow(column, nullable),
        DateTime(dt) => match dt {
            DateTimeType::DateTime32 => TimestampSecondArray::to_arrow(column, nullable),
            DateTimeType::DateTime64(p, _) => match precision_time_unit(p as u64)? {
                TimeUnit::Second => TimestampSecondArray::to_arrow(column, nullable),
                TimeUnit::Millisecond => TimestampMillisecondArray::to_arrow(column, nullable),
                TimeUnit::Microsecond => TimestampMicrosecondArray::to_arrow(column, nullable),
                TimeUnit::Nanosecond => TimestampNanosecondArray::to_arrow(column, nullable),
            },
            DateTimeType::Chrono => TimestampMicrosecondArray::to_arrow(column, nullable),
        },
        Nullable(inner) => column_to_arrow(column, inner.clone(), true),
        Array(_inner) => todo!("clickhouse arrays"),
        Decimal(_p, _s) => todo!("clickhouse decimals"),
        Ipv4 | Ipv6 | Uuid | Enum8(..) | Enum16(..) => {
            return ts_unimplemented!("ClickHouse type: {:?}", st);
        }
    }
}

pub fn arrow_to_column(
    block: Block,
    name: &str,
    array: &dyn ArrowArray,
) -> crate::types::error::Result<Block> {
    use arrow::array::*;
    use arrow::datatypes::DataType::*;

    let data = array.data();

    Ok(match data.data_type() {
        Null => block.add_column::<Vec<Option<i32>>>(name, vec![None; data.len()]),
        UInt8 => UInt8Array::to_block(name, array, block),
        UInt16 => UInt16Array::to_block(name, array, block),
        UInt32 => UInt32Array::to_block(name, array, block),
        UInt64 => UInt64Array::to_block(name, array, block),
        Int8 => Int8Array::to_block(name, array, block),
        Int16 => Int16Array::to_block(name, array, block),
        Int32 => Int32Array::to_block(name, array, block),
        Int64 => Int64Array::to_block(name, array, block),
        Utf8 => StringArray::to_block(name, array, block),
        Float32 => Float32Array::to_block(name, array, block),
        Float64 => Float64Array::to_block(name, array, block),
        Date32 => Date32Array::to_block(name, array, block),
        Time64(tu) => match tu {
            arrow::datatypes::TimeUnit::Second => {
                TimestampSecondArray::to_block(name, array, block)
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                TimestampMillisecondArray::to_block(name, array, block)
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                TimestampMicrosecondArray::to_block(name, array, block)
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                TimestampNanosecondArray::to_block(name, array, block)
            }
        },

        _ => return ts_unimplemented!("ClickHouse type: {:?}", data.data_type()),
    })
}

trait ClickHouseToArrow {
    fn to_arrow(
        column: &Column<Complex>,
        nullable: bool,
    ) -> crate::types::error::Result<Arc<dyn ArrowArray>>;

    fn to_block(
        name: &str,
        array: &dyn arrow::array::Array,
        block: clickhouse_rs::Block,
    ) -> clickhouse_rs::Block;
}

macro_rules! primitive_array_conversion {
    ($ch_type:ty, $arrow_array:tt) => {
        impl ClickHouseToArrow for arrow::array::$arrow_array {
            fn to_arrow(
                column: &Column<Complex>,
                nullable: bool,
            ) -> crate::types::error::Result<Arc<dyn ArrowArray>> {
                if nullable {
                    Ok(Arc::new(Self::from_iter(
                        column.iter::<Option<$ch_type>>()?.map(|i| i.map(|i| *i)),
                    )))
                } else {
                    Ok(Arc::new(Self::from_iter(
                        column.iter::<$ch_type>()?.map(|i| *i),
                    )))
                }
            }

            fn to_block(
                name: &str,
                array: &dyn arrow::array::Array,
                block: clickhouse_rs::Block,
            ) -> clickhouse_rs::Block {
                block.add_column::<Vec<Option<$ch_type>>>(
                    name,
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::$arrow_array>()
                        .unwrap()
                        .iter()
                        .collect(),
                )
            }
        }
    };
}

primitive_array_conversion!(u8, UInt8Array);
primitive_array_conversion!(u16, UInt16Array);
primitive_array_conversion!(u32, UInt32Array);
primitive_array_conversion!(u64, UInt64Array);
primitive_array_conversion!(i8, Int8Array);
primitive_array_conversion!(i16, Int16Array);
primitive_array_conversion!(i32, Int32Array);
primitive_array_conversion!(i64, Int64Array);
primitive_array_conversion!(f32, Float32Array);
primitive_array_conversion!(f64, Float64Array);

impl ClickHouseToArrow for arrow::array::StringArray {
    fn to_arrow(
        column: &Column<Complex>,
        nullable: bool,
    ) -> crate::types::error::Result<Arc<dyn ArrowArray>> {
        Ok(if nullable {
            let strings = column
                .iter::<Option<&[u8]>>()?
                .map(|i| i.map(|i| std::str::from_utf8(i)).transpose())
                .collect::<Result<Vec<_>, _>>()?;

            Arc::new(arrow::array::StringArray::from_iter(strings))
        } else {
            let strings = column
                .iter::<&[u8]>()?
                .map(|i| std::str::from_utf8(i))
                .collect::<Result<Vec<_>, _>>()?;

            Arc::new(arrow::array::StringArray::from_iter_values(strings))
        })
    }

    fn to_block(
        name: &str,
        array: &dyn arrow::array::Array,
        block: clickhouse_rs::Block,
    ) -> clickhouse_rs::Block {
        block.add_column::<Vec<&str>>(
            name,
            array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .iter()
                .map(|i| i.expect("non-null string in ClickHouse"))
                .collect(),
        )
    }
}

impl ClickHouseToArrow for arrow::array::Date32Array {
    fn to_arrow(
        column: &Column<Complex>,
        _nullable: bool, // Date32 has to be nullable (Arrow doesn't seem to support the other...)
    ) -> crate::types::error::Result<Arc<dyn ArrowArray>> {
        #[allow(deprecated)]
        Ok(Arc::new(Self::from_iter(
            column
                .iter::<chrono::DateTime<_>>()?
                .map(|i| Some(i.num_days_from_ce() - EPOCH_DAYS_FROM_CE)),
        )))
    }

    fn to_block(
        name: &str,
        array: &dyn arrow::array::Array,
        block: clickhouse_rs::Block,
    ) -> clickhouse_rs::Block {
        block.add_column::<Vec<Option<chrono::DateTime<_>>>>(
            name,
            array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap()
                .into_iter()
                .map(|i| {
                    i.map(|i| {
                        let tz = chrono_tz::UTC;
                        tz.from_local_datetime(&NaiveDateTime::new(
                            NaiveDate::from_num_days_from_ce_opt(i + EPOCH_DAYS_FROM_CE).unwrap(),
                            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                        ))
                        .unwrap()
                    })
                })
                .collect(),
        )
    }
}

macro_rules! timestamp_array_conversion {
    ($arrow_array:tt, $timestamp_method:tt, $denominator:literal) => {
        impl ClickHouseToArrow for arrow::array::$arrow_array {
            fn to_arrow(
                column: &Column<Complex>,
                _nullable: bool, // Dates have to be nullable in arrow
            ) -> crate::types::error::Result<Arc<dyn ArrowArray>> {
                Ok(Arc::new(Self::from_iter(
                    column
                        .iter::<chrono::DateTime<_>>()?
                        .map(|i| Some(i.$timestamp_method())),
                )))
            }

            fn to_block(
                name: &str,
                array: &dyn arrow::array::Array,
                block: clickhouse_rs::Block,
            ) -> clickhouse_rs::Block {
                block.add_column::<Vec<Option<chrono::DateTime<_>>>>(
                    name,
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::$arrow_array>()
                        .unwrap()
                        .into_iter()
                        .map(|i| {
                            i.map(|i| {
                                let tz = chrono_tz::UTC;
                                tz.from_local_datetime(
                                    &NaiveDateTime::from_timestamp_opt(
                                        i / $denominator,
                                        (i % $denominator) as u32,
                                    )
                                    .unwrap(),
                                )
                                .unwrap()
                            })
                        })
                        .collect(),
                )
            }
        }
    };
}

timestamp_array_conversion!(TimestampSecondArray, timestamp, 1000000000);
timestamp_array_conversion!(TimestampMillisecondArray, timestamp_millis, 1000000);
timestamp_array_conversion!(TimestampMicrosecondArray, timestamp_micros, 1000);
timestamp_array_conversion!(TimestampNanosecondArray, timestamp_nanos, 1);
