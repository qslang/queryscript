use arrow::array::Array as ArrowArray;
use chrono::Datelike;
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
        Array(inner) => todo!("clickhouse arrays"),
        Decimal(p, s) => todo!("clickhouse decimals"),
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
        Int32 => block.add_column::<Vec<Option<i32>>>(
            name,
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect(),
        ),
        _ => return ts_unimplemented!("ClickHouse type: {:?}", data.data_type()),
    })
}

trait ClickHouseToArrow {
    fn to_arrow(
        column: &Column<Complex>,
        nullable: bool,
    ) -> crate::types::error::Result<Arc<dyn ArrowArray>>;
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
}

macro_rules! timestamp_array_conversion {
    ($arrow_array:tt, $timestamp_method:tt) => {
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
        }
    };
}

timestamp_array_conversion!(TimestampSecondArray, timestamp);
timestamp_array_conversion!(TimestampMillisecondArray, timestamp_millis);
timestamp_array_conversion!(TimestampMicrosecondArray, timestamp_micros);
timestamp_array_conversion!(TimestampNanosecondArray, timestamp_nanos);
