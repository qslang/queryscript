use arrow::array::Array as ArrowArray;
use std::sync::Arc;

use clickhouse_rs::types::{Column, Complex, SqlType};

use crate::types::{error::ts_unimplemented, AtomicType, TimeUnit, Type};

impl TryFrom<&SqlType> for Type {
    type Error = crate::types::error::TypesystemError;
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
            DateTime(_) => Type::Atom(AtomicType::Timestamp(TimeUnit::Second, None)),
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
    nullable: bool,
) -> crate::types::error::Result<Arc<dyn ArrowArray>> {
    use SqlType::*;

    let st = column.sql_type();
    match st {
        UInt8 => u8::to_arrow(column, nullable),
        UInt16 => u16::to_arrow(column, nullable),
        UInt32 => u32::to_arrow(column, nullable),
        UInt64 => u64::to_arrow(column, nullable),
        Int8 => i8::to_arrow(column, nullable),
        Int16 => i16::to_arrow(column, nullable),
        Int32 => i32::to_arrow(column, nullable),
        Int64 => i64::to_arrow(column, nullable),
        String | FixedString(_) => <&[u8]>::to_arrow(column, nullable),
        Float32 => f32::to_arrow(column, nullable),
        Float64 => f64::to_arrow(column, nullable),

        // XXX Finish these
        Date => todo!("Date"),
        DateTime(_) => todo!("DateTime"),
        Nullable(inner) => column_to_arrow(column, true),
        Array(inner) => todo!(),
        Decimal(p, s) => todo!(),
        Ipv4 | Ipv6 | Uuid | Enum8(..) | Enum16(..) => {
            return ts_unimplemented!("ClickHouse type: {:?}", st);
        }
    }
}

trait ClickHouseToArrow {
    type Target: ArrowArray;

    fn to_arrow(
        column: &Column<Complex>,
        nullable: bool,
    ) -> crate::types::error::Result<Arc<dyn ArrowArray>>;
}

macro_rules! primitive_array_conversion {
    ($ch_type:ty, $arrow_array:tt) => {
        impl ClickHouseToArrow for $ch_type {
            type Target = arrow::array::$arrow_array;

            fn to_arrow(
                column: &Column<Complex>,
                nullable: bool,
            ) -> crate::types::error::Result<Arc<dyn ArrowArray>> {
                if nullable {
                    Ok(Arc::new(Self::Target::from_iter(
                        column.iter::<Option<$ch_type>>()?.map(|i| i.map(|i| *i)),
                    )))
                } else {
                    Ok(Arc::new(Self::Target::from_iter(
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

impl ClickHouseToArrow for &[u8] {
    type Target = arrow::array::StringArray;

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
