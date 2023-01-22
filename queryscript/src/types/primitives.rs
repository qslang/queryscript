pub use super::types::*;
pub use super::value::*;

macro_rules! primitive_conversion {
    ($native_ty:ty, $arm:tt, $return_ty:ty) => {
        impl From<$native_ty> for Value {
            fn from(val: $native_ty) -> Self {
                Value::$arm(val.into())
            }
        }

        impl TryInto<$return_ty> for Value {
            type Error = super::error::TypesystemError;

            fn try_into(self) -> super::error::Result<$return_ty> {
                match self {
                    Value::$arm(v) => Ok(v.into()),
                    _ => super::error::ts_fail!(
                        "Expected {} but got {:?}",
                        stringify!($return_ty),
                        self
                    ),
                }
            }
        }
    };
    ($native_ty:ty, $arm:tt) => {
        primitive_conversion!($native_ty, $arm, $native_ty);
    };
}

primitive_conversion!(bool, Boolean);
primitive_conversion!(i8, Int8);
primitive_conversion!(i16, Int16);
primitive_conversion!(i32, Int32);
primitive_conversion!(i64, Int64);
primitive_conversion!(u8, UInt8);
primitive_conversion!(u16, UInt16);
primitive_conversion!(u32, UInt32);
primitive_conversion!(u64, UInt64);
primitive_conversion!(half::f16, Float16);
primitive_conversion!(f32, Float32);
primitive_conversion!(f64, Float64);
primitive_conversion!(String, Utf8);
primitive_conversion!(&[u8], Binary, Vec<u8>);
primitive_conversion!(i128, Decimal128);
primitive_conversion!(i256, Decimal256);

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(val: Option<T>) -> Self {
        match val {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}
