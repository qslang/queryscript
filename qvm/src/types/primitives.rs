pub use super::types::*;
pub use super::value::*;

macro_rules! primitive_conversion {
    ($native_ty:ty, $arm:tt) => {
        impl From<$native_ty> for Value {
            fn from(val: $native_ty) -> Self {
                Value::$arm(val)
            }
        }
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

/*
impl From<i16> for Value {
    fn from(val: i16) -> Self {
        Value::Int16(val)
    }
}
*/
