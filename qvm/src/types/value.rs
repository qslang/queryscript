use std::any::Any;

use super::types::*;

trait Value {
    fn type_(&self) -> Type;
    fn as_any(&self) -> &dyn Any;
}

trait Record: Value {
    fn schema(&self) -> Vec<Field>;
    fn get_value(&self, name: &str) -> &dyn Value;
}

trait List: Value {
    fn data_type(&self) -> Type;
}

trait Batch: Value {
    fn schema(&self) -> Vec<Field>;
    fn row(&self, index: usize) -> &dyn Record;
    fn column(&self, index: usize) -> &dyn List;
}

pub enum AtomicValue {
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
}

impl Value for AtomicValue {
    fn type_(&self) -> Type {
        Type::Atom(match self {
            Self::Null => AtomicType::Null,
            Self::Boolean(_) => AtomicType::Boolean,
            Self::Int8(_) => AtomicType::Int8,
            Self::Int16(_) => AtomicType::Int16,
            Self::Int32(_) => AtomicType::Int32,
            Self::Int64(_) => AtomicType::Int64,
            Self::UInt8(_) => AtomicType::UInt8,
            Self::UInt16(_) => AtomicType::UInt16,
            Self::UInt32(_) => AtomicType::UInt32,
            Self::UInt64(_) => AtomicType::UInt64,
            Self::Float16(_) => AtomicType::Float16,
            Self::Float32(_) => AtomicType::Float32,
            Self::Float64(_) => AtomicType::Float64,
        })
    }

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
        }
    }
}
