use dyn_clone::{clone_trait_object, DynClone};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use super::error::Result;
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
    Record(Arc<dyn Record>),
    RecordBatch(Arc<dyn RecordBatch>),
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
    fn execute(&self, args: Vec<Value>) -> Result<Value>;
    fn fn_type(&self) -> FnType;
    fn as_any(&self) -> &dyn Any;
}

trait RecordBatch: fmt::Debug + DynClone + Send + Sync {
    fn schema(&self) -> Vec<Field>;
    fn as_any(&self) -> &dyn Any;
    fn row(&self, index: usize) -> &dyn Record;
    fn column(&self, index: usize) -> &dyn List;
}

clone_trait_object!(Record);
clone_trait_object!(RecordBatch);
clone_trait_object!(List);
clone_trait_object!(FnValue);

impl Value {
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
            Self::Record(r) => Type::Record(r.schema()),
            Self::RecordBatch(r) => Type::List(Box::new(Type::Record(r.schema()))),
            Self::List(l) => Type::List(Box::new(l.data_type())),
            Self::Fn(f) => Type::Fn(f.fn_type()),
        }
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
            Self::Record(r) => r.as_any(),
            Self::RecordBatch(r) => r.as_any(),
            Self::List(l) => l.as_any(),
            Self::Fn(f) => f.as_any(),
        }
    }
}
