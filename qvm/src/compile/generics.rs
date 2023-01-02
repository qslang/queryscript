use snafu::prelude::*;
use std::any::Any;
use std::sync::Arc;

use super::{
    error::*, inference::mkcref, inference::Constrainable, schema::*, sql::get_rowtype, Compiler,
};
use crate::types::{AtomicType, Type};

fn arg<T>(args: &Vec<T>) -> &T {
    assert!(args.len() == 1);
    &args[0]
}

pub fn is_generic<T: Generic + 'static>(g: &dyn Generic) -> bool {
    g.as_any().downcast_ref::<T>().is_some()
}

pub struct SumGeneric();

impl SumGeneric {
    pub fn new() -> Arc<dyn Generic> {
        Arc::new(SumGeneric())
    }
}

impl std::fmt::Debug for SumGeneric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SumAgg")
    }
}

impl Generic for SumGeneric {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_runtime_type(
        &self,
        args: Vec<crate::types::Type>,
    ) -> crate::runtime::error::Result<crate::types::Type> {
        let arg = arg(&args);

        // DuckDB's sum function follows the following rules:
        // 	sum(DECIMAL) -> DECIMAL
        //	sum(SMALLINT) -> HUGEINT
        //	sum(INTEGER) -> HUGEINT
        //	sum(BIGINT) -> HUGEINT
        //	sum(HUGEINT) -> HUGEINT
        //	sum(DOUBLE) -> DOUBLE
        match arg {
            Type::Atom(at) => Ok(Type::Atom(match &at {
                AtomicType::Int8
                | AtomicType::Int16
                | AtomicType::Int32
                | AtomicType::Int64
                | AtomicType::UInt8
                | AtomicType::UInt16
                | AtomicType::UInt32
                | AtomicType::UInt64 => AtomicType::Decimal128(38, 0),
                AtomicType::Float32 | AtomicType::Float64 => AtomicType::Float64,
                AtomicType::Decimal128(..) | AtomicType::Decimal256(..) => at.clone(),
                _ => {
                    return Err(crate::runtime::error::RuntimeError::new(
                        format!(
                            "sum(): expected argument to be a numeric ype, got {:?}",
                            arg
                        )
                        .as_str(),
                    ))
                }
            })),
            _ => {
                return Err(crate::runtime::error::RuntimeError::new(
                    format!(
                        "sum(): expected argument to be an atomic type, got {:?}",
                        arg
                    )
                    .as_str(),
                ))
            }
        }
    }

    fn unify(&self, args: &Vec<CRef<MType>>, other: &MType) -> Result<()> {
        // This is a bit of an approximate implementation, since it only works if the inner
        // type is known.
        let arg = arg(args);
        if arg.is_known()? {
            let arg = arg.must().context(RuntimeSnafu {
                loc: ErrorLocation::Unknown,
            })?;
            let arg = arg.read()?.to_runtime_type().context(RuntimeSnafu {
                loc: ErrorLocation::Unknown,
            })?;
            let final_type = MType::from_runtime_type(&self.to_runtime_type(vec![arg]).context(
                RuntimeSnafu {
                    loc: ErrorLocation::Unknown,
                },
            )?)?;
            other.unify(&final_type)?;
        }
        Ok(())
    }
}

pub struct ExternalType();

impl ExternalType {
    pub fn new() -> Arc<dyn Generic> {
        Arc::new(ExternalType())
    }
}

impl std::fmt::Debug for ExternalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "External")
    }
}

impl Generic for ExternalType {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_runtime_type(
        &self,
        mut args: Vec<crate::types::Type>,
    ) -> crate::runtime::error::Result<crate::types::Type> {
        Ok(args.swap_remove(0))
    }

    fn unify(&self, args: &Vec<CRef<MType>>, other: &MType) -> Result<()> {
        let inner_type = arg(args);

        match other {
            MType::Generic(inner) => {
                let (generic, other_args) = inner.get();
                if is_generic::<ExternalType>(generic.as_ref()) {
                    let other_arg = arg(other_args);
                    inner_type.unify(other_arg)?;
                } else {
                    inner_type.unify(&mkcref(other.clone()))?;
                }
            }
            other => {
                inner_type.unify(&mkcref(other.clone()))?;
            }
        };
        Ok(())
    }

    fn get_rowtype(
        &self,
        compiler: Compiler,
        args: &Vec<CRef<MType>>,
    ) -> Result<Option<CRef<MType>>> {
        let inner_type = arg(args);
        Ok(Some(get_rowtype(compiler, inner_type.clone())?))
    }
}
