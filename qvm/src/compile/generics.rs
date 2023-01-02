use snafu::prelude::*;

use super::error::*;
use super::inference::Constrainable;
use super::schema::*;
use crate::types::{AtomicType, Type};

pub struct SumGeneric();

impl SumGeneric {
    fn arg<T>(args: &Vec<T>) -> &T {
        assert!(args.len() == 1);
        &args[0]
    }

    pub fn new() -> SumGeneric {
        SumGeneric()
    }
}

impl std::fmt::Debug for SumGeneric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SumAgg")
    }
}

impl Generic for SumGeneric {
    fn to_runtime_type(
        &self,
        args: &Vec<crate::types::Type>,
    ) -> crate::runtime::error::Result<crate::types::Type> {
        let arg = Self::arg(args);
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
        let arg = Self::arg(args);
        if arg.is_known()? {
            let arg = arg.must().context(RuntimeSnafu {
                loc: ErrorLocation::Unknown,
            })?;
            let arg = arg.read()?.to_runtime_type().context(RuntimeSnafu {
                loc: ErrorLocation::Unknown,
            })?;
            let final_type = MType::from_runtime_type(&self.to_runtime_type(&vec![arg]).context(
                RuntimeSnafu {
                    loc: ErrorLocation::Unknown,
                },
            )?)?;
            other.unify(&final_type)?;
        }
        Ok(())
    }
}
