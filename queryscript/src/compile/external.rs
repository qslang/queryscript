use crate::{
    ast::{Ident, Located, SourceLocation},
    types::{AtomicType, Type},
};
use snafu::prelude::*;

use super::{
    error::{Result, RuntimeSnafu},
    inference::mkcref,
    schema::{CRef, CTypedExpr, Expr, MType},
    CompileError,
};

// TODO If a record has two fields with the same name, we throw an error. Eventually,
// we should support this for top-level queries (e.g. unsafe expressions).
fn validate_inferred_type(type_: &Type) -> Result<()> {
    match type_ {
        Type::Atom(..) => {}
        Type::Fn(..) => {}
        Type::List(inner) => validate_inferred_type(inner)?,
        Type::Record(fields) => {
            let mut seen = std::collections::HashSet::new();
            for field in fields {
                if seen.contains(&field.name) {
                    return Err(CompileError::duplicate_entry(vec![
                        Ident::without_location(field.name.clone()),
                    ]));
                }
                seen.insert(field.name.clone());
                validate_inferred_type(&field.type_)?;
            }
        }
    };
    Ok(())
}

pub fn schema_infer_expr_fn(
    folder: Option<String>,
    expr: CRef<Expr<CRef<MType>>>,
    inner_type: CRef<MType>,
) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
    async move {
        let mut ctx = crate::runtime::Context::new(folder, crate::runtime::SQLEngineType::DuckDB)
            .disable_typechecks();

        let typed_expr = CTypedExpr {
            expr: expr.clone(),
            type_: mkcref(MType::Atom(Located::new(
                AtomicType::Null,
                SourceLocation::Unknown,
            ))),
        }
        .to_runtime_type()
        .context(RuntimeSnafu {
            loc: SourceLocation::Unknown,
        })?;

        let result = crate::runtime::eval(&mut ctx, &typed_expr)
            .await
            .context(RuntimeSnafu {
                loc: SourceLocation::Unknown,
            })?;

        let inferred_type = result.type_();
        validate_inferred_type(&inferred_type)?;
        let inferred_mtype = mkcref(MType::from_runtime_type(&inferred_type)?);

        inner_type.unify(&inferred_mtype)?;
        Ok(())
    }
}
