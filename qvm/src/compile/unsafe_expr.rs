use snafu::prelude::*;
use std::cell::RefCell;
use std::sync::Arc;

use crate::compile::compile::ExternalTypeOrder;
use crate::compile::error::*;
use crate::compile::generics::ExternalType;
use crate::compile::inference::mkcref;
use crate::compile::schema::*;
use crate::compile::sql::compile_reference;
use crate::compile::traverse::{SQLVisitor, VisitSQL};
use crate::compile::Compiler;
use crate::types::AtomicType;
use crate::{
    ast,
    ast::{sqlast, SourceLocation, ToPath},
};

struct NameCollector {
    compiler: Compiler,
    schema: SchemaRef,
    names: RefCell<SQLNames<CRef<MType>>>,
}

impl NameCollector {
    pub fn new(compiler: Compiler, schema: SchemaRef) -> NameCollector {
        NameCollector {
            compiler,
            schema,
            names: RefCell::new(SQLNames::new()),
        }
    }

    fn can_inline_expr(expr: &Expr<CRef<MType>>) -> bool {
        match expr {
            Expr::SQL(..) | Expr::FnCall(..) => true,
            Expr::SchemaEntry(expr) => {
                // This logic is a bit hacky. We are really trying to avoid inlining things like builtin
                // functions. Unknown expressions are probably values defined by the user, and known expressions
                // are hopefully within a whitelist of expression types (cases above).
                !expr.expr.is_known().unwrap()
                    || Self::can_inline_expr(&*expr.expr.must().unwrap().read().unwrap())
            }
            _ => false,
        }
    }
}

impl SQLVisitor for NameCollector {
    fn visit_sqlpath(
        &self,
        path: &Vec<sqlast::Located<sqlast::Ident>>,
    ) -> Option<Vec<sqlast::Located<sqlast::Ident>>> {
        let ast_path = path.to_path(self.schema.read().unwrap().file.clone());

        match compile_reference(self.compiler.clone(), self.schema.clone(), &ast_path) {
            Ok(expr) => {
                if !Self::can_inline_expr(expr.expr.as_ref()) {
                    return None;
                }

                // Because we're not compiling, we can't rely on our own parameter logic to string together
                // references to the same expression. Furthermore, our parameters are strings (not paths), so
                // we can really only do this replacement for single-length paths.
                let name = if ast_path.len() == 1 {
                    ast_path[0].value.clone()
                } else {
                    return None;
                };
                self.names.borrow_mut().params.insert(name.clone(), expr);

                Some(vec![sqlast::Located::new(
                    sqlast::Ident {
                        value: name,
                        quote_style: None,
                    },
                    path[0].location().clone(),
                )])
            }
            Err(_) => None,
        }
    }
}

fn schema_infer_expr_fn(
    schema: SchemaRef,
    expr: CRef<Expr<CRef<MType>>>,
    inner_type: CRef<MType>,
) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
    async move {
        let ctx = crate::runtime::Context::new(&schema, crate::runtime::SQLEngineType::DuckDB)
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

        // XXX This should be doable without actually running the expression (e.g. applying limit 0)
        let result = crate::runtime::eval(&ctx, &typed_expr)
            .await
            .context(RuntimeSnafu {
                loc: SourceLocation::Unknown,
            })?;

        let inferred_type = result.type_();
        let inferred_mtype = mkcref(MType::from_runtime_type(&inferred_type)?);

        inner_type.unify(&inferred_mtype)?;
        Ok(())
    }
}

pub fn compile_unsafe_expr(
    compiler: Compiler,
    schema: Ref<Schema>,
    expr_body: &ast::ExprBody,
    loc: &SourceLocation,
) -> Result<CTypedExpr> {
    let name_collector = NameCollector::new(compiler.clone(), schema.clone());
    let body = match &expr_body {
        ast::ExprBody::SQLQuery(sql) => SQLBody::Query(sql.visit_sql(&name_collector)),
        ast::ExprBody::SQLExpr(expr) => SQLBody::Expr(expr.visit_sql(&name_collector)),
    };

    let names = name_collector.names.into_inner();
    let expr = mkcref(Expr::SQL(Arc::new(SQL { names, body })));

    let expr_type = CRef::new_unknown("unsafe expr");
    let resolve = schema_infer_expr_fn(schema.clone(), expr.clone(), expr_type.clone());

    compiler.add_external_type(resolve, expr_type.clone(), ExternalTypeOrder::UnsafeExpr)?;

    Ok(CTypedExpr {
        type_: mkcref(MType::Generic(Located::new(
            ExternalType::new(&loc, vec![expr_type])?,
            loc.clone(),
        ))),
        expr,
    })
}
