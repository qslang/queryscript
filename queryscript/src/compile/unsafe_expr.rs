use std::cell::RefCell;
use std::sync::Arc;

use crate::compile::compile::ExternalTypeRank;
use crate::compile::error::*;
use crate::compile::external::schema_infer_expr_fn;
use crate::compile::generics::{ExternalType, GenericConstructor};
use crate::compile::inference::mkcref;
use crate::compile::schema::*;
use crate::compile::sql::{compile_reference, select_limit_0};
use crate::compile::traverse::{SQLVisitor, VisitSQL};
use crate::compile::Compiler;
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

        // Because we're not compiling, we can't rely on our own parameter logic to string together
        // references to the same expression. Furthermore, our parameters are strings (not paths), so
        // we can really only do this replacement for single-length paths.
        let name = if path.len() == 1 {
            path[0].clone()
        } else {
            return None;
        };

        match compile_reference(self.compiler.clone(), self.schema.clone(), &ast_path) {
            Ok(expr) => {
                if !Self::can_inline_expr(expr.expr.as_ref()) {
                    return None;
                }

                self.names
                    .borrow_mut()
                    .params
                    .insert(name.get().into(), expr);

                Some(vec![name])
            }
            Err(_) => None,
        }
    }
}

pub fn compile_unsafe_expr(
    compiler: Compiler,
    schema: Ref<Schema>,
    expr_body: &ast::ExprBody,
    loc: &SourceLocation,
) -> Result<CTypedExpr> {
    let name_collector = NameCollector::new(compiler.clone(), schema.clone());
    let (runnable_body, inference_body) = match &expr_body {
        ast::ExprBody::SQLQuery(sql) => {
            let transformed = sql.visit_sql(&name_collector);

            let limit_0 = select_limit_0(transformed.clone());
            (SQLBody::Query(transformed.clone()), SQLBody::Query(limit_0))
        }
        ast::ExprBody::SQLExpr(expr) => {
            let transformed = SQLBody::Expr(expr.visit_sql(&name_collector));
            (transformed.clone(), transformed)
        }
    };

    let names = name_collector.names.into_inner();
    let expr = mkcref(Expr::native_sql(Arc::new(SQL {
        names: names.clone(),
        body: runnable_body,
    })));
    let inference_expr = mkcref(Expr::native_sql(Arc::new(SQL {
        names: names,
        body: inference_body,
    })));

    let expr_type = CRef::new_unknown("unsafe expr");
    let resolve = schema_infer_expr_fn(
        schema.read()?.folder.clone(),
        inference_expr,
        expr_type.clone(),
    );

    compiler.add_external_type(resolve, expr_type.clone(), ExternalTypeRank::UnsafeExpr)?;

    Ok(CTypedExpr {
        type_: mkcref(MType::Generic(Located::new(
            ExternalType::new(&loc, vec![expr_type])?,
            loc.clone(),
        ))),
        expr,
    })
}
