use lazy_static::lazy_static;
use snafu::prelude::*;
use sqlparser::ast::WildcardAdditionalOptions;
use sqlparser::{ast as sqlast, ast::DataType as ParserDataType};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use crate::compile::coerce::CoerceOp;
use crate::compile::compile::{
    coerce, lookup_path, resolve_global_atom, typecheck_path, Compiler, SymbolKind,
};
use crate::compile::error::*;
use crate::compile::generics::{as_generic, ExternalType};
use crate::compile::inference::*;
use crate::compile::inline::*;
use crate::compile::schema::*;
use crate::compile::scope::{AvailableReferences, SQLScope};
use crate::types::{number::parse_numeric_type, AtomicType, Type};
use crate::{
    ast,
    ast::{SourceLocation, ToPath, ToSqlIdent},
};

use super::compile::ExternalTypeRank;

const QS_NAMESPACE: &str = "__qs";

#[derive(Clone, Debug)]
pub struct TypedSQL {
    pub type_: CRef<MType>,
    pub sql: Ref<SQL<CRef<MType>>>,
}

// Here, "C" means constrained.  In general, any structs prefixed with C indicate that there are
// structures that may be unknown within them.
//
#[derive(Clone, Debug)]
pub struct CTypedNameAndSQL {
    pub name: Located<Ident>,
    pub type_: CRef<MType>,
    pub sql: CRef<SQL<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct NameAndSQL {
    pub name: Located<Ident>,
    pub sql: Arc<SQL<CRef<MType>>>,
}

// Here, "C" means constrained.  In general, any structs prefixed with C indicate that there are
// structures that may be unknown within them.
//
#[derive(Clone, Debug)]
pub struct CTypedSQL {
    pub type_: CRef<MType>,
    pub sql: CRef<SQL<CRef<MType>>>,
}

impl<Ty: Clone + fmt::Debug + Send + Sync, SQLAst: Clone + fmt::Debug + Send + Sync> Constrainable
    for SQLSnippet<Ty, SQLAst>
{
}
impl Constrainable for TypedSQL {}
impl Constrainable for NameAndSQL {}
impl Constrainable for CTypedNameAndSQL {}
impl Constrainable for CTypedSQL {}
impl Constrainable for CTypedExpr {}

pub fn get_rowtype(compiler: Compiler, relation: CRef<MType>) -> Result<CRef<MType>> {
    Ok(compiler.clone().async_cref(async move {
        let r = &relation;
        let reltype = r.await?;
        let locked = reltype.read()?;
        match &*locked {
            MType::List(inner) => Ok(inner.get().clone()),
            MType::Generic(generic) => Ok(match generic.get_rowtype(compiler)? {
                Some(rowtype) => rowtype,
                None => relation.clone(),
            }),
            _ => Ok(relation.clone()),
        }
    })?)
}

pub fn param_ident(value: String) -> sqlast::Located<sqlast::Ident> {
    sqlast::Ident::new(value)
}

pub fn select_from(
    projection: Vec<sqlast::SelectItem>,
    from: Vec<sqlast::TableWithJoins>,
) -> sqlast::Query {
    sqlast::Query {
        with: None,
        body: Box::new(sqlast::SetExpr::Select(Box::new(sqlast::Select {
            distinct: false,
            top: None,
            projection,
            into: None,
            from,
            lateral_views: Vec::new(),
            selection: None,
            group_by: Vec::new(),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            qualify: None,
        }))),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        fetch: None,
        locks: Vec::new(),
    }
}

pub fn select_no_from(
    expr: sqlast::Expr,
    alias: Option<sqlast::Located<sqlast::Ident>>,
) -> sqlast::Query {
    select_from(
        vec![match alias {
            Some(alias) => sqlast::SelectItem::ExprWithAlias { expr, alias },
            None => sqlast::SelectItem::UnnamedExpr(expr),
        }],
        Vec::new(),
    )
}

pub fn select_star_from(relation: sqlast::TableFactor) -> sqlast::Query {
    select_from(
        vec![sqlast::SelectItem::Wildcard(WildcardAdditionalOptions {
            opt_exclude: None,
            opt_except: None,
            opt_rename: None,
        })],
        vec![sqlast::TableWithJoins {
            relation,
            joins: Vec::new(),
        }],
    )
}

pub fn with_table_alias(
    table: &sqlast::TableFactor,
    alias: Option<sqlast::TableAlias>,
) -> sqlast::TableFactor {
    let mut table = table.clone();
    let alias_ref: &mut Option<sqlast::TableAlias> = match &mut table {
        sqlast::TableFactor::Table { alias, .. } => alias,
        sqlast::TableFactor::Derived { alias, .. } => alias,
        sqlast::TableFactor::TableFunction { alias, .. } => alias,
        sqlast::TableFactor::UNNEST { alias, .. } => alias,
        sqlast::TableFactor::NestedJoin { alias, .. } => alias,
    };
    *alias_ref = alias;
    table
}

pub fn compile_sqlreference(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    sqlpath: &Vec<sqlast::Located<sqlast::Ident>>,
) -> Result<CTypedExpr> {
    let file = schema.read()?.file.clone();
    let sqlpath = sqlpath.clone();
    let path = sqlpath.to_path(file.clone());
    let loc = path_location(&path);
    match sqlpath.len() {
        0 => {
            return Err(CompileError::internal(
                loc.clone(),
                "Reference must have at least one part",
            ));
        }
        1 => {
            let name = sqlpath[0].clone();
            let name_ident: Ident = path[0].get().clone();

            if let Some((relation_type, relation_loc)) = scope.read()?.get_relation(&name_ident)? {
                let type_ = get_rowtype(compiler.clone(), relation_type)?;
                let expr = mkcref(Expr::SQL(Arc::new(SQL {
                    names: CSQLNames::from_unbound(&sqlpath),
                    body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(sqlpath.clone())),
                })));
                compiler.run_on_symbol::<ExprEntry>(
                    path[0].clone(),
                    SymbolKind::Field,
                    mkcref(type_.clone().into()),
                    relation_loc,
                    None,
                )?;
                return Ok(CTypedExpr { type_, expr });
            } else {
                let available =
                    scope
                        .read()?
                        .get_available_references(compiler.clone(), &loc, None)?;

                let tse = available.then({
                    move |available: Ref<AvailableReferences>| {
                        if let Some(fm) = available.read()?.get(&name_ident) {
                            if let Some(type_) = fm.type_.clone() {
                                let sqlpath = vec![fm.relation.to_sqlident(), name.clone()];
                                compiler.run_on_symbol::<ExprEntry>(
                                    path[0].clone(),
                                    SymbolKind::Field,
                                    mkcref(type_.clone().into()),
                                    fm.relation.location().clone(),
                                    None,
                                )?;
                                Ok(mkcref(TypedExpr {
                                    type_: type_.clone(),
                                    expr: Arc::new(Expr::SQL(Arc::new(SQL {
                                        names: CSQLNames::from_unbound(&sqlpath),
                                        body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(
                                            sqlpath,
                                        )),
                                    }))),
                                }))
                            } else {
                                Err(CompileError::duplicate_entry(vec![Ident::from_sqlident(
                                    loc.clone(),
                                    name.get().clone(),
                                )]))
                            }
                        } else {
                            // If it doesn't match any names of fields in SQL relations,
                            // compile it as a normal reference.
                            //
                            let te = compile_reference(compiler.clone(), schema.clone(), &path)?;
                            Ok(mkcref(te))
                        }
                    }
                })?;
                let type_ =
                    tse.then(|tse: Ref<TypedExpr<CRef<MType>>>| Ok(tse.read()?.type_.clone()))?;
                let expr = tse.then(|tse: Ref<TypedExpr<CRef<MType>>>| {
                    Ok(mkcref(tse.read()?.expr.as_ref().clone()))
                })?;

                return Ok(CTypedExpr { type_, expr });
            }
        }
        2 => {
            let relation_name = sqlpath[0].get().into();

            // If the relation can't be found in the scope, just fall through
            //
            if let Some((relation_type, relation_loc)) =
                scope.read()?.get_relation(&relation_name)?
            {
                let rowtype = get_rowtype(compiler.clone(), relation_type)?;
                let type_ = typecheck_path(rowtype, vec![path[1].clone()].as_slice())?;
                let expr = mkcref(Expr::SQL(Arc::new(SQL {
                    names: CSQLNames::from_unbound(&sqlpath),
                    body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(sqlpath.clone())),
                })));
                compiler.run_on_symbol::<ExprEntry>(
                    path[1].clone(),
                    SymbolKind::Value,
                    mkcref(type_.clone().into()),
                    relation_loc,
                    None,
                )?;
                return Ok(CTypedExpr { type_, expr });
            }
        }
        // References longer than two parts must be pointing outside the query, so just fall
        // through
        //
        _ => {}
    }

    let te = compile_reference(compiler.clone(), schema.clone(), &path)?;
    Ok(CTypedExpr {
        type_: te.type_.clone(),
        expr: mkcref(te.expr.as_ref().clone()),
    })
}

pub fn compile_reference(
    compiler: Compiler,
    schema: Ref<Schema>,
    path: &ast::Path,
) -> Result<TypedExpr<CRef<MType>>> {
    let (_, decl, remainder) = lookup_path::<ExprEntry>(
        compiler.clone(),
        schema.clone(),
        &path,
        true, /* import_global */
        true, /* resolve_last */
    )?;

    let decl = decl.ok_or_else(|| CompileError::no_such_entry(path.clone()))?;
    let remainder_cpy = remainder.clone();

    let expr = &decl.value;
    let type_ = expr
        .type_
        .then(|t: Ref<SType>| Ok(t.read()?.instantiate()?))?;
    typecheck_path(type_.clone(), remainder_cpy.as_slice())?;

    let top_level_ref = TypedExpr {
        type_: type_.clone(),
        expr: Arc::new(Expr::SchemaEntry(expr.clone())),
    };

    let (stype, r) = match remainder.len() {
        0 => (expr.type_.clone(), top_level_ref),
        _ => {
            // Turn the top level reference into a SQL placeholder, and return
            // a path accessing it
            let (placeholder_name, placeholder) =
                intern_nonsql_placeholder(compiler.clone(), "param", &top_level_ref)?;
            let mut full_name = vec![placeholder_name.clone()];
            full_name.extend(remainder.clone().into_iter().map(|n| n.to_sqlident()));

            let expr = Arc::new(Expr::SQL(Arc::new(SQL {
                names: placeholder.names.clone(),
                body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(full_name)),
            })));

            (mkcref(type_.clone().into()), TypedExpr { type_, expr })
        }
    };

    if let Some(ident) = path.last() {
        let kind = if decl.fn_arg {
            SymbolKind::Argument
        } else {
            SymbolKind::Value
        };
        compiler.run_on_symbol(
            ident.clone(),
            kind,
            stype.clone(),
            decl.name.location().clone(),
            Some(decl.clone()),
        )?;
    }

    Ok(r)
}

pub fn intern_placeholder(
    compiler: Compiler,
    kind: &str,
    expr: &TypedExpr<CRef<MType>>,
) -> Result<Arc<SQL<CRef<MType>>>> {
    match &*expr.expr {
        Expr::SQL(sql) => Ok(sql.clone()),
        _ => {
            let (_, e) = intern_nonsql_placeholder(compiler.clone(), kind, expr)?;
            Ok(e)
        }
    }
}

pub fn intern_nonsql_placeholder(
    compiler: Compiler,
    kind: &str,
    expr: &TypedExpr<CRef<MType>>,
) -> Result<(sqlast::Located<sqlast::Ident>, Arc<SQL<CRef<MType>>>)> {
    match &*expr.expr {
        Expr::SQL(_) => Err(CompileError::internal(
            SourceLocation::Unknown,
            "Cannot call intern_nonsql_placeholder on a SQL expression",
        )),
        _ => {
            let placeholder_name = "@".to_string() + compiler.next_placeholder(kind)?.as_str();

            Ok((
                param_ident(placeholder_name.clone()),
                Arc::new(SQL {
                    names: SQLNames {
                        params: Params::from([(placeholder_name.clone().into(), expr.clone())]),
                        unbound: BTreeSet::new(),
                    },
                    body: SQLBody::Expr(sqlast::Expr::Identifier(param_ident(
                        placeholder_name.clone(),
                    ))),
                }),
            ))
        }
    }
}

pub fn intern_cref_placeholder(
    compiler: Compiler,
    kind: String,
    te: CTypedExpr,
) -> Result<CTypedSQL> {
    let type_ = te.type_.clone();
    let sql = te.expr.clone().then(move |expr: Ref<Expr<CRef<MType>>>| {
        let te = te.clone();
        let sqlexpr: SQL<CRef<MType>> = intern_placeholder(
            compiler.clone(),
            kind.as_str(),
            &TypedExpr {
                type_: te.type_.clone(),
                expr: Arc::new(expr.read()?.clone()),
            },
        )?
        .as_ref()
        .clone();
        Ok(mkcref(sqlexpr))
    })?;
    Ok(CTypedSQL { type_, sql })
}

pub fn compile_sqlarg(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    loc: &SourceLocation,
    expr: &sqlast::Expr,
) -> Result<CTypedSQL> {
    let compiled = compile_sqlexpr(compiler.clone(), schema.clone(), scope.clone(), loc, expr)?;
    intern_cref_placeholder(compiler.clone(), "param".to_string(), compiled)
}

pub type CSQLNames = SQLNames<CRef<MType>>;

pub fn combine_crefs<T: 'static + Constrainable>(all: Vec<CRef<T>>) -> Result<CRef<Vec<Ref<T>>>> {
    let mut ret = mkcref(Vec::new());

    for a in all {
        ret = ret.then(move |sofar: Ref<Vec<Ref<T>>>| {
            a.then(move |a: Ref<T>| Ok(mkcref(vec![sofar.read()?.clone(), vec![a]].concat())))
        })?;
    }

    Ok(ret)
}

pub fn combine_sqlnames(all: &Vec<Ref<SQL<CRef<MType>>>>) -> Result<CSQLNames> {
    let mut ret = CSQLNames::new();
    for e in all {
        ret.extend(e.read()?.names.clone());
    }
    Ok(ret)
}

// This trait is used to compile sql expressions with their parameters, without computing
// their types. This is useful for expressions that do not directly impact a query's type,
// e.g. the OVER clause of a window function, or an ORDER BY clause.
type CSQLSnippet<T> = SQLSnippet<CRef<MType>, T>;
type CRefSnippet<T> = CRef<CWrap<CSQLSnippet<T>>>;

trait CompileSQL<Wrapper = CRefSnippet<Self>>: Clone + fmt::Debug + Send + Sync + 'static {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<Wrapper>;
}

impl<T> CompileSQL for Option<T>
where
    T: CompileSQL,
{
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        match self {
            Some(t) => {
                let c_t = t.compile_sql(compiler, schema, scope, loc)?;
                compiler.async_cref(async move {
                    let t = c_t.await?;
                    let t = cunwrap(t)?;
                    Ok(CSQLSnippet::wrap(t.names, Some(t.body)))
                })
            }
            None => Ok(CSQLSnippet::wrap(CSQLNames::new(), None)),
        }
    }
}

impl<T> CompileSQL for Box<T>
where
    T: CompileSQL,
{
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let c_t = self.as_ref().compile_sql(compiler, schema, scope, loc)?;
        compiler.async_cref(async move {
            let t = c_t.await?;
            let t = cunwrap(t)?;
            Ok(CSQLSnippet::wrap(t.names, Box::new(t.body)))
        })
    }
}

impl<T> CompileSQL for Vec<T>
where
    T: CompileSQL,
{
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let c_vec = combine_crefs(
            self.iter()
                .map(|t| t.compile_sql(compiler, schema, scope, loc))
                .collect::<Result<Vec<_>>>()?,
        )?;
        compiler.async_cref(async move {
            let c_vec = c_vec.await?;
            let mut names = CSQLNames::new();
            let mut ret = Vec::new();
            for element in c_vec.read()?.iter() {
                let element = cunwrap(element.clone())?;
                names.extend(element.names);
                ret.push(element.body);
            }

            Ok(CSQLSnippet::wrap(names, ret))
        })
    }
}

// This is not the primary implementation of compiling an expression, but is a convenience function so that we can
// run compile_sqlarg through the compile_sql() trait (e.g. to help compile an Option<Expr>).
impl CompileSQL for sqlast::Expr {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let compiled = compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, self)?;
        compiler.async_cref(async move {
            let compiled = compiled.sql.await?;
            let compiled = compiled.read()?;
            Ok(CSQLSnippet::wrap(
                compiled.names.clone(),
                compiled.body.as_expr(),
            ))
        })
    }
}

impl CompileSQL for sqlast::TableFactor {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let file = schema.read()?.file.clone();
        Ok(match self {
            sqlast::TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                let loc = path_location(&name.0.to_path(file.clone()));

                if args.is_some() {
                    return Err(CompileError::unimplemented(
                        loc.clone(),
                        "Table valued functions",
                    ));
                }

                if with_hints.len() > 0 {
                    return Err(CompileError::unimplemented(loc.clone(), "WITH hints"));
                }

                // TODO: This currently assumes that table references always come from outside
                // the query, which is not actually the case.
                //
                let relation = compile_reference(
                    compiler.clone(),
                    schema.clone(),
                    &name.to_path(file.clone()),
                )?;

                let list_type = mkcref(MType::List(Located::new(
                    MType::new_unknown(format!("FROM {}", name.to_string()).as_str()),
                    loc.clone(),
                )));
                list_type.unify(&relation.type_)?;

                let name = match alias {
                    Some(a) => a.name.clone(),
                    None => name
                        .0
                        .last()
                        .ok_or_else(|| {
                            CompileError::internal(
                                loc.clone(),
                                "Table name must have at least one part",
                            )
                        })?
                        .clone(),
                };

                let mut from_names = CSQLNames::new();

                scope
                    .write()?
                    .add_reference(&name.get().into(), &loc, relation.type_.clone())?;

                let placeholder_name =
                    QS_NAMESPACE.to_string() + compiler.next_placeholder("rel")?.as_str();
                from_names
                    .params
                    .insert(placeholder_name.clone().into(), relation);

                CSQLSnippet::wrap(
                    from_names,
                    sqlast::TableFactor::Table {
                        name: sqlast::ObjectName(vec![param_ident(placeholder_name)]),
                        alias: Some(sqlast::TableAlias {
                            name: name.clone(),
                            columns: Vec::new(),
                        }),
                        args: None,
                        with_hints: Vec::new(),
                    },
                )
            }
            sqlast::TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    // This is a lateral subquery, which I haven't tested yet (because it will require
                    // forwarding the outer scope into the subquery).
                    return Err(CompileError::unimplemented(
                        loc.clone(),
                        "Lateral Subqueries",
                    ));
                }

                // NOTE: Once we thread locations through the parse tree, we should use the location here.
                let (_scope, subquery_type, subquery) = compile_sqlquery(
                    compiler.clone(),
                    schema.clone(),
                    Some(scope.clone()),
                    loc,
                    subquery,
                )?;

                let (loc, name) = match alias {
                    Some(a) => (
                        a.name
                            .location()
                            .as_ref()
                            .map(|r| SourceLocation::from_file_range(file.clone(), Some(r.clone())))
                            .unwrap_or(loc.clone()),
                        a.name.clone(),
                    ),
                    None => (
                        loc.clone(),
                        param_ident(compiler.next_placeholder("anonymous_subquery")?),
                    ),
                };

                scope
                    .write()?
                    .add_reference(&name.get().into(), &loc, subquery_type.clone())?;

                let lateral = *lateral;
                compiler.async_cref(async move {
                    let subquery_expr = cunwrap(subquery.await?)?;

                    Ok(CSQLSnippet::wrap(
                        subquery_expr.names,
                        sqlast::TableFactor::Derived {
                            lateral,
                            subquery: Box::new(subquery_expr.body),
                            alias: Some(sqlast::TableAlias {
                                name: name.clone(),
                                columns: Vec::new(),
                            }),
                        },
                    ))
                })?
            }
            sqlast::TableFactor::TableFunction { .. } => {
                return Err(CompileError::unimplemented(loc.clone(), "TABLE"))
            }
            sqlast::TableFactor::UNNEST { .. } => {
                return Err(CompileError::unimplemented(loc.clone(), "UNNEST"))
            }
            sqlast::TableFactor::NestedJoin { .. } => {
                return Err(CompileError::unimplemented(loc.clone(), "Nested JOIN"))
            }
        })
    }
}

impl CompileSQL for sqlast::JoinConstraint {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        use sqlast::JoinConstraint::*;
        Ok(match self {
            On(e) => {
                let sql = compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, &e)?;
                sql.type_
                    .unify(&resolve_global_atom(compiler.clone(), "bool")?)?;
                compiler.async_cref(async move {
                    let sql = sql.sql.await?;
                    let sql = sql.read()?;
                    Ok(CSQLSnippet::wrap(sql.names.clone(), On(sql.body.as_expr())))
                })?
            }

            // USING is quite difficult to implement, because it also affects the type of the query itself. If a field
            // is "used" in a USING, then it's only projected from the first relation. We should probably store an exclusion
            // list on the scope, and then not include the field(s) during get_available_references(). Additionally, we will
            // need to pass the target table down here (the left side of the USING field should be whichever references is
            // available prior to excluding the field from the USING clause).
            Using(_) => return Err(CompileError::unimplemented(loc.clone(), "JOIN ... USING")),

            Natural => CSQLSnippet::wrap(CSQLNames::new(), Natural),
            None => CSQLSnippet::wrap(CSQLNames::new(), None),
        })
    }
}

impl CompileSQL for sqlast::JoinOperator {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        use sqlast::JoinOperator::*;
        let join_constructor = match self {
            Inner(_) => Inner,
            LeftOuter(_) => LeftOuter,
            RightOuter(_) => RightOuter,
            FullOuter(_) => FullOuter,
            o => {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    format!("{:?}", o).as_str(),
                ))
            }
        };

        Ok(match self {
            Inner(c) | LeftOuter(c) | RightOuter(c) | FullOuter(c) => {
                let constraint = c.compile_sql(compiler, schema, scope, loc)?;
                compiler.async_cref(async move {
                    let join_constraint = cunwrap(constraint.await?)?;
                    Ok(CSQLSnippet::wrap(
                        join_constraint.names,
                        join_constructor(join_constraint.body),
                    ))
                })?
            }
            _ => unreachable!(),
        })
    }
}

impl CompileSQL for sqlast::Join {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let relation = self.relation.compile_sql(compiler, schema, scope, loc)?;
        let join_operator = self
            .join_operator
            .compile_sql(compiler, schema, scope, loc)?;
        compiler.async_cref(async move {
            let relation = cunwrap(relation.await?)?;
            let join_operator = cunwrap(join_operator.await?)?;

            let mut names = CSQLNames::new();
            names.extend(relation.names);
            names.extend(join_operator.names);
            Ok(CSQLSnippet::wrap(
                names,
                sqlast::Join {
                    relation: relation.body,
                    join_operator: join_operator.body,
                },
            ))
        })
    }
}

impl CompileSQL for sqlast::TableWithJoins {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let c_relation = self.relation.compile_sql(compiler, schema, scope, loc)?;
        let c_joins = self.joins.compile_sql(compiler, schema, scope, loc)?;

        compiler.async_cref(async move {
            let relation = cunwrap(c_relation.await?)?;
            let joins = cunwrap(c_joins.await?)?;

            let mut table_params = CSQLNames::new();
            table_params.extend(relation.names);
            table_params.extend(joins.names);

            Ok(CSQLSnippet::wrap(
                table_params,
                sqlast::TableWithJoins {
                    relation: relation.body,
                    joins: joins.body,
                },
            ))
        })
    }
}

pub fn compile_from(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    parent_scope: Option<Ref<SQLScope>>,
    loc: &SourceLocation,
    from: &Vec<sqlast::TableWithJoins>,
) -> Result<(Ref<SQLScope>, CRefSnippet<Vec<sqlast::TableWithJoins>>)> {
    let scope = SQLScope::new(parent_scope);
    let from = from.compile_sql(compiler, schema, &scope, loc)?;

    Ok((scope, from))
}

pub fn compile_order_by(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    order_by: &Vec<sqlast::OrderByExpr>,
) -> Result<CRef<CWrap<(CSQLNames, Vec<sqlast::OrderByExpr>)>>> {
    let mut compiled_order_by = Vec::new();
    let mut compiled_opts = Vec::new();
    for ob in order_by {
        compiled_order_by.push(compile_gb_ob_expr(
            compiler.clone(),
            schema.clone(),
            scope.clone(),
            loc,
            &ob.expr,
        )?);
        compiled_opts.push((ob.asc.clone(), ob.nulls_first.clone()));
    }

    compiler.async_cref({
        async move {
            let mut resolved_order_by = Vec::new();
            let mut names = CSQLNames::new();
            for (expr, (asc, nulls_first)) in
                compiled_order_by.into_iter().zip(compiled_opts.into_iter())
            {
                let resolved_expr = expr.await?;
                let resolved_expr = resolved_expr.read()?;
                names.extend(resolved_expr.names.clone());
                resolved_order_by.push(sqlast::OrderByExpr {
                    expr: resolved_expr.body.as_expr(),
                    asc,
                    nulls_first,
                });
            }

            Ok(cwrap((names, resolved_order_by)))
        }
    })
}

pub fn compile_gb_ob_expr(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    loc: &SourceLocation,
    expr: &sqlast::Expr,
) -> Result<CRef<SQL<CRef<MType>>>> {
    let maybe_numeric_ref = match expr {
        sqlast::Expr::Value(sqlast::Value::Number(n, _)) => n.parse::<u64>().ok().map(|_| SQL {
            names: CSQLNames::new(),
            body: SQLBody::Expr(expr.clone()),
        }),
        _ => None,
    };

    Ok(match maybe_numeric_ref {
        Some(numeric_ref) => mkcref(numeric_ref),
        _ => {
            let compiled =
                compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), &loc, expr)?;
            compiled.sql
        }
    })
}

fn extract_renamings(
    file: Option<String>,
    o: &sqlast::WildcardAdditionalOptions,
) -> BTreeMap<Ident, Option<Located<Ident>>> {
    let mut ret = BTreeMap::new();

    if let Some(exclude) = &o.opt_exclude {
        use sqlast::ExcludeSelectItem::*;
        match exclude {
            Single(i) => {
                ret.insert(i.get().into(), None);
            }
            Multiple(l) => {
                ret.extend(l.iter().map(|i| (i.get().into(), None)));
            }
        }
    }

    if let Some(except) = &o.opt_except {
        ret.insert(except.first_element.get().into(), None);
        ret.extend(
            except
                .additional_elements
                .iter()
                .map(|i| (i.get().into(), None)),
        );
    }

    if let Some(rename) = &o.opt_rename {
        use sqlast::RenameSelectItem::*;
        match rename {
            Single(i) => {
                ret.insert(
                    i.ident.get().into(),
                    Some(Ident::from_located_sqlident(file.clone(), i.alias.clone())),
                );
            }
            Multiple(l) => {
                ret.extend(l.iter().map(|i| {
                    (
                        i.ident.get().into(),
                        Some(Ident::from_located_sqlident(file.clone(), i.alias.clone())),
                    )
                }));
            }
        }
    }

    ret
}

pub fn compile_select(
    compiler: Compiler,
    schema: Ref<Schema>,
    parent_scope: Option<Ref<SQLScope>>,
    loc: &SourceLocation,
    select: &sqlast::Select,
) -> Result<(
    Ref<SQLScope>,
    CRef<MType>,
    CRef<CWrap<(CSQLNames, sqlast::Select)>>,
)> {
    if select.top.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "TOP"));
    }

    if select.into.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "INTO"));
    }

    if select.lateral_views.len() > 0 {
        return Err(CompileError::unimplemented(loc.clone(), "Lateral views"));
    }

    if select.cluster_by.len() > 0 {
        return Err(CompileError::unimplemented(loc.clone(), "CLUSTER BY"));
    }

    if select.distribute_by.len() > 0 {
        return Err(CompileError::unimplemented(loc.clone(), "DISTRIBUTE BY"));
    }

    if select.sort_by.len() > 0 {
        return Err(CompileError::unimplemented(loc.clone(), "SORT BY"));
    }

    if select.having.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "HAVING"));
    }

    if select.qualify.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "QUALIFY"));
    }

    let (scope, from) = compile_from(&compiler, &schema, parent_scope.clone(), loc, &select.from)?;

    let exprs = select
        .projection
        .iter()
        .map(|p| {
            Ok(match p {
                sqlast::SelectItem::UnnamedExpr(expr) => {
                    // If the expression is an identifier, then simply forward it along. In the case of a
                    // compound identifier (e.g. table.foo), SQL semantics are to pick the last element (i.e.
                    // foo) as the new name.
                    let name: Ident = match expr {
                        sqlast::Expr::Identifier(i) => i.get().into(),
                        sqlast::Expr::CompoundIdentifier(c) => c
                            .last()
                            .expect("Compound identifiers should have at least one element")
                            .get()
                            .into(),
                        _ => format!("{}", expr).into(),
                    };
                    let compiled =
                        compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, expr)?;
                    mkcref(vec![CTypedNameAndSQL {
                        name: Ident::with_location(loc.clone(), name),
                        type_: compiled.type_,
                        sql: compiled.sql,
                    }])
                }
                sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                    let compiled =
                        compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, expr)?;
                    mkcref(vec![CTypedNameAndSQL {
                        name: Ident::from_sqlident(loc.clone(), alias.get().clone()),
                        type_: compiled.type_,
                        sql: compiled.sql,
                    }])
                }
                sqlast::SelectItem::Wildcard(..) | sqlast::SelectItem::QualifiedWildcard { .. } => {
                    let (qualifier, renamings) = match p {
                        sqlast::SelectItem::Wildcard(options) => {
                            (None, extract_renamings(loc.file(), options))
                        }
                        sqlast::SelectItem::QualifiedWildcard(qualifier, options) => {
                            if qualifier.0.len() != 1 {
                                return Err(CompileError::unimplemented(
                                    loc.clone(),
                                    "Wildcard of lenght != 1",
                                ));
                            }

                            (
                                Some(qualifier.0[0].get().into()),
                                extract_renamings(loc.file(), options),
                            )
                        }
                        _ => unreachable!(),
                    };

                    let available =
                        scope
                            .read()?
                            .get_available_references(compiler.clone(), loc, qualifier)?;
                    available.then({
                        let loc = loc.clone();
                        move |available: Ref<AvailableReferences>| {
                            let mut ret = Vec::new();
                            for (_, m) in available.read()?.current_level().unwrap().iter() {
                                let type_ = match &m.type_ {
                                    Some(t) => t.clone(),
                                    None => {
                                        return Err(CompileError::duplicate_entry(vec![m
                                            .field
                                            .replace_location(loc.clone())]))
                                    }
                                };

                                let name = match renamings.get(&m.field) {
                                    Some(Some(n)) => n.clone(),
                                    Some(None) => continue,
                                    None => m.field.clone(),
                                };

                                let sqlpath = vec![m.relation.to_sqlident(), m.field.to_sqlident()];
                                ret.push(CTypedNameAndSQL {
                                    name,
                                    type_,
                                    sql: mkcref(SQL {
                                        names: CSQLNames::from_unbound(&sqlpath),
                                        body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(
                                            sqlpath,
                                        )),
                                    }),
                                });
                            }
                            Ok(mkcref(ret))
                        }
                    })?
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let projections = combine_crefs(exprs)?;

    let type_: CRef<MType> = projections.then({
        let loc = loc.clone();
        move |exprs: Ref<Vec<Ref<Vec<CTypedNameAndSQL>>>>| {
            let mut fields = Vec::new();
            for a in &*exprs.read()? {
                for b in &*a.read()? {
                    fields.push(MField {
                        name: b.name.get().clone(),
                        type_: b.type_.clone(),
                        nullable: true,
                    });
                }
            }

            Ok(mkcref(MType::List(Located::new(
                mkcref(MType::Record(Located::new(fields, loc.clone()))),
                loc.clone(),
            ))))
        }
    })?;

    let select = select.clone();
    let expr: CRef<_> = compiler.clone().async_cref({
        let scope = scope.clone();
        let loc = loc.clone();
        async move {
            let from = cunwrap(from.await?)?;

            let exprs = projections.await?;
            let mut proj_exprs = Vec::new();
            let exprs = (*exprs.read()?).clone();
            for a in exprs {
                let a = (*a.read()?).clone();
                for b in a {
                    let n = b.name.clone();
                    proj_exprs.push(NameAndSQL {
                        name: n.clone(),
                        sql: Arc::new(b.sql.await?.read()?.clone()),
                    });
                }
            }

            let select = select.clone();

            let mut names = CSQLNames::new();
            let mut projection = Vec::new();
            for sqlexpr in &*proj_exprs {
                names.extend(sqlexpr.sql.names.clone());
                projection.push(sqlast::SelectItem::ExprWithAlias {
                    alias: sqlexpr.name.to_sqlident(),
                    expr: sqlexpr.sql.body.as_expr(),
                });
            }
            names.extend(from.names);

            let selection = match &select.selection {
                Some(selection) => {
                    let compiled = compile_sqlarg(
                        compiler.clone(),
                        schema.clone(),
                        scope.clone(),
                        &loc,
                        selection,
                    )?;
                    compiled
                        .type_
                        .unify(&resolve_global_atom(compiler.clone(), "bool")?)?;
                    let sql = compiled.sql.await?.read()?.clone();
                    names.extend(sql.names.clone());
                    Some(sql.body.as_expr())
                }
                None => None,
            };

            let mut group_by = Vec::new();
            for gb in &select.group_by {
                let sql =
                    compile_gb_ob_expr(compiler.clone(), schema.clone(), scope.clone(), &loc, gb)?
                        .await?;
                let sql = sql.read()?;
                names.extend(sql.names.clone());
                group_by.push(sql.body.as_expr());
            }

            let mut ret = select.clone();
            ret.from = from.body;
            ret.projection = projection;
            ret.selection = selection;
            ret.group_by = group_by;

            let names = scope
                .read()?
                .remove_bound_references(compiler.clone(), names)?;
            let names = names.await?.read()?.clone();

            Ok(cwrap((names, ret)))
        }
    })?;

    Ok((scope, type_, expr))
}

pub async fn finish_sqlexpr(
    loc: &SourceLocation,
    expr: CRef<Expr<CRef<MType>>>,
    names: &mut CSQLNames,
) -> Result<sqlast::Expr> {
    let expr = expr.clone_inner().await?;
    Ok(match expr {
        Expr::SQL(s) => {
            names.extend(s.names.clone());
            s.body.as_expr()
        }
        _ => {
            return Err(CompileError::unimplemented(
                loc.clone(),
                "Non-SQL expression",
            ))
        }
    })
}

pub fn compile_sqlquery(
    compiler: Compiler,
    schema: Ref<Schema>,
    parent_scope: Option<Ref<SQLScope>>,
    loc: &SourceLocation,
    query: &sqlast::Query,
) -> Result<(Ref<SQLScope>, CRef<MType>, CRefSnippet<sqlast::Query>)> {
    if query.with.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "WITH"));
    }

    let limit = match &query.limit {
        Some(limit) => {
            let expr = compile_sqlexpr(
                compiler.clone(),
                schema.clone(),
                SQLScope::empty(),
                loc,
                &limit,
            )?;
            expr.type_
                .unify(&resolve_global_atom(compiler.clone(), "bigint")?)?;

            Some(expr)
        }
        None => None,
    };

    let offset = match &query.offset {
        Some(offset) => {
            let expr = compile_sqlexpr(
                compiler.clone(),
                schema.clone(),
                SQLScope::empty(),
                loc,
                &offset.value,
            )?;
            expr.type_
                .unify(&resolve_global_atom(compiler.clone(), "bigint")?)?;

            Some((expr, offset.rows.clone()))
        }
        None => None,
    };

    if query.fetch.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "FETCH"));
    }

    if query.locks.len() > 0 {
        return Err(CompileError::unimplemented(
            loc.clone(),
            "FOR { UPDATE | SHARE }",
        ));
    }

    let (scope, type_, set_expr) =
        compile_setexpr(&compiler, &schema, parent_scope.clone(), loc, &query.body)?;

    Ok((
        scope.clone(),
        type_,
        compiler.async_cref({
            let compiled_order_by =
                compile_order_by(&compiler, &schema, &scope, loc, &query.order_by)?;

            let loc = loc.clone();
            let compiler = compiler.clone();
            async move {
                let body = cunwrap(set_expr.await?)?;
                let SQLSnippet { mut names, body } = body;
                let limit = match limit {
                    Some(limit) => Some(finish_sqlexpr(&loc, limit.expr, &mut names).await?),
                    None => None,
                };

                let offset = match offset {
                    Some((offset, rows)) => Some(sqlparser::ast::Offset {
                        value: finish_sqlexpr(&loc, offset.expr, &mut names).await?,
                        rows,
                    }),
                    None => None,
                };

                let (ob_names, order_by) = cunwrap(compiled_order_by.await?)?;
                names.extend(ob_names);

                let names = scope
                    .read()?
                    .remove_bound_references(compiler.clone(), names)?;
                let names = names.await?.read()?.clone();

                Ok(SQLSnippet::wrap(
                    names,
                    sqlast::Query {
                        with: None,
                        body: Box::new(body),
                        order_by,
                        limit,
                        offset,
                        fetch: None,
                        locks: Vec::new(),
                    },
                ))
            }
        })?,
    ))
}

pub fn compile_setexpr(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    parent_scope: Option<Ref<SQLScope>>,
    loc: &SourceLocation,
    set_expr: &sqlast::SetExpr,
) -> Result<(Ref<SQLScope>, CRef<MType>, CRefSnippet<sqlast::SetExpr>)> {
    match set_expr {
        sqlast::SetExpr::Select(s) => {
            let (scope, type_, select) =
                compile_select(compiler.clone(), schema.clone(), parent_scope, loc, s)?;

            Ok((
                scope,
                type_,
                compiler.async_cref(async move {
                    let (names, body) = cunwrap(select.await?)?;
                    Ok(SQLSnippet::wrap(
                        names,
                        sqlast::SetExpr::Select(Box::new(body)),
                    ))
                })?,
            ))
        }
        sqlast::SetExpr::Query(q) => {
            let (scope, type_, query) =
                compile_sqlquery(compiler.clone(), schema.clone(), parent_scope, loc, &q)?;

            Ok((
                scope,
                type_,
                compiler.async_cref(async move {
                    let query = cunwrap(query.await?)?;
                    Ok(SQLSnippet::wrap(
                        query.names,
                        sqlast::SetExpr::Query(Box::new(query.body)),
                    ))
                })?,
            ))
        }
        sqlast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let op = op.clone();
            let set_quantifier = set_quantifier.clone();
            let (left_scope, left_type, left_set_expr) =
                compile_setexpr(compiler, schema, parent_scope.clone(), loc, &left)?;
            let (_right_scope, right_type, right_set_expr) =
                compile_setexpr(compiler, schema, parent_scope.clone(), loc, &right)?;

            left_type.unify(&right_type)?;

            Ok((
                left_scope,
                left_type,
                compiler.async_cref({
                    async move {
                        let left = cunwrap(left_set_expr.await?)?;
                        let right = cunwrap(right_set_expr.await?)?;

                        let SQLSnippet {
                            names: mut left_names,
                            body: left_body,
                        } = left;
                        let SQLSnippet {
                            names: right_names,
                            body: right_body,
                        } = right;

                        left_names.extend(right_names);
                        Ok(SQLSnippet::wrap(
                            left_names,
                            sqlast::SetExpr::SetOperation {
                                op,
                                set_quantifier,
                                left: Box::new(left_body),
                                right: Box::new(right_body),
                            },
                        ))
                    }
                })?,
            ))
        }
        sqlast::SetExpr::Values(_) => Err(CompileError::unimplemented(loc.clone(), "VALUES")),
        sqlast::SetExpr::Insert(_) => Err(CompileError::unimplemented(loc.clone(), "INSERT")),
        sqlast::SetExpr::Table(_) => Err(CompileError::unimplemented(loc.clone(), "TABLE")),
    }
}

lazy_static! {
    static ref GLOBAL_COMPILER: Compiler = Compiler::new().unwrap();
    static ref NULL_SQLEXPR: Arc<SQL<CRef<MType>>> = Arc::new(SQL {
        names: CSQLNames::new(),
        body: SQLBody::Expr(sqlast::Expr::Value(sqlast::Value::Null)),
    });
    static ref NULL: CTypedExpr = CTypedExpr {
        type_: resolve_global_atom(GLOBAL_COMPILER.clone(), "null").unwrap(),
        expr: mkcref(Expr::SQL(NULL_SQLEXPR.clone())),
    };
}

fn apply_sqlcast(
    compiler: Compiler,
    sql: CRef<SQL<CRef<MType>>>,
    target_type: Ref<MType>,
) -> Result<CRef<SQL<CRef<MType>>>> {
    let target_type = target_type.read()?;
    let loc = target_type.location();
    let dt: ParserDataType = (&target_type
        .to_runtime_type()
        .context(RuntimeSnafu { loc: loc.clone() })?)
        .try_into()
        .context(TypesystemSnafu { loc: loc.clone() })?;

    Ok(compiler.async_cref(async move {
        let final_expr = sql.clone_inner().await?;
        Ok(mkcref(SQL {
            names: final_expr.names,
            body: SQLBody::Expr(sqlast::Expr::Cast {
                expr: Box::new(final_expr.body.as_expr()),
                data_type: dt,
            }),
        }))
    })?)
}

fn coerce_all(
    compiler: &Compiler,
    op: &sqlparser::ast::BinaryOperator,
    args: Vec<CTypedSQL>,
    unknown_debug_name: &str,
) -> Result<(CRef<MType>, Vec<CTypedSQL>)> {
    let mut exprs = Vec::new();
    let mut iter = args.iter();

    let mut target = CRef::new_unknown(unknown_debug_name);
    if let Some(first) = iter.next() {
        exprs.push(first.clone());
        target = first.type_.clone();
        for next in iter {
            exprs.push(next.clone());
            target = coerce(
                compiler.clone(),
                CoerceOp::Binary(op.clone()),
                target,
                next.type_.clone(),
            )?;
        }
    }

    let mut ret = Vec::new();
    for arg in exprs.into_iter() {
        let target2 = target.clone();
        let compiler2 = compiler.clone();

        ret.push(CTypedSQL {
            type_: target.clone(),
            sql: compiler.clone().async_cref(async move {
                let resolved_target = target2.await?;
                let resolved_arg = arg.type_.await?;
                let my_type = resolved_arg.read()?;
                let their_type = resolved_target.read()?;

                Ok(
                    if their_type.to_runtime_type().context(RuntimeSnafu {
                        loc: their_type.location(),
                    })? != my_type.to_runtime_type().context(RuntimeSnafu {
                        loc: my_type.location(),
                    })? {
                        apply_sqlcast(compiler2, arg.sql.clone(), resolved_target.clone())?
                    } else {
                        arg.sql
                    },
                )
            })?,
        })
    }

    Ok((target, ret))
}

pub fn unify_all<T, C, I>(mut iter: I, unknown_debug_name: &str) -> Result<CRef<T>>
where
    T: Constrainable + 'static,
    C: HasCType<T>,
    I: Iterator<Item = C>,
{
    if let Some(first) = iter.next() {
        for next in iter {
            first.type_().unify(&next.type_())?;
        }
        Ok(first.type_().clone())
    } else {
        Ok(CRef::new_unknown(unknown_debug_name))
    }
}

pub fn combine_sql_exprs<'a, I>(iter: I, names: &mut CSQLNames) -> Result<Vec<sqlast::Expr>>
where
    I: Iterator<Item = &'a Ref<SQL<CRef<MType>>>>,
{
    iter.map(|c| {
        let c = c.read()?;
        names.extend(c.names.clone());
        Ok(c.body.as_expr())
    })
    .collect::<Result<Vec<_>>>()
}

pub fn has_unbound_names(expr: Arc<Expr<CRef<MType>>>) -> bool {
    match expr.as_ref() {
        Expr::SQL(e) => {
            !e.names.unbound.is_empty()
                || e.names
                    .params
                    .iter()
                    .any(|(_, p)| has_unbound_names(p.expr.clone()))
        }
        _ => false,
    }
}

pub fn schema_infer_load_fn(
    schema: SchemaRef,
    args: Vec<TypedNameAndExpr<CRef<MType>>>,
    inner_type: CRef<MType>,
) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
    async move {
        let ctx = crate::runtime::Context::new(&schema, crate::runtime::SQLEngineType::DuckDB)
            .disable_typechecks();
        let mut runtime_args = Vec::new();
        for e in args {
            let runtime_expr = e.to_typed_expr().to_runtime_type().context(RuntimeSnafu {
                loc: SourceLocation::Unknown,
            })?;
            let eval_expr = crate::runtime::eval(&ctx, &runtime_expr).await.context({
                RuntimeSnafu {
                    loc: SourceLocation::Unknown,
                }
            })?;
            runtime_args.push(eval_expr);
        }
        let inferred_type = crate::runtime::functions::LoadFileFn::infer(&ctx, runtime_args)
            .await
            .context(RuntimeSnafu {
                loc: SourceLocation::Unknown,
            })?;

        let inferred_mtype = mkcref(MType::from_runtime_type(&inferred_type)?);

        inner_type.unify(&inferred_mtype)?;
        Ok(())
    }
}

// This implementation is used to compile OrderByExprs that cannot refer to projection
// terms (e.g. ORDER BY 1 _cannot_ be compiled by this function). It's used by window
// functions, array_agg, etc.
impl CompileSQL for sqlast::OrderByExpr {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let cexpr = self.expr.compile_sql(compiler, schema, scope, loc)?;

        let asc = self.asc.clone();
        let nulls_first = self.nulls_first.clone();
        Ok(compiler.async_cref(async move {
            let ob = cunwrap(cexpr.await?)?;
            Ok(CSQLSnippet::wrap(
                ob.names,
                sqlast::OrderByExpr {
                    expr: ob.body,
                    asc,
                    nulls_first,
                },
            ))
        })?)
    }
}

impl CompileSQL for sqlast::WindowFrameBound {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        use sqlast::WindowFrameBound::*;
        Ok(match self {
            CurrentRow => CSQLSnippet::wrap(CSQLNames::new(), CurrentRow),
            Preceding(e) | Following(e) => {
                let preceding = match self {
                    Preceding(_) => true,
                    Following(_) => false,
                    _ => unreachable!(),
                };

                let c_e = e.compile_sql(compiler, schema, scope, loc)?;
                compiler.async_cref(async move {
                    let c_e = cunwrap(c_e.await?)?;
                    Ok(CSQLSnippet::wrap(
                        c_e.names,
                        match preceding {
                            true => Preceding(c_e.body),
                            false => Following(c_e.body),
                        },
                    ))
                })?
            }
        })
    }
}

impl CompileSQL for sqlast::WindowFrame {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let sqlast::WindowFrame {
            units,
            start_bound,
            end_bound,
        } = self;
        let c_start = start_bound.compile_sql(compiler, schema, scope, loc)?;
        let c_end = end_bound.compile_sql(compiler, schema, scope, loc)?;
        let units = units.clone();
        compiler.async_cref(async move {
            let start_bound = cunwrap(c_start.await?)?;
            let end_bound = cunwrap(c_end.await?)?;

            let mut names = CSQLNames::new();
            names.extend(start_bound.names);
            names.extend(end_bound.names);

            Ok(CSQLSnippet::wrap(
                names,
                sqlast::WindowFrame {
                    units,
                    start_bound: start_bound.body,
                    end_bound: end_bound.body,
                },
            ))
        })
    }
}

impl CompileSQL for sqlast::WindowSpec {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let sqlast::WindowSpec {
            partition_by,
            order_by,
            window_frame,
        } = self;

        let c_partition_by = partition_by.compile_sql(compiler, schema, scope, loc)?;
        let c_order_by = order_by.compile_sql(compiler, schema, scope, loc)?;

        let window_frame = window_frame.compile_sql(compiler, schema, scope, loc)?;

        compiler.async_cref(async move {
            let partition_by = cunwrap(c_partition_by.await?)?;
            let order_by = cunwrap(c_order_by.await?)?;
            let window_frame = cunwrap(window_frame.await?)?;

            let mut names = SQLNames::new();
            names.extend(partition_by.names);
            names.extend(order_by.names);
            names.extend(window_frame.names);

            Ok(CSQLSnippet::wrap(
                names,
                sqlast::WindowSpec {
                    partition_by: partition_by.body,
                    order_by: order_by.body,
                    window_frame: window_frame.body,
                },
            ))
        })
    }
}

async fn extract_scalar_subselect_field(
    loc: &SourceLocation,
    query_type: &MType,
) -> Result<MField> {
    let inner_record = match &query_type {
        MType::List(t) => {
            let inner_type = t.get().await?;
            let inner_type = inner_type.read()?;
            match &*inner_type {
                MType::Record(r) => Some(r.clone()),
                _ => None,
            }
        }
        _ => None,
    };

    let inner_record = match inner_record {
        Some(i) => i,
        None => {
            return Err(CompileError::internal(
                loc.clone(),
                format!(
                    "Subselect expected to return a list of records (not {:?})",
                    query_type
                )
                .as_str(),
            ))
        }
    };

    Ok(if inner_record.get().len() == 1 {
        inner_record.get()[0].clone()
    } else {
        return Err(CompileError::scalar_subselect(
            inner_record.location().clone(),
            format!(
                "should return a single field (not {})",
                inner_record.get().len()
            )
            .as_str(),
        ));
    })
}

pub fn compile_sqlexpr(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    loc: &SourceLocation,
    expr: &sqlast::Expr,
) -> Result<CTypedExpr> {
    let file = schema.read()?.file.clone();
    let c_sqlarg =
        |e: &sqlast::Expr| compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, e);

    let ret = match expr {
        sqlast::Expr::Value(v) => match v {
            sqlast::Value::Number(n, _) => {
                let numeric_type =
                    parse_numeric_type(n).context(TypesystemSnafu { loc: loc.clone() })?;
                let parser_data_type: ParserDataType = (&Type::Atom(numeric_type.clone()))
                    .try_into()
                    .context(TypesystemSnafu { loc: loc.clone() })?;

                CTypedExpr {
                    type_: mkcref(MType::Atom(Located::new(numeric_type, loc.clone()))),
                    expr: mkcref(Expr::SQL(Arc::new(SQL {
                        names: CSQLNames::new(),
                        body: SQLBody::Expr(sqlast::Expr::Cast {
                            expr: Box::new(expr.clone()),
                            data_type: parser_data_type,
                        }),
                    }))),
                }
            }
            sqlast::Value::SingleQuotedString(_)
            | sqlast::Value::EscapedStringLiteral(_)
            | sqlast::Value::NationalStringLiteral(_)
            | sqlast::Value::HexStringLiteral(_)
            | sqlast::Value::UnQuotedString(_)
            | sqlast::Value::DollarQuotedString(_)
            | sqlast::Value::DoubleQuotedString(_) => CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "string")?,
                expr: mkcref(Expr::SQL(Arc::new(SQL {
                    names: CSQLNames::new(),
                    body: SQLBody::Expr(expr.clone()),
                }))),
            },
            sqlast::Value::Boolean(_) => CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "bool")?,
                expr: mkcref(Expr::SQL(Arc::new(SQL {
                    names: CSQLNames::new(),
                    body: SQLBody::Expr(expr.clone()),
                }))),
            },
            sqlast::Value::Null => NULL.clone(),
            sqlast::Value::Placeholder(_) => {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    format!("SQL Parameter syntax: {}", expr).as_str(),
                ))
            }
        },
        sqlast::Expr::Array(sqlast::Array { elem, .. }) => {
            let c_elems = elem
                .iter()
                .map(|e| compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, e))
                .collect::<Result<Vec<_>>>()?;
            let mut c_elem_iter = c_elems.iter();
            let data_type = if let Some(first) = c_elem_iter.next() {
                for next in c_elem_iter {
                    first.type_.unify(&next.type_)?;
                }
                first.type_.clone()
            } else {
                mkcref(MType::Atom(Located::new(AtomicType::Null, loc.clone())))
            };

            CTypedExpr {
                type_: data_type,
                expr: combine_crefs(c_elems.iter().map(|s| s.sql.clone()).collect())?.then({
                    let expr = expr.clone();
                    move |args: Ref<Vec<Ref<SQL<CRef<MType>>>>>| {
                        let names = combine_sqlnames(&*args.read()?)?;
                        Ok(mkcref(Expr::SQL(Arc::new(SQL {
                            names,
                            body: SQLBody::Expr(expr.clone()),
                        }))))
                    }
                })?,
            }
        }
        sqlast::Expr::IsNull(e) | sqlast::Expr::IsNotNull(e) => {
            let compiled =
                compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, &e)?;
            let constructor = match expr {
                sqlast::Expr::IsNull(_) => sqlast::Expr::IsNull,
                sqlast::Expr::IsNotNull(_) => sqlast::Expr::IsNotNull,
                _ => unreachable!(),
            };
            CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "bool")?,
                expr: compiled.sql.then({
                    move |sqlexpr: Ref<SQL<CRef<MType>>>| {
                        Ok(mkcref(Expr::SQL(Arc::new(SQL {
                            names: sqlexpr.read()?.names.clone(),
                            body: SQLBody::Expr(constructor(Box::new(
                                sqlexpr.read()?.body.as_expr(),
                            ))),
                        }))))
                    }
                })?,
            }
        }
        sqlast::Expr::BinaryOp { left, op, right } => {
            let op = op.clone();
            let mut cleft = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                left.as_ref(),
            )?;
            let mut cright = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                right.as_ref(),
            )?;
            use sqlast::BinaryOperator::*;
            let type_ = match op {
                Plus | Minus | Multiply | Divide => {
                    let (result_type, casted) = coerce_all(
                        &compiler,
                        &op,
                        vec![cleft, cright],
                        format!("{:?}", op).as_str(),
                    )?;
                    (cleft, cright) = (casted[0].clone(), casted[1].clone());
                    result_type
                }
                Eq | NotEq | Lt | LtEq | Gt | GtEq => {
                    let (_, casted) = coerce_all(
                        &compiler,
                        &op,
                        vec![cleft, cright],
                        format!("{:?}", op).as_str(),
                    )?;
                    (cleft, cright) = (casted[0].clone(), casted[1].clone());
                    resolve_global_atom(compiler.clone(), "bool")?
                }

                And | Or | Xor | BitwiseOr | BitwiseAnd | BitwiseXor | PGBitwiseXor
                | PGBitwiseShiftLeft | PGBitwiseShiftRight => {
                    let bool_val = CTypedSQL {
                        type_: resolve_global_atom(compiler.clone(), "bool")?,
                        sql: mkcref(SQL {
                            names: CSQLNames::new(),
                            body: SQLBody::Expr(sqlast::Expr::Value(sqlast::Value::Null)),
                        }),
                    };
                    let (_, casted) = coerce_all(
                        &compiler,
                        &op,
                        vec![cleft, cright, bool_val],
                        format!("{:?}", op).as_str(),
                    )?;
                    (cleft, cright) = (casted[0].clone(), casted[1].clone());
                    resolve_global_atom(compiler.clone(), "bool")?
                }
                _ => {
                    return Err(CompileError::unimplemented(
                        loc.clone(),
                        format!("Binary operator: {}", op).as_str(),
                    ));
                }
            };
            CTypedExpr {
                type_,
                expr: combine_crefs(vec![cleft.sql, cright.sql])?.then({
                    move |args: Ref<Vec<Ref<SQL<CRef<MType>>>>>| {
                        let names = combine_sqlnames(&*args.read()?)?;
                        Ok(mkcref(Expr::SQL(Arc::new(SQL {
                            names,
                            body: SQLBody::Expr(sqlast::Expr::BinaryOp {
                                left: Box::new(args.read()?[0].read()?.body.as_expr()),
                                op: op.clone(),
                                right: Box::new(args.read()?[1].read()?.body.as_expr()),
                            }),
                        }))))
                    }
                })?,
            }
        }
        sqlast::Expr::UnaryOp { op, expr } => {
            let op = op.clone();
            let cexpr = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                expr.as_ref(),
            )?;
            use sqlast::UnaryOperator::*;
            let type_ = match op {
                Plus | Minus => {
                    // NOTE: There is some logic varies per SQL engine about which types are
                    // accepted in the + and - unary operators. Ideally, we throw a compiler error
                    // here, depending on the engine.
                    cexpr.type_.clone()
                }
                Not => resolve_global_atom(compiler.clone(), "bool")?,
                _ => {
                    return Err(CompileError::unimplemented(
                        loc.clone(),
                        format!("Unary operator: {}", op).as_str(),
                    ));
                }
            };
            CTypedExpr {
                type_,
                expr: compiler.async_cref(async move {
                    let expr = cexpr.sql.await?;
                    let expr = expr.read()?;

                    Ok(mkcref(Expr::SQL(Arc::new(SQL {
                        names: expr.names.clone(),
                        body: SQLBody::Expr(sqlast::Expr::UnaryOp {
                            op,
                            expr: Box::new(expr.body.as_expr()),
                        }),
                    }))))
                })?,
            }
        }
        sqlast::Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let c_operand = match operand {
                Some(o) => Some(c_sqlarg(&o)?),
                None => None,
            };

            let c_conditions = conditions
                .iter()
                .map(|c| c_sqlarg(&c))
                .collect::<Result<Vec<_>>>()?;

            // If there's an operand, then unify the conditions against it, otherwise
            // unify them to bool.
            let condition_type = match &c_operand {
                Some(o) => o.type_.clone(),
                None => resolve_global_atom(compiler.clone(), "bool")?,
            };

            for c_cond in c_conditions.iter() {
                c_cond.type_.unify(&condition_type)?;
            }

            let mut c_results = results
                .iter()
                .map(|c| c_sqlarg(&c))
                .collect::<Result<Vec<_>>>()?;

            if let Some(e) = else_result {
                let ret = c_sqlarg(e)?;
                c_results.push(ret);
            }

            let (result_type, mut c_results) = coerce_all(
                &compiler,
                &sqlast::BinaryOperator::Eq,
                c_results,
                "case result",
            )?;

            let c_else_result = match else_result {
                Some(_) => Some(c_results.pop().unwrap()),
                None => None,
            };

            let combined_conditions =
                combine_crefs(c_conditions.iter().map(|s| s.sql.clone()).collect())?;
            let combined_results =
                combine_crefs(c_results.iter().map(|s| s.sql.clone()).collect())?;

            CTypedExpr {
                type_: result_type,
                expr: compiler.async_cref({
                    async move {
                        let mut names = CSQLNames::new();
                        let operand = match c_operand {
                            Some(ref o) => {
                                let operand = (&o.sql).await?;
                                let operand = operand.read()?;
                                names.extend(operand.names.clone());
                                Some(Box::new(operand.body.as_expr()))
                            }
                            None => None,
                        };

                        let conditions = combine_sql_exprs(
                            combined_conditions.await?.read()?.iter(),
                            &mut names,
                        )?;

                        let results =
                            combine_sql_exprs(combined_results.await?.read()?.iter(), &mut names)?;

                        let else_result = match c_else_result {
                            Some(ref o) => {
                                let operand = (&o.sql).await?;
                                let operand = operand.read()?;
                                names.extend(operand.names.clone());
                                Some(Box::new(operand.body.as_expr()))
                            }
                            None => None,
                        };

                        let body = sqlast::Expr::Case {
                            operand,
                            conditions,
                            results,
                            else_result,
                        };

                        Ok(mkcref(Expr::SQL(Arc::new(SQL {
                            names,
                            body: SQLBody::Expr(body),
                        }))))
                    }
                })?,
            }
        }
        sqlast::Expr::Function(sqlast::Function {
            name,
            args,
            over,
            distinct,
            special,
        }) => {
            let distinct = *distinct;
            let special = *special;
            let over = over.compile_sql(&compiler, &schema, &scope, loc)?;

            let func_name = name.to_path(file.clone());
            let func = compile_reference(compiler.clone(), schema.clone(), &func_name)?;
            let fn_type = match func
                .type_
                .must()
                .context(RuntimeSnafu { loc: loc.clone() })?
                .read()?
                .clone()
            {
                MType::Fn(f) => f,
                _ => {
                    return Err(CompileError::wrong_type(
                        &MType::Fn(Located::new(
                            MFnType {
                                args: Vec::new(),
                                ret: MType::new_unknown("ret"),
                            },
                            loc.clone(),
                        )),
                        &*func
                            .type_
                            .must()
                            .context(RuntimeSnafu { loc: loc.clone() })?
                            .read()?,
                    ))
                }
            };
            let mut compiled_args: BTreeMap<Ident, CTypedNameAndExpr> = BTreeMap::new();
            let mut pos: usize = 0;
            for arg in args {
                let (name, expr) = match arg {
                    sqlast::FunctionArg::Named { name, arg } => {
                        (Ident::with_location(loc.clone(), name.get()), arg)
                    }
                    sqlast::FunctionArg::Unnamed(arg) => {
                        if pos >= fn_type.args.len() {
                            return Err(CompileError::no_such_entry(vec![Ident::with_location(
                                loc.clone(),
                                format!("argument {}", pos),
                            )]));
                        }
                        pos += 1;
                        (
                            Ident::with_location(loc.clone(), fn_type.args[pos - 1].name.clone()),
                            arg,
                        )
                    }
                };

                let expr = match expr {
                    sqlast::FunctionArgExpr::Expr(e) => Cow::Borrowed(e),
                    sqlast::FunctionArgExpr::Wildcard
                    | sqlast::FunctionArgExpr::QualifiedWildcard(_) => {
                        // TODO: This is a really clunky way of checking for wildcards. Ideally we do not need to produce
                        // the count function name each time.
                        //
                        // Wildcards (qualified or not) are only supported for certain functions
                        // (count as far as we know, and potentially others).
                        if func_name.as_slice().first().map(|s| s.get())
                            == Some(Into::<Ident>::into("count")).as_ref()
                        {
                            Cow::Owned(sqlast::Expr::Value(sqlast::Value::Number(
                                "1".to_string(),
                                false,
                            )))
                        } else {
                            return Err(CompileError::unimplemented(
                                loc.clone(),
                                &format!("wildcard arguments for {:?} function", func_name),
                            ));
                        }
                    }
                };

                if compiled_args.get(&name).is_some() {
                    return Err(CompileError::duplicate_entry(vec![name]));
                }

                let compiled_arg =
                    compile_sqlexpr(compiler.clone(), schema.clone(), scope.clone(), loc, &expr)?;
                compiled_args.insert(
                    name.get().clone(),
                    CTypedNameAndExpr {
                        name: name.get().clone(),
                        type_: compiled_arg.type_,
                        expr: compiled_arg.expr,
                    },
                );
            }

            let mut arg_exprs = Vec::new();
            for arg in &fn_type.args {
                if let Some(compiled_arg) = compiled_args.get_mut(&arg.name) {
                    arg.type_.unify(&compiled_arg.type_)?;
                    arg_exprs.push(compiled_arg.clone());
                } else if arg.nullable {
                    // If the argument is missing and nullable, then set it to NULL
                    // as a default value. Eventually we may want to generalize this
                    // so that functions can declare other kinds of default values too.
                    arg_exprs.push(CTypedNameAndExpr {
                        name: arg.name.clone(),
                        type_: NULL.type_.clone(),
                        expr: NULL.expr.clone(),
                    });
                } else {
                    return Err(CompileError::missing_arg(vec![Ident::without_location(
                        arg.name.clone(),
                    )]));
                }
            }

            let type_ = fn_type.ret.clone();

            let expr = compiler.async_cref({
                let compiler = compiler.clone();
                let schema = schema.clone();
                let loc = loc.clone();
                let name = name.clone();
                let type_ = type_.clone();
                async move {
                    let arg_exprs = arg_exprs
                        .into_iter()
                        .map(move |cte| {
                            cte.expr.then(move |expr: Ref<Expr<CRef<MType>>>| {
                                Ok(mkcref(TypedNameAndExpr {
                                    type_: cte.type_.clone(),
                                    name: cte.name.clone(),
                                    expr: Arc::new(expr.read()?.clone()),
                                }))
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let arg_exprs = combine_crefs(arg_exprs)?.await?;

                    let over = cunwrap(over.await?)?;

                    let args = arg_exprs
                        .read()?
                        .iter()
                        .map(|e| Ok(e.read()?.clone()))
                        .collect::<Result<Vec<_>>>()?;

                    if type_.is_known()? {
                        match &*type_
                            .must()
                            .context(RuntimeSnafu { loc: loc.clone() })?
                            .read()?
                        {
                            MType::Generic(generic) => {
                                if let Some(generic) =
                                    as_generic::<ExternalType>(generic.get().as_ref())
                                {
                                    let inner_type = generic.inner_type();

                                    // TODO We should place some metadata on the function, or have a whitelist
                                    // of functions that work this way, but for now, we simply special case the
                                    // load function
                                    if func_name.as_slice()[0].get() != &Into::<Ident>::into("load")
                                    {
                                        return Err(CompileError::unimplemented(
                                            loc.clone(),
                                            "external types for non-load functions",
                                        ));
                                    }

                                    let resolve = schema_infer_load_fn(
                                        schema.clone(),
                                        args.clone(),
                                        inner_type.clone(),
                                    );
                                    compiler.add_external_type(
                                        resolve,
                                        inner_type.clone(),
                                        ExternalTypeRank::Load,
                                    )?;
                                }
                            }
                            _ => {}
                        }
                    }

                    let func_expr = func.expr.unwrap_schema_entry().await?;

                    let (fn_kind, fn_body) = match func_expr.as_ref() {
                        Expr::NativeFn(_) => (FnKind::Native, None),
                        Expr::Fn(FnExpr { body, .. }) => match body {
                            FnBody::SQLBuiltin => (FnKind::SQLBuiltin, None),
                            FnBody::Expr(expr) => (FnKind::Expr, Some(expr.clone())),
                        },
                        _ => {
                            return Err(CompileError::internal(
                                loc.clone(),
                                "Function value must be function expression",
                            ))
                        }
                    };

                    // Function calls against native functions that do not reference any unbound
                    // SQL names in their arguments can be lifted out of the SQL body, which is
                    // important because we don't yet support running native functions within SQL.
                    // It will always be necessary to some extent, because some native functions
                    // can't be pushed down either because of their types or because they must be
                    // run locally (e.g. `load`).
                    //
                    let can_lift = !args.iter().any(|a| has_unbound_names(a.expr.clone()))
                        || !over.names.unbound.is_empty();
                    let should_lift = if compiler.allow_inlining()? {
                        matches!(fn_kind, FnKind::Native)
                    } else {
                        !matches!(fn_kind, FnKind::SQLBuiltin)
                    };
                    let lift = can_lift && should_lift;

                    if lift {
                        let args = args
                            .iter()
                            .map(TypedNameAndExpr::to_typed_expr)
                            .collect::<Vec<_>>();
                        Ok(mkcref(Expr::FnCall(FnCallExpr {
                            func: Arc::new(TypedExpr {
                                type_: mkcref(MType::Fn(fn_type.clone())),
                                expr: func.expr.clone(),
                            }),
                            args,
                            ctx_folder: schema.read()?.folder.clone(),
                        })))
                    } else {
                        match (&fn_kind, fn_body) {
                            // If the function body is an expression, inline it.
                            //
                            (FnKind::Expr, Some(fn_body)) if compiler.allow_inlining()? => {
                                // Within a function body, arguments are represented as
                                // Expr::ContextRef with the given name.  This first pass will
                                // replace any context references with the actual argument bodies,
                                // but leave those expressions in place.  This means the function
                                // aguments will be embedded within the params of the expression
                                // temporarily, even though they may contain free SQL variables.
                                //
                                let fn_body = inline_context(
                                    fn_body,
                                    args.iter()
                                        .map(|a| (a.name.clone(), a.expr.clone()))
                                        .collect(),
                                )
                                .await?;
                                // Next, eagerly inline any parameters with SQL definitions into
                                // the SQL of the function body.  This should result in a version
                                // of the body with all SQL arguments fully inlined.
                                //
                                let fn_body = inline_params(fn_body).await?;

                                Ok(mkcref(fn_body.as_ref().clone()))
                            }
                            // Otherwise, create a SQL function call.
                            //
                            _ => {
                                let mut names = CSQLNames::new();
                                let mut args = Vec::new();
                                for arg in &*arg_exprs.read()? {
                                    let sql = intern_placeholder(
                                        compiler.clone(),
                                        "arg",
                                        &arg.read()?.to_typed_expr(),
                                    )?;
                                    args.push(sqlast::FunctionArg::Named {
                                        name: Ident::without_location(arg.read()?.name.clone())
                                            .to_sqlident(),
                                        arg: sqlast::FunctionArgExpr::Expr(sql.body.as_expr()),
                                    });
                                    names.extend(sql.names.clone());
                                }

                                let name = match fn_kind {
                                    FnKind::SQLBuiltin => name,
                                    _ => {
                                        // Note that this branch will only be matched in the case
                                        // of a native function that couldn't be lifted (i.e.
                                        // because an argument referenced a SQL name).  It will
                                        // attempt to provide the function value as a parameter to
                                        // the SQL, which will fail in the runtime code until we
                                        // implement UDFs.
                                        //
                                        let (func_name, func) = intern_nonsql_placeholder(
                                            compiler.clone(),
                                            "func",
                                            &func,
                                        )?;
                                        names.extend(func.names.clone());
                                        sqlast::ObjectName(vec![func_name])
                                    }
                                };
                                Ok(mkcref(Expr::SQL(Arc::new(SQL {
                                    names,
                                    body: SQLBody::Expr(sqlast::Expr::Function(sqlast::Function {
                                        name,
                                        args,
                                        over: over.body,
                                        distinct,
                                        special,
                                    })),
                                }))))
                            }
                        }
                    }
                }
            })?;

            CTypedExpr { type_, expr }
        }
        sqlast::Expr::Tuple(fields) => {
            let c_fields = fields
                .iter()
                .map(|f| compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, f))
                .collect::<Result<Vec<_>>>()?;

            let c_types = c_fields.iter().map(|f| f.type_.clone()).collect::<Vec<_>>();
            let c_exprs =
                combine_crefs(c_fields.iter().map(|f| f.sql.clone()).collect::<Vec<_>>())?;

            CTypedExpr {
                // NOTE: Postgres turns tuples into records whose fields are named f1, f2, ...,
                // whereas DuckDB creates records into fields named v1, v2 ,... We pick Postgres
                // semantics here, but may need to specify this based on the target dialect.
                type_: mkcref(MType::Record(Located::new(
                    c_types
                        .iter()
                        .enumerate()
                        .map(|(i, t)| {
                            Ok(MField {
                                name: format!("f{}", i + 1).into(),
                                type_: t.clone(),
                                nullable: true,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    SourceLocation::Unknown,
                ))),
                expr: compiler.async_cref(async move {
                    let exprs = c_exprs.await?;
                    let mut names = CSQLNames::new();

                    let mut ret = Vec::new();
                    for expr in &*exprs.read()? {
                        let expr = expr.read()?;
                        names.extend(expr.names.clone());
                        ret.push(expr.body.as_expr());
                    }

                    Ok(mkcref(Expr::SQL(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::Tuple(ret)),
                    }))))
                })?,
            }
        }
        sqlast::Expr::ArrayAgg(sqlast::ArrayAgg {
            distinct,
            expr,
            order_by,
            limit,
            within_group,
        }) => {
            let compiled_expr = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                expr.as_ref(),
            )?;

            let ob = order_by.compile_sql(&compiler, &schema, &scope, loc)?;

            let limit = match limit {
                Some(l) => Some(compile_sqlarg(
                    compiler.clone(),
                    schema.clone(),
                    scope.clone(),
                    loc,
                    l,
                )?),
                None => None,
            };

            let distinct = *distinct;
            let within_group = *within_group;

            CTypedExpr {
                type_: mkcref(MType::List(Located::new(
                    compiled_expr.type_.clone(),
                    loc.clone(),
                ))),
                expr: compiler.async_cref(async move {
                    let mut names = CSQLNames::new();
                    let expr = compiled_expr.sql.await?;

                    let ob = cunwrap(ob.await?)?;

                    let limit = match limit {
                        Some(limit) => Some(limit.sql.await?),
                        None => None,
                    };

                    let expr = expr.read()?;
                    names.extend(expr.names.clone());

                    names.extend(ob.names);

                    let limit = match limit {
                        Some(limit) => {
                            let limit = limit.read()?;
                            names.extend(limit.names.clone());
                            Some(Box::new(limit.body.as_expr()))
                        }
                        None => None,
                    };

                    Ok(mkcref(Expr::SQL(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::ArrayAgg(sqlast::ArrayAgg {
                            distinct,
                            expr: Box::new(expr.body.as_expr()),
                            order_by: ob.body,
                            limit,
                            within_group,
                        })),
                    }))))
                })?,
            }
        }
        sqlast::Expr::Subquery(query) => {
            let (_scope, type_, subquery) = compile_sqlquery(
                compiler.clone(),
                schema.clone(),
                Some(scope.clone()),
                loc,
                query.as_ref(),
            )?;

            let loc = loc.clone();
            let type_ = compiler.async_cref(async move {
                let query_type = type_.await?;
                let query_type = query_type.read()?.clone();

                let first_field = extract_scalar_subselect_field(&loc, &query_type).await?;
                Ok(first_field.type_)
            })?;

            let expr = compiler.async_cref(async {
                let inner_query = cunwrap(subquery.await?)?;
                Ok(mkcref(Expr::SQL(Arc::new(SQL {
                    names: inner_query.names.clone(),
                    body: SQLBody::Expr(sqlast::Expr::Subquery(Box::new(inner_query.body))),
                }))))
            })?;

            CTypedExpr { type_, expr }
        }
        sqlast::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let expr = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                expr.as_ref(),
            )?;
            let (_scope, subquery_type, subquery) = compile_sqlquery(
                compiler.clone(),
                schema.clone(),
                Some(scope.clone()),
                loc,
                subquery.as_ref(),
            )?;

            compiler.async_cref({
                let loc = loc.clone();
                async move {
                    let subquery_type = subquery_type.await?;
                    let subquery_type = subquery_type.read()?.clone();

                    let first_field = extract_scalar_subselect_field(&loc, &subquery_type).await?;
                    first_field.type_.unify(&expr.type_)?;

                    Ok(mkcref(()))
                }
            })?;

            let negated = *negated;

            CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "bool")?,
                expr: compiler.async_cref(async move {
                    let expr = expr.sql.clone().await?;
                    let subquery = cunwrap(subquery.await?)?;
                    let expr = expr.read()?;

                    let mut names = CSQLNames::new();
                    names.extend(expr.names.clone());
                    names.extend(subquery.names);

                    Ok(mkcref(Expr::SQL(Arc::new(SQL {
                        names: names,
                        body: SQLBody::Expr(sqlast::Expr::InSubquery {
                            expr: Box::new(expr.body.as_expr()),
                            subquery: Box::new(subquery.body),
                            negated,
                        }),
                    }))))
                })?,
            }
        }
        sqlast::Expr::CompoundIdentifier(sqlpath) => {
            compile_sqlreference(compiler.clone(), schema.clone(), scope.clone(), sqlpath)?
        }
        sqlast::Expr::Identifier(ident) => compile_sqlreference(
            compiler.clone(),
            schema.clone(),
            scope.clone(),
            &vec![ident.clone()],
        )?,
        sqlast::Expr::Nested(expr) => compile_sqlexpr(
            compiler.clone(),
            schema.clone(),
            scope.clone(),
            loc,
            expr.as_ref(),
        )?,
        _ => {
            return Err(CompileError::unimplemented(
                loc.clone(),
                format!("Expression: {:?}", expr).as_str(),
            ))
        }
    };

    Ok(ret)
}
