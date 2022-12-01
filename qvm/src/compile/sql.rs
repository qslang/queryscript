use lazy_static::lazy_static;
use sqlparser::{ast as sqlast, ast::DataType as ParserDataType};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::compile::compile::{coerce, lookup_path, resolve_global_atom, typecheck_path, Compiler};
use crate::compile::error::{CompileError, Result};
use crate::compile::inference::*;
use crate::compile::schema::*;
use crate::compile::util::InsertionOrderMap;
use crate::types::{number::parse_numeric_type, AtomicType};

const QVM_NAMESPACE: &str = "__qvm";

#[derive(Clone, Debug)]
pub struct TypedSQL {
    pub type_: CRef<MType>,
    pub sql: Ref<SQL<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct CTypedNameAndSQL {
    pub name: String,
    pub type_: CRef<MType>,
    pub sql: CRef<SQL<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct NameAndSQL {
    pub name: String,
    pub sql: Arc<SQL<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct CTypedSQL {
    pub type_: CRef<MType>,
    pub sql: CRef<SQL<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct CTypedSQLQuery {
    pub type_: CRef<MType>,
    pub query: CRef<SQLQuery<CRef<MType>>>,
}

impl<Ty: Clone + fmt::Debug + Send + Sync> Constrainable for SQL<Ty> {}
impl<Ty: Clone + fmt::Debug + Send + Sync> Constrainable for SQLQuery<Ty> {}
impl Constrainable for TypedSQL {}
impl Constrainable for NameAndSQL {}
impl Constrainable for CTypedNameAndSQL {}
impl Constrainable for CTypedSQL {}
impl Constrainable for CTypedSQLQuery {}
impl Constrainable for CTypedExpr {}

pub fn get_rowtype(compiler: Compiler, relation: CRef<MType>) -> Result<CRef<MType>> {
    Ok(compiler.async_cref(async move {
        let r = &relation;
        let reltype = r.await?;
        let locked = reltype.read()?;
        match &*locked {
            MType::List(inner) => Ok(inner.clone()),
            _ => Ok(relation.clone()),
        }
    })?)
}

pub fn compile_sqlreference(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    sqlpath: &Vec<sqlast::Ident>,
) -> Result<CTypedExpr> {
    let path: Vec<_> = sqlpath.iter().map(|e| e.value.clone()).collect();
    match sqlpath.len() {
        0 => {
            return Err(CompileError::internal(
                "Reference must have at least one part",
            ));
        }
        1 => {
            let name = sqlpath[0].value.clone();

            if let Some(relation) = scope.read()?.relations.get(&name) {
                let type_ = get_rowtype(compiler.clone(), relation.type_.clone())?;
                let expr = mkcref(Expr::SQL(Arc::new(SQL {
                    params: BTreeMap::new(),
                    body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(sqlpath.clone())),
                })));
                return Ok(CTypedExpr { type_, expr });
            } else {
                let available = scope.read()?.get_available_references(compiler.clone())?;

                let tse = available.then(
                    move |available: Ref<InsertionOrderMap<String, FieldMatch>>| {
                        if let Some(fm) = available.read()?.get(&name) {
                            if let Some(type_) = fm.type_.clone() {
                                Ok(mkcref(TypedExpr {
                                    type_: type_.clone(),
                                    expr: Arc::new(Expr::SQL(Arc::new(SQL {
                                        params: BTreeMap::new(),
                                        body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(
                                            vec![
                                                sqlast::Ident {
                                                    value: fm.relation.clone(),
                                                    quote_style: None,
                                                },
                                                sqlast::Ident {
                                                    value: name.clone(),
                                                    quote_style: None,
                                                },
                                            ],
                                        )),
                                    }))),
                                }))
                            } else {
                                Err(CompileError::duplicate_entry(vec![name.clone()]))
                            }
                        } else {
                            // If it doesn't match any names of fields in SQL relations,
                            // compile it as a normal reference.
                            //
                            let te = compile_reference(compiler.clone(), schema.clone(), &path)?;
                            Ok(mkcref(te))
                        }
                    },
                )?;
                let type_ =
                    tse.then(|tse: Ref<TypedExpr<CRef<MType>>>| Ok(tse.read()?.type_.clone()))?;
                let expr = tse.then(|tse: Ref<TypedExpr<CRef<MType>>>| {
                    Ok(mkcref(tse.read()?.expr.as_ref().clone()))
                })?;

                return Ok(CTypedExpr { type_, expr });
            }
        }
        2 => {
            let relation_name = sqlpath[0].value.clone();
            let field_name = sqlpath[1].value.clone();

            // If the relation can't be found in the scope, just fall through
            //
            if let Some(relation) = scope.read()?.relations.get(&relation_name) {
                let rowtype = get_rowtype(compiler.clone(), relation.type_.clone())?;
                let type_ = typecheck_path(rowtype, vec![field_name].as_slice())?;
                let expr = mkcref(Expr::SQL(Arc::new(SQL {
                    params: BTreeMap::new(),
                    body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(sqlpath.clone())),
                })));
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
    path: &Vec<String>,
) -> Result<TypedExpr<CRef<MType>>> {
    let (_, decl, remainder) = lookup_path(
        compiler.clone(),
        schema.clone(),
        &path,
        true, /* import_global */
        true, /* resolve_last */
    )?;

    let decl = decl.ok_or_else(|| CompileError::no_such_entry(path.clone()))?;
    let entry = decl.value.clone();
    let remainder_cpy = remainder.clone();

    let expr = match &entry {
        SchemaEntry::Expr(v) => v.clone(),
        _ => return Err(CompileError::wrong_kind(path.clone(), "value", &decl)),
    };
    let type_ = expr
        .type_
        .then(|t: Ref<SType>| Ok(t.read()?.instantiate()?))?;
    typecheck_path(type_.clone(), remainder_cpy.as_slice())?;

    let top_level_ref = TypedExpr {
        type_: type_.clone(),
        expr: Arc::new(Expr::SchemaEntry(expr.clone())),
    };

    let r = match remainder.len() {
        0 => top_level_ref,
        _ => {
            // Turn the top level reference into a SQL placeholder, and return
            // a path accessing it
            let placeholder = intern_placeholder(compiler.clone(), "param", &top_level_ref)?;
            let placeholder_name = match &placeholder.body {
                SQLBody::Expr(sqlast::Expr::Identifier(i)) => i,
                _ => panic!("placeholder expected to be an identifier"),
            };
            let mut full_name = vec![placeholder_name.clone()];
            full_name.extend(remainder.clone().into_iter().map(|n| sqlast::Ident {
                value: n,
                quote_style: None,
            }));

            let expr = Arc::new(Expr::SQL(Arc::new(SQL {
                params: placeholder.params.clone(),
                body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(full_name)),
            })));

            TypedExpr { type_, expr }
        }
    };

    Ok(r)
}

pub fn intern_placeholder(
    compiler: Compiler,
    kind: &str,
    expr: &TypedExpr<CRef<MType>>,
) -> Result<Arc<SQL<CRef<MType>>>> {
    match &*expr.expr {
        Expr::SQL(sqlexpr) => Ok(sqlexpr.clone()),
        _ => {
            let placeholder_name = "@".to_string() + compiler.next_placeholder(kind)?.as_str();

            Ok(Arc::new(SQL {
                params: Params::from([(placeholder_name.clone(), expr.clone())]),
                body: SQLBody::Expr(sqlast::Expr::Identifier(sqlast::Ident {
                    value: placeholder_name.clone(),
                    quote_style: None,
                })),
            }))
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
    expr: &sqlast::Expr,
) -> Result<CTypedSQL> {
    let compiled = compile_sqlexpr(compiler.clone(), schema.clone(), scope.clone(), expr)?;
    intern_cref_placeholder(compiler.clone(), "param".to_string(), compiled)
}

type CParams = Params<CRef<MType>>;

pub fn combine_crefs<T: 'static + Constrainable>(all: Vec<CRef<T>>) -> Result<CRef<Vec<Ref<T>>>> {
    let mut ret = mkcref(Vec::new());

    for a in all {
        ret = ret.then(move |sofar: Ref<Vec<Ref<T>>>| {
            a.then(move |a: Ref<T>| Ok(mkcref(vec![sofar.read()?.clone(), vec![a]].concat())))
        })?;
    }

    Ok(ret)
}

pub fn combine_sqlparams(all: &Vec<Ref<SQL<CRef<MType>>>>) -> Result<CParams> {
    let mut ret = BTreeMap::new();
    for e in all {
        insert_sqlparams(&mut ret, &*e.read()?)?;
    }
    Ok(ret)
}

pub fn insert_sqlparams(dest: &mut CParams, src: &SQL<CRef<MType>>) -> Result<()> {
    for (n, p) in &src.params {
        dest.insert(n.clone(), p.clone());
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct FieldMatch {
    pub relation: String,
    pub field: String,
    pub type_: Option<CRef<MType>>,
}
impl Constrainable for FieldMatch {}

#[derive(Clone, Debug)]
pub struct SQLScope {
    pub parent: Option<Arc<SQLScope>>,
    pub relations: BTreeMap<String, TypedExpr<CRef<MType>>>,
    pub multiple_rows: bool,
}

impl SQLScope {
    pub fn new(parent: Option<Arc<SQLScope>>) -> SQLScope {
        SQLScope {
            parent,
            relations: BTreeMap::new(),
            multiple_rows: true,
        }
    }

    pub fn empty() -> Ref<SQLScope> {
        mkref(Self::new(None))
    }

    pub fn get_available_references(
        &self,
        compiler: Compiler,
    ) -> Result<CRef<InsertionOrderMap<String, FieldMatch>>> {
        combine_crefs(
            self.relations
                .iter()
                .map(|(n, te)| {
                    let n = n.clone();
                    get_rowtype(compiler.clone(), te.type_.clone())?.then(
                        move |rowtype: Ref<MType>| {
                            let rowtype = rowtype.read()?.clone();
                            match &rowtype {
                                MType::Record(fields) => Ok(mkcref(
                                    fields
                                        .iter()
                                        .map(|field| FieldMatch {
                                            relation: n.clone(),
                                            field: field.name.clone(),
                                            type_: Some(field.type_.clone()),
                                        })
                                        .collect(),
                                )),
                                _ => Ok(mkcref(vec![FieldMatch {
                                    relation: n.clone(),
                                    field: n.clone(),
                                    type_: Some(mkcref(rowtype)),
                                }])),
                            }
                        },
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        )?
        .then(|relations: Ref<Vec<Ref<Vec<FieldMatch>>>>| {
            let mut ret = InsertionOrderMap::<String, FieldMatch>::new();
            for a in &*relations.read()? {
                for b in &*a.read()? {
                    if let Some(existing) = ret.get_mut(&b.field) {
                        existing.type_ = None;
                    } else {
                        ret.insert(b.field.clone(), b.clone());
                    }
                }
            }
            Ok(mkcref(ret))
        })
    }
}

impl Constrainable for SQLScope {}

pub fn compile_select(
    compiler: Compiler,
    schema: Ref<Schema>,
    select: &sqlast::Select,
) -> Result<(
    CRef<MType>,
    CRef<CWrap<(Params<CRef<MType>>, Box<sqlast::SetExpr>)>>,
)> {
    if select.distinct {
        return Err(CompileError::unimplemented("DISTINCT"));
    }

    if select.top.is_some() {
        return Err(CompileError::unimplemented("TOP"));
    }

    if select.into.is_some() {
        return Err(CompileError::unimplemented("INTO"));
    }

    if select.lateral_views.len() > 0 {
        return Err(CompileError::unimplemented("Lateral views"));
    }

    if select.cluster_by.len() > 0 {
        return Err(CompileError::unimplemented("CLUSTER BY"));
    }

    if select.distribute_by.len() > 0 {
        return Err(CompileError::unimplemented("DISTRIBUTE BY"));
    }

    if select.sort_by.len() > 0 {
        return Err(CompileError::unimplemented("ORDER BY"));
    }

    if select.having.is_some() {
        return Err(CompileError::unimplemented("HAVING"));
    }

    if select.qualify.is_some() {
        return Err(CompileError::unimplemented("QUALIFY"));
    }

    let scope = SQLScope::empty();
    match select.from.len() {
        0 => {}
        1 => {
            if select.from[0].joins.len() > 0 {
                return Err(CompileError::unimplemented("JOIN"));
            }

            match &select.from[0].relation {
                sqlast::TableFactor::Table {
                    name,
                    alias,
                    args,
                    with_hints,
                } => {
                    if args.is_some() {
                        return Err(CompileError::unimplemented("Table valued functions"));
                    }

                    if with_hints.len() > 0 {
                        return Err(CompileError::unimplemented("WITH hints"));
                    }

                    // TODO: This currently assumes that table references always come from outside
                    // the query, which is not actually the case.
                    //
                    let path: Vec<_> = name.0.iter().map(|e| e.value.clone()).collect();
                    let relation = compile_reference(compiler.clone(), schema.clone(), &path)?;

                    let list_type = mkcref(MType::List(MType::new_unknown(
                        format!("FROM {}", name.to_string()).as_str(),
                    )));
                    list_type.unify(&relation.type_)?;

                    let name = match alias {
                        Some(a) => a.name.value.clone(),
                        None => name
                            .0
                            .last()
                            .ok_or_else(|| {
                                CompileError::internal("Table name must have at least one part")
                            })?
                            .value
                            .clone(),
                    };
                    scope.write()?.relations.insert(name, relation);
                }
                sqlast::TableFactor::Derived { .. } => {
                    return Err(CompileError::unimplemented("Subqueries"))
                }
                sqlast::TableFactor::TableFunction { .. } => {
                    return Err(CompileError::unimplemented("TABLE"))
                }
                sqlast::TableFactor::UNNEST { .. } => {
                    return Err(CompileError::unimplemented("UNNEST"))
                }
                sqlast::TableFactor::NestedJoin { .. } => {
                    return Err(CompileError::unimplemented("JOIN"))
                }
            }
        }
        _ => return Err(CompileError::unimplemented("JOIN")),
    };

    let mut from = Vec::new();
    let mut from_params = BTreeMap::new();
    for (name, relation) in &scope.read()?.relations {
        let placeholder_name =
            QVM_NAMESPACE.to_string() + compiler.next_placeholder("rel")?.as_str();

        from.push(sqlast::TableWithJoins {
            relation: sqlast::TableFactor::Table {
                name: sqlast::ObjectName(vec![sqlast::Ident {
                    value: placeholder_name.clone(),
                    quote_style: None,
                }]),
                alias: Some(sqlast::TableAlias {
                    name: sqlast::Ident {
                        value: name.clone(),
                        quote_style: None,
                    },
                    columns: Vec::new(),
                }),
                args: None,
                with_hints: Vec::new(),
            },
            joins: Vec::new(),
        });
        from_params.insert(placeholder_name.clone(), relation.clone());
    }

    let exprs = select
        .projection
        .iter()
        .map(|p| {
            Ok(match p {
                sqlast::SelectItem::UnnamedExpr(expr) => {
                    let name = format!("{}", expr);
                    let compiled =
                        compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), expr)?;
                    mkcref(vec![CTypedNameAndSQL {
                        name,
                        type_: compiled.type_,
                        sql: compiled.sql,
                    }])
                }
                sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                    let name = alias.value.clone();
                    let compiled =
                        compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), expr)?;
                    mkcref(vec![CTypedNameAndSQL {
                        name,
                        type_: compiled.type_,
                        sql: compiled.sql,
                    }])
                }
                sqlast::SelectItem::Wildcard => {
                    let available = scope.read()?.get_available_references(compiler.clone())?;
                    available.then(|available: Ref<InsertionOrderMap<String, FieldMatch>>| {
                        let mut ret = Vec::new();
                        for (_, m) in available.read()?.iter() {
                            let type_ = match &m.type_ {
                                Some(t) => t.clone(),
                                None => {
                                    return Err(CompileError::duplicate_entry(vec![m
                                        .field
                                        .clone()]))
                                }
                            };
                            ret.push(CTypedNameAndSQL {
                                name: m.field.clone(),
                                type_,
                                sql: mkcref(SQL {
                                    params: BTreeMap::new(),
                                    body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(vec![
                                        sqlast::Ident {
                                            value: m.relation.clone(),
                                            quote_style: None,
                                        },
                                        sqlast::Ident {
                                            value: m.field.clone(),
                                            quote_style: None,
                                        },
                                    ])),
                                }),
                            });
                        }
                        Ok(mkcref(ret))
                    })?
                }
                sqlast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(CompileError::unimplemented("Table wildcard"));
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let projections = combine_crefs(exprs)?;

    let type_: CRef<MType> = projections.then(|exprs: Ref<Vec<Ref<Vec<CTypedNameAndSQL>>>>| {
        let mut fields = Vec::new();
        for a in &*exprs.read()? {
            for b in &*a.read()? {
                fields.push(MField {
                    name: b.name.clone(),
                    type_: b.type_.clone(),
                    nullable: true,
                });
            }
        }

        Ok(mkcref(MType::List(mkcref(MType::Record(fields)))))
    })?;

    let select = select.clone();
    let expr: CRef<_> = compiler.clone().async_cref(async move {
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
        let from = from.clone();
        let from_params = from_params.clone();
        let mut params = BTreeMap::new();
        let mut projection = Vec::new();
        for sqlexpr in &*proj_exprs {
            insert_sqlparams(&mut params, sqlexpr.sql.as_ref())?;
            projection.push(sqlast::SelectItem::ExprWithAlias {
                alias: sqlast::Ident {
                    value: sqlexpr.name.clone(),
                    quote_style: None,
                },
                expr: sqlexpr.sql.body.as_expr()?,
            });
        }
        for (n, p) in &from_params {
            params.insert(n.clone(), p.clone());
        }

        let selection = match &select.selection {
            Some(selection) => {
                let compiled =
                    compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), selection)?;
                compiled
                    .type_
                    .unify(&resolve_global_atom(compiler.clone(), "bool")?)?;
                let sql = compiled.sql.await?.read()?.clone();
                insert_sqlparams(&mut params, &sql)?;
                Some(sql.body.as_expr()?)
            }
            None => None,
        };

        let mut group_by = Vec::new();
        for gb in &select.group_by {
            let compiled = compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), gb)?;
            let sql = compiled.sql.await?.read()?.clone();
            insert_sqlparams(&mut params, &sql)?;
            group_by.push(sql.body.as_expr()?);
        }

        let mut ret = select.clone();
        ret.from = from.clone();
        ret.projection = projection;
        ret.selection = selection;
        ret.group_by = group_by;

        Ok(cwrap((
            params,
            Box::new(sqlast::SetExpr::Select(Box::new(ret.clone()))),
        )))
    })?;

    Ok((type_, expr))
}

pub async fn finish_sqlexpr(
    expr: CRef<Expr<CRef<MType>>>,
    params: &mut Params<CRef<MType>>,
) -> Result<sqlast::Expr> {
    let expr = expr.clone_inner().await?;
    Ok(match expr {
        Expr::SQL(s) => {
            params.extend(s.params.clone().into_iter());
            s.body.as_expr()?.clone()
        }
        _ => return Err(CompileError::unimplemented("Non-SQL expression")),
    })
}

pub fn compile_sqlquery(
    compiler: Compiler,
    schema: Ref<Schema>,
    query: &sqlast::Query,
) -> Result<CTypedExpr> {
    if query.with.is_some() {
        return Err(CompileError::unimplemented("WITH"));
    }

    if query.order_by.len() > 0 {
        return Err(CompileError::unimplemented("ORDER BY"));
    }

    let limit = match &query.limit {
        Some(limit) => {
            let expr =
                compile_sqlexpr(compiler.clone(), schema.clone(), SQLScope::empty(), &limit)?;
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
                &offset.value,
            )?;
            expr.type_
                .unify(&resolve_global_atom(compiler.clone(), "bigint")?)?;

            Some((expr, offset.rows.clone()))
        }
        None => None,
    };

    if query.fetch.is_some() {
        return Err(CompileError::unimplemented("FETCH"));
    }

    if query.lock.is_some() {
        return Err(CompileError::unimplemented("FOR { UPDATE | SHARE }"));
    }

    match query.body.as_ref() {
        sqlast::SetExpr::Select(s) => {
            let (type_, select) = compile_select(compiler.clone(), schema.clone(), s)?;
            Ok(CTypedExpr {
                type_,
                expr: compiler.async_cref(async move {
                    let (mut params, body) = cunwrap(select.await?)?;
                    let limit = match limit {
                        Some(limit) => Some(finish_sqlexpr(limit.expr, &mut params).await?),
                        None => None,
                    };

                    let offset = match offset {
                        Some((offset, rows)) => Some(sqlparser::ast::Offset {
                            value: finish_sqlexpr(offset.expr, &mut params).await?,
                            rows,
                        }),
                        None => None,
                    };

                    Ok(mkcref(Expr::SQLQuery(Arc::new(SQLQuery {
                        params,
                        query: sqlast::Query {
                            with: None,
                            body,
                            order_by: Vec::new(),
                            limit,
                            offset,
                            fetch: None,
                            lock: None,
                        },
                    }))))
                })?,
            })
        }
        sqlast::SetExpr::Query(q) => compile_sqlquery(compiler.clone(), schema.clone(), q),
        sqlast::SetExpr::SetOperation { .. } => {
            Err(CompileError::unimplemented("UNION | EXCEPT | INTERSECT"))
        }
        sqlast::SetExpr::Values(_) => Err(CompileError::unimplemented("VALUES")),
        sqlast::SetExpr::Insert(_) => Err(CompileError::unimplemented("INSERT")),
    }
}

lazy_static! {
    static ref GLOBAL_COMPILER: Compiler = Compiler::new().unwrap();
    static ref NULL_SQLEXPR: Arc<SQL<CRef<MType>>> = Arc::new(SQL {
        params: BTreeMap::new(),
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
    let dt: ParserDataType = (&target_type.to_runtime_type()?).try_into()?;

    Ok(compiler.async_cref(async move {
        let final_expr = sql.clone_inner().await?;
        Ok(mkcref(SQL {
            params: final_expr.params,
            body: SQLBody::Expr(sqlast::Expr::Cast {
                expr: Box::new(final_expr.body.as_expr()?),
                data_type: dt,
            }),
        }))
    })?)
}

// NOTE: This is intentionally written to support a length > 2, but  currently constrained to 2
// elements because of how coerce() is implemented.
fn apply_coerce_casts(
    compiler: Compiler,
    mut args: [CTypedSQL; 2],
    targets: CRef<CWrap<[Option<CRef<MType>>; 2]>>,
) -> Result<[CTypedSQL; 2]> {
    for i in 0..args.len() {
        let targets1 = targets.clone();
        let targets2 = targets.clone();
        let compiler2 = compiler.clone();

        let arg = args[i].clone();

        args[i] = CTypedSQL {
            type_: compiler.clone().async_cref(async move {
                let resolved_targets = CWrap::clone_inner(&targets1).await?;

                Ok(match &resolved_targets[i] {
                    Some(cast) => cast.clone(),
                    None => arg.type_.clone(),
                })
            })?,
            sql: compiler.clone().async_cref(async move {
                let resolved_targets = CWrap::clone_inner(&targets2).await?;

                Ok(match &resolved_targets[i] {
                    Some(cast) => apply_sqlcast(compiler2, arg.sql.clone(), cast.await?)?,
                    None => arg.sql,
                })
            })?,
        };
    }

    Ok(args)
}

pub fn compile_sqlexpr(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    expr: &sqlast::Expr,
) -> Result<CTypedExpr> {
    let ret = match expr {
        sqlast::Expr::Value(v) => match v {
            sqlast::Value::Number(n, _) => CTypedExpr {
                type_: mkcref(MType::Atom(parse_numeric_type(n)?)),
                expr: mkcref(Expr::SQL(Arc::new(SQL {
                    params: BTreeMap::new(),
                    body: SQLBody::Expr(expr.clone()),
                }))),
            },
            sqlast::Value::SingleQuotedString(_)
            | sqlast::Value::EscapedStringLiteral(_)
            | sqlast::Value::NationalStringLiteral(_)
            | sqlast::Value::HexStringLiteral(_)
            | sqlast::Value::DoubleQuotedString(_) => CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "string")?,
                expr: mkcref(Expr::SQL(Arc::new(SQL {
                    params: BTreeMap::new(),
                    body: SQLBody::Expr(expr.clone()),
                }))),
            },
            sqlast::Value::Boolean(_) => CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "bool")?,
                expr: mkcref(Expr::SQL(Arc::new(SQL {
                    params: BTreeMap::new(),
                    body: SQLBody::Expr(expr.clone()),
                }))),
            },
            sqlast::Value::Null => NULL.clone(),
            sqlast::Value::Placeholder(_) => {
                return Err(CompileError::unimplemented(
                    format!("SQL Parameter syntax: {}", expr).as_str(),
                ))
            }
        },
        sqlast::Expr::Array(sqlast::Array { elem, .. }) => {
            let c_elems: Vec<CTypedExpr> = elem
                .iter()
                .map(|e| compile_sqlexpr(compiler.clone(), schema.clone(), scope.clone(), e))
                .collect::<Result<Vec<_>>>()?;
            let mut c_elem_iter = c_elems.iter();
            let data_type = if let Some(first) = c_elem_iter.next() {
                for next in c_elem_iter {
                    first.type_.unify(&next.type_)?;
                }
                first.type_.clone()
            } else {
                mkcref(MType::Atom(AtomicType::Null))
            };

            CTypedExpr {
                type_: data_type,
                expr: mkcref(Expr::SQL(Arc::new(SQL {
                    params: BTreeMap::new(),
                    body: SQLBody::Expr(expr.clone()),
                }))),
            }
        }
        sqlast::Expr::IsNotNull(expr) => {
            let compiled = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                expr.as_ref(),
            )?;
            CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "bool")?,
                expr: compiled.sql.then(move |sqlexpr: Ref<SQL<CRef<MType>>>| {
                    Ok(mkcref(Expr::SQL(Arc::new(SQL {
                        params: sqlexpr.read()?.params.clone(),
                        body: SQLBody::Expr(sqlast::Expr::IsNotNull(Box::new(
                            sqlexpr.read()?.body.as_expr()?,
                        ))),
                    }))))
                })?,
            }
        }
        sqlast::Expr::BinaryOp { left, op, right } => {
            let op = op.clone();
            let left = left.clone();
            let right = right.clone();
            let mut cleft = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                left.as_ref(),
            )?;
            let mut cright = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                right.as_ref(),
            )?;
            use sqlast::BinaryOperator::*;
            let type_ = match op {
                Plus | Minus | Multiply | Divide => {
                    let casts = coerce(
                        compiler.clone(),
                        op.clone(),
                        cleft.type_.clone(),
                        cright.type_.clone(),
                    )?;
                    [cleft, cright] = apply_coerce_casts(compiler.clone(), [cleft, cright], casts)?;
                    cleft.type_
                }
                Eq | NotEq | Lt | LtEq | Gt | GtEq => {
                    let casts = coerce(
                        compiler.clone(),
                        op.clone(),
                        cleft.type_.clone(),
                        cright.type_.clone(),
                    )?;
                    [cleft, cright] = apply_coerce_casts(compiler.clone(), [cleft, cright], casts)?;
                    resolve_global_atom(compiler.clone(), "bool")?
                }
                _ => {
                    return Err(CompileError::unimplemented(
                        format!("Binary operator: {}", op).as_str(),
                    ));
                }
            };
            CTypedExpr {
                type_,
                expr: combine_crefs(vec![cleft.sql, cright.sql])?.then(
                    move |args: Ref<Vec<Ref<SQL<CRef<MType>>>>>| {
                        let params = combine_sqlparams(&*args.read()?)?;
                        Ok(mkcref(Expr::SQL(Arc::new(SQL {
                            params,
                            body: SQLBody::Expr(sqlast::Expr::BinaryOp {
                                left: Box::new(args.read()?[0].read()?.body.as_expr()?),
                                op: op.clone(),
                                right: Box::new(args.read()?[1].read()?.body.as_expr()?),
                            }),
                        }))))
                    },
                )?,
            }
        }
        sqlast::Expr::Function(sqlast::Function {
            name,
            args,
            over,
            distinct,
            ..
        }) => {
            if *distinct {
                return Err(CompileError::unimplemented(
                    format!("Function calls with DISTINCT").as_str(),
                ));
            }

            if over.is_some() {
                return Err(CompileError::unimplemented(
                    format!("Function calls with OVER").as_str(),
                ));
            }

            let path: Vec<_> = name.0.iter().map(|e| e.value.clone()).collect();
            let func = compile_reference(compiler.clone(), schema.clone(), &path)?;
            let fn_type = match func.type_.must()?.read()?.clone() {
                MType::Fn(f) => f,
                _ => {
                    return Err(CompileError::wrong_type(
                        &MType::Fn(MFnType {
                            args: Vec::new(),
                            ret: MType::new_unknown("ret"),
                        }),
                        &*func.type_.must()?.read()?,
                    ))
                }
            };
            let mut compiled_args: BTreeMap<String, CTypedNameAndSQL> = BTreeMap::new();
            let mut pos: usize = 0;
            for arg in args {
                let (name, expr) = match arg {
                    sqlast::FunctionArg::Named { name, arg } => (name.value.clone(), arg),
                    sqlast::FunctionArg::Unnamed(arg) => {
                        if pos >= fn_type.args.len() {
                            return Err(CompileError::no_such_entry(vec![format!(
                                "argument {}",
                                pos
                            )]));
                        }
                        pos += 1;
                        (fn_type.args[pos - 1].name.clone(), arg)
                    }
                };

                let expr = match expr {
                    sqlast::FunctionArgExpr::Expr(e) => e,
                    _ => {
                        return Err(CompileError::unimplemented(
                            format!("Weird function arguments").as_str(),
                        ))
                    }
                };

                if compiled_args.get(&name).is_some() {
                    return Err(CompileError::duplicate_entry(vec![name.clone()]));
                }

                let compiled_arg =
                    compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), &expr)?;
                compiled_args.insert(
                    name.clone(),
                    CTypedNameAndSQL {
                        name: name.clone(),
                        type_: compiled_arg.type_.clone(),
                        sql: compiled_arg.sql.clone(),
                    },
                );
            }

            let mut arg_sqlexprs = Vec::new();
            for arg in &fn_type.args {
                if let Some(compiled_arg) = compiled_args.get_mut(&arg.name) {
                    arg.type_.unify(&compiled_arg.type_)?;
                    arg_sqlexprs.push(compiled_arg.clone());
                } else if arg.nullable {
                    // If the argument is missing and nullable, then set it to NULL
                    // as a default value. Eventually we may want to generalize this
                    // so that functions can declare other kinds of default values too.
                    arg_sqlexprs.push(CTypedNameAndSQL {
                        name: arg.name.clone(),
                        type_: NULL.type_.clone(),
                        sql: mkcref(NULL_SQLEXPR.as_ref().clone()),
                    });
                } else {
                    return Err(CompileError::missing_arg(vec![arg.name.clone()]));
                }
            }

            // XXX: Now that we are binding SQL queries, we need to detect whether the arguments
            // contain any references to values within the SQL query and avoid lifting the function
            // expression if they do.
            //
            let type_ = fn_type.ret.clone();
            let is_builtin = match func.expr.as_ref() {
                Expr::SchemaEntry(STypedExpr { expr, .. }) => {
                    expr.is_known()?
                        && matches!(
                            *(expr.must()?.read()?),
                            Expr::Fn(FnExpr {
                                body: FnBody::SQLBuiltin,
                                ..
                            })
                        )
                }
                _ => false,
            };

            let expr = if is_builtin {
                let arg_exprs: Vec<_> = arg_sqlexprs
                    .iter()
                    .map(|e| {
                        let name = Arc::new(e.name.clone());
                        e.sql.then(move |sql: Ref<SQL<CRef<MType>>>| {
                            // This is a bit annoying. It'd be a lot nicer if we didn't
                            // have to clone the name twice.
                            Ok(mkcref(NameAndSQL {
                                name: name.as_ref().clone(),
                                sql: Arc::new(sql.read()?.clone()),
                            }))
                        })
                    })
                    .collect::<Result<_>>()?;

                let name_wrapper = Arc::new(name.clone());
                combine_crefs(arg_exprs)?.then(move |arg_exprs: Ref<Vec<Ref<NameAndSQL>>>| {
                    let mut args = Vec::new();
                    let mut params = BTreeMap::new();
                    for arg in arg_exprs.read()?.iter() {
                        let arg = arg.read()?;
                        args.push(sqlast::FunctionArg::Named {
                            name: sqlast::Ident {
                                value: arg.name.clone(),
                                quote_style: None,
                            },
                            arg: sqlast::FunctionArgExpr::Expr(arg.sql.body.as_expr()?),
                        });
                        params.extend(arg.sql.params.clone());
                    }

                    Ok(mkcref(Expr::SQL(Arc::new(SQL {
                        params,
                        body: SQLBody::Expr(sqlast::Expr::Function(sqlast::Function {
                            name: name_wrapper.as_ref().clone(),
                            args,

                            // TODO: We may want to forward these fields from the
                            // expr (but doing that blindly causes ownership errors)
                            over: None,
                            distinct: false,
                            special: false,
                        })),
                    }))))
                })?
            } else {
                let arg_exprs: Vec<_> = arg_sqlexprs
                    .iter()
                    .map(|e| {
                        let type_ = e.type_.clone();
                        e.sql.then(move |sql: Ref<SQL<CRef<MType>>>| {
                            Ok(mkcref(TypedExpr {
                                type_: type_.clone(),
                                expr: Arc::new(Expr::SQL(Arc::new(sql.read()?.clone()))),
                            }))
                        })
                    })
                    .collect::<Result<_>>()?;
                combine_crefs(arg_exprs)?.then(
                    move |arg_exprs: Ref<Vec<Ref<TypedExpr<CRef<MType>>>>>| {
                        Ok(mkcref(Expr::FnCall(FnCallExpr {
                            func: Arc::new(TypedExpr {
                                type_: mkcref(MType::Fn(fn_type.clone())),
                                expr: func.expr.clone(),
                            }),
                            args: arg_exprs
                                .read()?
                                .iter()
                                .map(|e| Ok(e.read()?.clone()))
                                .collect::<Result<_>>()?,
                            ctx_folder: schema.read()?.folder.clone(),
                        })))
                    },
                )?
            };
            // let args_nonames = arg_sqlexprs
            //     .iter()
            //     .map(|e| CTypedSQL {
            //         type_: e.type_.clone(),
            //         expr: e.expr.clone(),
            //     })
            //     .collect::<Vec<_>>();
            // let mut params = combine_sqlparams(args_nonames.iter().collect())?;

            // let placeholder_name = QVM_NAMESPACE.to_string()
            //     + new_placeholder_name(schema.clone(), "func").as_str();

            // params.insert(
            //     placeholder_name.clone(),
            //     TypedExpr {
            //         expr: func.expr,
            //         type_: fn_type.ret.clone(),
            //     },
            // );

            // Arc::new(Expr::SQL(Arc::new(SQL {
            //     params,
            //     expr: sqlast::Expr::Function(sqlast::Function {
            //         name: sqlast::ObjectName(vec![sqlast::Ident {
            //             value: placeholder_name.clone(),
            //             quote_style: None,
            //         }]),
            //         args: arg_sqlexprs
            //             .iter()
            //             .map(|e| sqlast::FunctionArg::Named {
            //                 name: sqlast::Ident {
            //                     value: e.name.clone(),
            //                     quote_style: None,
            //                 },
            //                 arg: sqlast::FunctionArgExpr::Expr(e.expr.expr.clone()),
            //             })
            //             .collect(),
            //         over: None,
            //         distinct: false,
            //         special: false,
            //     }),
            // })))

            // XXX There are cases here where we could inline eagerly
            //
            CTypedExpr { type_, expr }
        }
        sqlast::Expr::CompoundIdentifier(sqlpath) => {
            // XXX There are cases here where we could inline eagerly
            //
            compile_sqlreference(compiler.clone(), schema.clone(), scope.clone(), sqlpath)?
        }
        sqlast::Expr::Identifier(ident) => {
            // XXX There are cases here where we could inline eagerly
            //
            compile_sqlreference(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                &vec![ident.clone()],
            )?
        }
        _ => {
            return Err(CompileError::unimplemented(
                format!("Expression: {:?}", expr).as_str(),
            ))
        }
    };

    Ok(ret)
}
