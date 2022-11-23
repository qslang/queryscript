use crate::compile::compile::{lookup_path, resolve_global_atom, typecheck_path, Compiler};
use crate::compile::error::{CompileError, Result};
use crate::compile::inference::*;
use crate::compile::schema::*;
use crate::types::{number::parse_numeric_type, AtomicType};
use sqlparser::ast as sqlast;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

const QVM_NAMESPACE: &str = "__qvm";

#[derive(Clone, Debug)]
pub struct TypedSQLExpr {
    pub type_: CRef<MType>,
    pub expr: Ref<SQLExpr<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct CTypedNameAndSQLExpr {
    pub name: String,
    pub type_: CRef<MType>,
    pub expr: CRef<SQLExpr<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct NameAndSQLExpr {
    pub name: String,
    pub expr: Arc<SQLExpr<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct CTypedSQLExpr {
    pub type_: CRef<MType>,
    pub expr: CRef<SQLExpr<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct CTypedSQLQuery {
    pub type_: CRef<MType>,
    pub query: CRef<SQLQuery<CRef<MType>>>,
}

impl<Ty: Clone + fmt::Debug + Send + Sync> Constrainable for SQLExpr<Ty> {}
impl<Ty: Clone + fmt::Debug + Send + Sync> Constrainable for SQLQuery<Ty> {}
impl Constrainable for TypedSQLExpr {}
impl Constrainable for NameAndSQLExpr {}
impl Constrainable for CTypedNameAndSQLExpr {}
impl Constrainable for CTypedSQLExpr {}
impl Constrainable for CTypedSQLQuery {}

pub fn get_rowtype(relation: CRef<MType>) -> Result<CRef<MType>> {
    let r = relation.clone();
    r.then(move |r: Ref<MType>| match &*r.read()? {
        MType::List(inner) => Ok(inner.clone()),
        _ => Ok(relation.clone()),
    })
}

pub fn compile_sqlreference(
    compiler: Ref<Compiler>,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    sqlpath: &Vec<sqlast::Ident>,
) -> Result<CTypedExpr> {
    match sqlpath.len() {
        0 => {
            return Err(CompileError::internal(
                "Reference must have at least one part",
            ));
        }
        1 => {
            let name = sqlpath[0].value.clone();

            if let Some(relation) = scope.read()?.relations.get(&name) {
                let type_ = get_rowtype(relation.type_.clone())?;
                let expr = mkcref(Expr::SQLExpr(Arc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: sqlast::Expr::CompoundIdentifier(sqlpath.clone()),
                })));
                return Ok(CTypedExpr { type_, expr });
            } else {
                let available = scope.read()?.get_available_references()?;

                let sqlpath = sqlpath.clone();
                let tse = available.then(move |available: Ref<BTreeMap<String, FieldMatch>>| {
                    if let Some(fm) = available.read()?.get(&name) {
                        if let Some(type_) = fm.type_.clone() {
                            Ok(mkcref(TypedExpr {
                                type_: type_.clone(),
                                expr: Arc::new(Expr::SQLExpr(Arc::new(SQLExpr {
                                    params: BTreeMap::new(),
                                    expr: sqlast::Expr::CompoundIdentifier(vec![
                                        sqlast::Ident {
                                            value: fm.relation.clone(),
                                            quote_style: None,
                                        },
                                        sqlast::Ident {
                                            value: name.clone(),
                                            quote_style: None,
                                        },
                                    ]),
                                }))),
                            }))
                        } else {
                            Err(CompileError::duplicate_entry(vec![name.clone()]))
                        }
                    } else {
                        // If it doesn't match any names of fields in SQL relations,
                        // compile it as a normal reference.
                        //
                        let te = compile_reference(compiler.clone(), schema.clone(), &sqlpath)?;
                        Ok(mkcref(te))
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
            let relation_name = sqlpath[0].value.clone();
            let field_name = sqlpath[1].value.clone();

            // If the relation can't be found in the scope, just fall through
            //
            if let Some(relation) = scope.read()?.relations.get(&relation_name) {
                let rowtype = get_rowtype(relation.type_.clone())?;
                let type_ = typecheck_path(rowtype, vec![field_name].as_slice())?;
                let expr = mkcref(Expr::SQLExpr(Arc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: sqlast::Expr::CompoundIdentifier(sqlpath.clone()),
                })));
                return Ok(CTypedExpr { type_, expr });
            }
        }
        // References longer than two parts must be pointing outside the query, so just fall
        // through
        //
        _ => {}
    }

    let te = compile_reference(compiler.clone(), schema.clone(), sqlpath)?;
    Ok(CTypedExpr {
        type_: te.type_.clone(),
        expr: mkcref(te.expr.as_ref().clone()),
    })
}

pub fn compile_reference(
    compiler: Ref<Compiler>,
    schema: Ref<Schema>,
    sqlpath: &Vec<sqlast::Ident>,
) -> Result<TypedExpr<CRef<MType>>> {
    let path: Vec<_> = sqlpath.iter().map(|e| e.value.clone()).collect();
    let (decl, remainder) = lookup_path(schema.clone(), &path, true /* import_global */)?;

    let entry = decl.value.clone();
    let remainder_cpy = remainder.clone();

    let type_ = match &entry {
        SchemaEntry::Expr(v) => v.clone(),
        _ => return Err(CompileError::wrong_kind(path.clone(), "value", &decl)),
    }
    .then(move |typed: Ref<STypedExpr>| -> Result<CRef<MType>> {
        let type_ = typed
            .read()?
            .type_
            .then(|t: Ref<SType>| Ok(t.read()?.instantiate()?))?;
        typecheck_path(type_.clone(), remainder_cpy.as_slice())
    })?;

    let top_level_ref = TypedExpr {
        type_: type_.clone(),
        expr: Arc::new(Expr::SchemaEntry(SchemaEntryExpr {
            debug_name: decl.name.clone(),
            entry: entry.clone(),
        })),
    };

    let r = match remainder.len() {
        0 => top_level_ref,
        _ => {
            // Turn the top level reference into a SQL placeholder, and return
            // a path accessing it
            let placeholder = intern_placeholder(compiler.clone(), "param", &top_level_ref)?;
            let placeholder_name = match &placeholder.expr {
                sqlast::Expr::Identifier(i) => i,
                _ => panic!("placeholder expected to be an identifier"),
            };
            let mut full_name = vec![placeholder_name.clone()];
            full_name.extend(remainder.clone().into_iter().map(|n| sqlast::Ident {
                value: n,
                quote_style: None,
            }));

            let expr = Arc::new(Expr::SQLExpr(Arc::new(SQLExpr {
                params: placeholder.params.clone(),
                expr: sqlast::Expr::CompoundIdentifier(full_name),
            })));

            TypedExpr { type_, expr }
        }
    };

    Ok(r)
}

pub fn intern_placeholder(
    compiler: Ref<Compiler>,
    kind: &str,
    expr: &TypedExpr<CRef<MType>>,
) -> Result<Arc<SQLExpr<CRef<MType>>>> {
    match &*expr.expr {
        Expr::SQLExpr(sqlexpr) => Ok(sqlexpr.clone()),
        _ => {
            let placeholder_name =
                "@".to_string() + compiler.write()?.next_placeholder(kind).as_str();

            Ok(Arc::new(SQLExpr {
                params: Params::from([(placeholder_name.clone(), expr.clone())]),
                expr: sqlast::Expr::Identifier(sqlast::Ident {
                    value: placeholder_name.clone(),
                    quote_style: None,
                }),
            }))
        }
    }
}

pub fn intern_cref_placeholder(
    compiler: Ref<Compiler>,
    kind: String,
    te: CTypedExpr,
) -> Result<CTypedSQLExpr> {
    let type_ = te.type_.clone();
    let expr = te.expr.clone().then(move |expr: Ref<Expr<CRef<MType>>>| {
        let te = te.clone();
        let sqlexpr: SQLExpr<CRef<MType>> = intern_placeholder(
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
    Ok(CTypedSQLExpr { type_, expr })
}

pub fn compile_sqlarg(
    compiler: Ref<Compiler>,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    expr: &sqlast::Expr,
) -> Result<CTypedSQLExpr> {
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

pub fn combine_sqlparams(all: &Vec<Ref<SQLExpr<CRef<MType>>>>) -> Result<CParams> {
    let mut ret = BTreeMap::new();
    for e in all {
        for (n, p) in &e.read()?.params {
            ret.insert(n.clone(), p.clone());
        }
    }
    Ok(ret)
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

    pub fn get_available_references(&self) -> Result<CRef<BTreeMap<String, FieldMatch>>> {
        combine_crefs(
            self.relations
                .iter()
                .map(|(n, te)| {
                    let n = n.clone();
                    get_rowtype(te.type_.clone())?.then(move |rowtype: Ref<MType>| {
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
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        )?
        .then(|relations: Ref<Vec<Ref<Vec<FieldMatch>>>>| {
            let mut ret = BTreeMap::<String, FieldMatch>::new();
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
    compiler: Ref<Compiler>,
    schema: Ref<Schema>,
    select: &sqlast::Select,
) -> Result<CTypedExpr> {
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

    if select.selection.is_some() {
        return Err(CompileError::unimplemented("WHERE"));
    }

    if select.group_by.len() > 0 {
        return Err(CompileError::unimplemented("GROUP BY"));
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

    let scope = mkref(SQLScope::new(None));

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
                    let relation = compile_reference(compiler.clone(), schema.clone(), &name.0)?;

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
            QVM_NAMESPACE.to_string() + compiler.write()?.next_placeholder("rel").as_str();

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
                    mkcref(vec![CTypedNameAndSQLExpr {
                        name,
                        type_: compiled.type_,
                        expr: compiled.expr,
                    }])
                }
                sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                    let name = alias.value.clone();
                    let compiled =
                        compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), expr)?;
                    mkcref(vec![CTypedNameAndSQLExpr {
                        name,
                        type_: compiled.type_,
                        expr: compiled.expr,
                    }])
                }
                sqlast::SelectItem::Wildcard => {
                    let available = scope.read()?.get_available_references()?;
                    available.then(|available: Ref<BTreeMap<String, FieldMatch>>| {
                        let mut ret = Vec::new();
                        for (_, m) in &*available.read()? {
                            let type_ = match &m.type_ {
                                Some(t) => t.clone(),
                                None => {
                                    return Err(CompileError::duplicate_entry(vec![m
                                        .field
                                        .clone()]))
                                }
                            };
                            ret.push(CTypedNameAndSQLExpr {
                                name: m.field.clone(),
                                type_,
                                expr: mkcref(SQLExpr {
                                    params: BTreeMap::new(),
                                    expr: sqlast::Expr::CompoundIdentifier(vec![
                                        sqlast::Ident {
                                            value: m.relation.clone(),
                                            quote_style: None,
                                        },
                                        sqlast::Ident {
                                            value: m.field.clone(),
                                            quote_style: None,
                                        },
                                    ]),
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

    let type_: CRef<MType> =
        projections.then(|exprs: Ref<Vec<Ref<Vec<CTypedNameAndSQLExpr>>>>| {
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
    let expr: CRef<Expr<CRef<MType>>> =
        projections.then(move |exprs: Ref<Vec<Ref<Vec<CTypedNameAndSQLExpr>>>>| {
            let mut proj_exprs = Vec::new();
            for a in &*exprs.read()? {
                for b in &*a.read()? {
                    let n = b.name.clone();
                    proj_exprs.push(b.expr.then(move |expr: Ref<SQLExpr<CRef<MType>>>| {
                        Ok(mkcref(NameAndSQLExpr {
                            name: n.clone(),
                            expr: Arc::new(expr.read()?.clone()),
                        }))
                    })?);
                }
            }

            let select = select.clone();
            let from = from.clone();
            let from_params = from_params.clone();
            combine_crefs(proj_exprs)?.then(move |proj_exprs: Ref<Vec<Ref<NameAndSQLExpr>>>| {
                let mut params = BTreeMap::new();
                let mut projection = Vec::new();
                for sqlexpr in &*proj_exprs.read()? {
                    let b = sqlexpr.read()?;
                    let expr = &b.expr;
                    for (n, p) in &expr.params {
                        params.insert(n.clone(), p.clone());
                    }
                    projection.push(sqlast::SelectItem::ExprWithAlias {
                        alias: sqlast::Ident {
                            value: b.name.clone(),
                            quote_style: None,
                        },
                        expr: expr.expr.clone(),
                    });
                }
                for (n, p) in &from_params {
                    params.insert(n.clone(), p.clone());
                }

                let mut ret = select.clone();
                ret.from = from.clone();
                ret.projection = projection;

                Ok(mkcref(Expr::SQLQuery(Arc::new(SQLQuery {
                    params,
                    query: sqlast::Query {
                        with: None,
                        body: Box::new(sqlast::SetExpr::Select(Box::new(ret.clone()))),
                        order_by: Vec::new(),
                        limit: None,
                        offset: None,
                        fetch: None,
                        lock: None,
                    },
                }))))
            })
        })?;

    return Ok(CTypedExpr { type_, expr });
}

pub fn compile_sqlquery(
    compiler: Ref<Compiler>,
    schema: Ref<Schema>,
    query: &sqlast::Query,
) -> Result<CTypedExpr> {
    if query.with.is_some() {
        return Err(CompileError::unimplemented("WITH"));
    }

    if query.order_by.len() > 0 {
        return Err(CompileError::unimplemented("ORDER BY"));
    }

    if query.limit.is_some() {
        return Err(CompileError::unimplemented("LIMIT"));
    }

    if query.offset.is_some() {
        return Err(CompileError::unimplemented("OFFSET"));
    }

    if query.fetch.is_some() {
        return Err(CompileError::unimplemented("FETCH"));
    }

    if query.lock.is_some() {
        return Err(CompileError::unimplemented("FOR { UPDATE | SHARE }"));
    }

    match query.body.as_ref() {
        sqlast::SetExpr::Select(s) => compile_select(compiler.clone(), schema.clone(), s),
        sqlast::SetExpr::Query(q) => compile_sqlquery(compiler.clone(), schema.clone(), q),
        sqlast::SetExpr::SetOperation { .. } => {
            Err(CompileError::unimplemented("UNION | EXCEPT | INTERSECT"))
        }
        sqlast::SetExpr::Values(_) => Err(CompileError::unimplemented("VALUES")),
        sqlast::SetExpr::Insert(_) => Err(CompileError::unimplemented("INSERT")),
    }
}

pub fn compile_sqlexpr(
    compiler: Ref<Compiler>,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    expr: &sqlast::Expr,
) -> Result<CTypedExpr> {
    let ret = match expr {
        sqlast::Expr::Value(v) => match v {
            sqlast::Value::Number(n, _) => CTypedExpr {
                type_: mkcref(MType::Atom(parse_numeric_type(n)?)),
                expr: mkcref(Expr::SQLExpr(Arc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::SingleQuotedString(_)
            | sqlast::Value::EscapedStringLiteral(_)
            | sqlast::Value::NationalStringLiteral(_)
            | sqlast::Value::HexStringLiteral(_)
            | sqlast::Value::DoubleQuotedString(_) => CTypedExpr {
                type_: resolve_global_atom("string")?,
                expr: mkcref(Expr::SQLExpr(Arc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::Boolean(_) => CTypedExpr {
                type_: resolve_global_atom("bool")?,
                expr: mkcref(Expr::SQLExpr(Arc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
            sqlast::Value::Null => CTypedExpr {
                type_: resolve_global_atom("null")?,
                expr: mkcref(Expr::SQLExpr(Arc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            },
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
                expr: mkcref(Expr::SQLExpr(Arc::new(SQLExpr {
                    params: BTreeMap::new(),
                    expr: expr.clone(),
                }))),
            }
        }
        sqlast::Expr::BinaryOp { left, op, right } => {
            let op = op.clone();
            let left = left.clone();
            let right = right.clone();
            let cleft = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                left.as_ref(),
            )?;
            let cright = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                right.as_ref(),
            )?;
            let type_ = match op {
                sqlast::BinaryOperator::Plus => {
                    cleft.type_.unify(&cright.type_)?;
                    cleft.type_
                }
                _ => {
                    return Err(CompileError::unimplemented(
                        format!("Binary operator: {}", op).as_str(),
                    ));
                }
            };
            CTypedExpr {
                type_,
                expr: combine_crefs(vec![cleft.expr, cright.expr])?.then(
                    move |args: Ref<Vec<Ref<SQLExpr<CRef<MType>>>>>| {
                        let params = combine_sqlparams(&*args.read()?)?;
                        Ok(mkcref(Expr::SQLExpr(Arc::new(SQLExpr {
                            params,
                            expr: sqlast::Expr::BinaryOp {
                                left: Box::new(args.read()?[0].read()?.expr.clone()),
                                op: op.clone(),
                                right: Box::new(args.read()?[1].read()?.expr.clone()),
                            },
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

            let func = compile_reference(compiler.clone(), schema.clone(), &name.0)?;
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
            let mut compiled_args: BTreeMap<String, CTypedNameAndSQLExpr> = BTreeMap::new();
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
                    CTypedNameAndSQLExpr {
                        name: name.clone(),
                        type_: compiled_arg.type_.clone(),
                        expr: compiled_arg.expr.clone(),
                    },
                );
            }

            let mut arg_sqlexprs = Vec::new();
            for arg in &fn_type.args {
                if let Some(compiled_arg) = compiled_args.get_mut(&arg.name) {
                    arg.type_.unify(&compiled_arg.type_)?;
                    arg_sqlexprs.push(compiled_arg.clone());
                } else {
                    return Err(CompileError::no_such_entry(vec![arg.name.clone()]));
                }
            }

            // XXX: Now that we are binding SQL queries, we need to detect whether the arguments
            // contain any references to values within the SQL query and avoid lifting the function
            // expression if they do.
            //
            let type_ = fn_type.ret.clone();
            let expr = if true {
                let arg_exprs: Vec<_> = arg_sqlexprs
                    .iter()
                    .map(|e| {
                        let type_ = e.type_.clone();
                        e.expr.then(move |expr: Ref<SQLExpr<CRef<MType>>>| {
                            Ok(mkcref(TypedExpr {
                                type_: type_.clone(),
                                expr: Arc::new(Expr::SQLExpr(Arc::new(expr.read()?.clone()))),
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
                        })))
                    },
                )?
            } else {
                panic!();
                // let args_nonames = arg_sqlexprs
                //     .iter()
                //     .map(|e| CTypedSQLExpr {
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

                // Arc::new(Expr::SQLExpr(Arc::new(SQLExpr {
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
            };

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
