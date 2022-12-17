use lazy_static::lazy_static;
use snafu::prelude::*;
use sqlparser::{ast as sqlast, ast::DataType as ParserDataType};
use std::borrow::Cow;
use std::collections::{btree_map, BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use crate::ast::{SourceLocation, ToPath};
use crate::compile::coerce::CoerceOp;
use crate::compile::compile::{coerce, lookup_path, resolve_global_atom, typecheck_path, Compiler};
use crate::compile::error::*;
use crate::compile::inference::*;
use crate::compile::inline::*;
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
    pub name: Ident,
    pub type_: CRef<MType>,
    pub sql: CRef<SQL<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct NameAndSQL {
    pub name: Ident,
    pub sql: Arc<SQL<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct CTypedSQL {
    pub type_: CRef<MType>,
    pub sql: CRef<SQL<CRef<MType>>>,
}

impl HasCExpr<SQL<CRef<MType>>> for &CTypedSQL {
    fn expr(&self) -> &CRef<SQL<CRef<MType>>> {
        &self.sql
    }
}

impl HasCType<MType> for &CTypedSQL {
    fn type_(&self) -> &CRef<MType> {
        &self.type_
    }
}

impl<Ty: Clone + fmt::Debug + Send + Sync> Constrainable for SQL<Ty> {}
impl Constrainable for TypedSQL {}
impl Constrainable for NameAndSQL {}
impl Constrainable for CTypedNameAndSQL {}
impl Constrainable for CTypedSQL {}
impl Constrainable for CTypedExpr {}

pub fn get_rowtype(compiler: Compiler, relation: CRef<MType>) -> Result<CRef<MType>> {
    Ok(compiler.async_cref(async move {
        let r = &relation;
        let reltype = r.await?;
        let locked = reltype.read()?;
        match &*locked {
            MType::List(MListType { inner, .. }) => Ok(inner.clone()),
            _ => Ok(relation.clone()),
        }
    })?)
}

pub fn ident(value: String) -> sqlast::Ident {
    sqlast::Ident {
        value,
        quote_style: None,
    }
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
        lock: None,
    }
}

pub fn select_no_from(expr: sqlast::Expr) -> sqlast::Query {
    select_from(
        vec![sqlast::SelectItem::ExprWithAlias {
            expr,
            alias: sqlast::Ident {
                value: "value".to_string(),
                quote_style: None,
            },
        }],
        Vec::new(),
    )
}

pub fn select_star_from(relation: sqlast::TableFactor) -> sqlast::Query {
    select_from(
        vec![sqlast::SelectItem::Wildcard],
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
    loc: &SourceLocation,
    sqlpath: &Vec<sqlast::Ident>,
) -> Result<CTypedExpr> {
    let path = sqlpath.to_path(loc);
    match sqlpath.len() {
        0 => {
            return Err(CompileError::internal(
                loc.clone(),
                "Reference must have at least one part",
            ));
        }
        1 => {
            let name = sqlpath[0].value.clone();

            if let Some(relation) = scope.read()?.relations.get(&name) {
                let type_ = get_rowtype(compiler.clone(), relation.type_.clone())?;
                let expr = mkcref(Expr::SQL(Arc::new(SQL {
                    names: CSQLNames::from_unbound(sqlpath),
                    body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(sqlpath.clone())),
                })));
                return Ok(CTypedExpr { type_, expr });
            } else {
                let available = scope
                    .read()?
                    .get_available_references(compiler.clone(), loc)?;

                let tse = available.then({
                    let loc = loc.clone();
                    move |available: Ref<InsertionOrderMap<String, FieldMatch>>| {
                        if let Some(fm) = available.read()?.get(&name) {
                            if let Some(type_) = fm.type_.clone() {
                                let sqlpath =
                                    vec![ident(fm.relation.value.clone()), ident(name.clone())];
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
                                Err(CompileError::duplicate_entry(vec![Ident::with_location(
                                    loc.clone(),
                                    name.clone(),
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
            let relation_name = sqlpath[0].value.clone();
            let field_name = sqlpath[1].value.clone();

            // If the relation can't be found in the scope, just fall through
            //
            if let Some(relation) = scope.read()?.relations.get(&relation_name) {
                let rowtype = get_rowtype(compiler.clone(), relation.type_.clone())?;
                let type_ = typecheck_path(
                    rowtype,
                    vec![Ident::with_location(loc.clone(), field_name)].as_slice(),
                )?;
                let expr = mkcref(Expr::SQL(Arc::new(SQL {
                    names: CSQLNames::from_unbound(&sqlpath),
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
    path: &Vec<Ident>,
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
            let (placeholder_name, placeholder) =
                intern_nonsql_placeholder(compiler.clone(), "param", &top_level_ref)?;
            let mut full_name = vec![placeholder_name.clone()];
            full_name.extend(remainder.clone().into_iter().map(|n| ident(n.value)));

            let expr = Arc::new(Expr::SQL(Arc::new(SQL {
                names: placeholder.names.clone(),
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
) -> Result<(sqlast::Ident, Arc<SQL<CRef<MType>>>)> {
    match &*expr.expr {
        Expr::SQL(_) => Err(CompileError::internal(
            SourceLocation::Unknown,
            "Cannot call intern_nonsql_placeholder on a SQL expression",
        )),
        _ => {
            let placeholder_name = "@".to_string() + compiler.next_placeholder(kind)?.as_str();

            Ok((
                ident(placeholder_name.clone()),
                Arc::new(SQL {
                    names: SQLNames {
                        params: Params::from([(placeholder_name.clone(), expr.clone())]),
                        unbound: BTreeSet::new(),
                    },
                    body: SQLBody::Expr(sqlast::Expr::Identifier(ident(placeholder_name.clone()))),
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

type CSQLNames = SQLNames<CRef<MType>>;

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

#[derive(Clone, Debug)]
pub struct FieldMatch {
    pub relation: Ident,
    pub field: Ident,
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
        loc: &SourceLocation,
    ) -> Result<CRef<InsertionOrderMap<String, FieldMatch>>> {
        combine_crefs(
            self.relations
                .iter()
                .map(|(n, te)| {
                    let n = Ident::with_location(loc.clone(), n.clone());
                    get_rowtype(compiler.clone(), te.type_.clone())?.then(
                        move |rowtype: Ref<MType>| {
                            let rowtype = rowtype.read()?.clone();
                            match &rowtype {
                                MType::Record(MRecordType { fields, .. }) => Ok(mkcref(
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
                    if let Some(existing) = ret.get_mut(&b.field.value) {
                        existing.type_ = None;
                    } else {
                        ret.insert(b.field.value.clone(), b.clone());
                    }
                }
            }
            Ok(mkcref(ret))
        })
    }

    pub fn remove_bound_references(
        &self,
        compiler: Compiler,
        names: CSQLNames,
    ) -> Result<CRef<CSQLNames>> {
        let relations = self.relations.clone();
        compiler.async_cref({
            let compiler = compiler.clone();
            async move {
                let mut names = names.clone();
                for (relation, expr) in &relations {
                    names.unbound.remove(&vec![relation.clone()]);
                    let rowtype = get_rowtype(compiler.clone(), expr.type_.clone())?.await?;
                    match &*rowtype.read()? {
                        MType::Record(MRecordType { fields, .. }) => {
                            for field in fields {
                                names
                                    .unbound
                                    .remove(&vec![relation.clone(), field.name.value.clone()]);
                                names.unbound.remove(&vec![field.name.value.clone()]);
                            }
                        }
                        _ => {}
                    };
                }
                Ok(mkcref(names))
            }
        })
    }
}

impl Constrainable for SQLScope {}

pub fn compile_relation(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    relation: &sqlast::TableFactor,
    from_names: &mut CSQLNames,
) -> Result<sqlast::TableFactor> {
    Ok(match relation {
        sqlast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => {
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
            let relation = compile_reference(compiler.clone(), schema.clone(), &name.to_path(loc))?;

            let list_type = mkcref(MType::List(MListType {
                loc: loc.clone(),
                inner: MType::new_unknown(format!("FROM {}", name.to_string()).as_str()),
            }));
            list_type.unify(&relation.type_)?;

            let name = match alias {
                Some(a) => a.name.value.clone(),
                None => name
                    .0
                    .last()
                    .ok_or_else(|| {
                        CompileError::internal(
                            loc.clone(),
                            "Table name must have at least one part",
                        )
                    })?
                    .value
                    .clone(),
            };
            match scope.write()?.relations.entry(name.clone()) {
                btree_map::Entry::Occupied(_) => {
                    return Err(CompileError::duplicate_entry(vec![Ident::with_location(
                        loc.clone(),
                        name,
                    )]))
                }
                btree_map::Entry::Vacant(e) => {
                    let relation = e.insert(relation).clone();

                    let placeholder_name =
                        QVM_NAMESPACE.to_string() + compiler.next_placeholder("rel")?.as_str();
                    from_names.params.insert(placeholder_name.clone(), relation);

                    sqlast::TableFactor::Table {
                        name: sqlast::ObjectName(vec![ident(placeholder_name)]),
                        alias: Some(sqlast::TableAlias {
                            name: ident(name.to_string()),
                            columns: Vec::new(),
                        }),
                        args: None,
                        with_hints: Vec::new(),
                    }
                }
            }
        }
        sqlast::TableFactor::Derived { .. } => {
            return Err(CompileError::unimplemented(loc.clone(), "Subqueries"))
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

pub fn compile_join_constraint(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    join_constraint: &sqlast::JoinConstraint,
) -> Result<CRef<CWrap<(CSQLNames, sqlast::JoinConstraint)>>> {
    use sqlast::JoinConstraint::*;
    Ok(match join_constraint {
        On(e) => {
            let sql = compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, &e)?;
            sql.type_
                .unify(&resolve_global_atom(compiler.clone(), "bool")?)?;
            compiler.async_cref({
                async move {
                    let sql = sql.sql.await?;
                    let sql = sql.read()?;
                    Ok(cwrap((sql.names.clone(), On(sql.body.as_expr()))))
                }
            })?
        }
        Using(_) => return Err(CompileError::unimplemented(loc.clone(), "JOIN ... USING")),
        Natural => cwrap((CSQLNames::new(), Natural)),
        None => cwrap((CSQLNames::new(), None)),
    })
}

pub fn compile_join_operator(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    join_operator: &sqlast::JoinOperator,
) -> Result<CRef<CWrap<(CSQLNames, sqlast::JoinOperator)>>> {
    use sqlast::JoinOperator::*;
    let join_constructor = match join_operator {
        Inner(_) => Some(Inner),
        LeftOuter(_) => Some(Inner),
        RightOuter(_) => Some(Inner),
        FullOuter(_) => Some(Inner),
        _ => None,
    };

    Ok(match join_operator {
        Inner(c) | LeftOuter(c) | RightOuter(c) | FullOuter(c) => {
            let constraint = compile_join_constraint(compiler, schema, scope, loc, c)?;
            compiler.async_cref(async move {
                let (names, sql) = cunwrap(constraint.await?)?;
                Ok(cwrap((names, join_constructor.unwrap()(sql))))
            })?
        }
        o => {
            return Err(CompileError::unimplemented(
                loc.clone(),
                format!("{:?}", o).as_str(),
            ))
        }
    })
}

pub fn compile_table_with_joins(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    table: &sqlast::TableWithJoins,
) -> Result<CRef<CWrap<(CSQLNames, sqlast::TableWithJoins)>>> {
    let mut table_names = CSQLNames::new();
    let relation = compile_relation(
        compiler,
        schema,
        scope,
        loc,
        &table.relation,
        &mut table_names,
    )?;

    let mut join_rels = Vec::new();
    let mut join_ops = Vec::new();
    for join in &table.joins {
        let join_relation = compile_relation(
            compiler,
            schema,
            scope,
            loc,
            &join.relation,
            &mut table_names,
        )?;

        join_rels.push(join_relation);
        join_ops.push(compile_join_operator(
            compiler,
            schema,
            scope,
            loc,
            &join.join_operator,
        )?);
    }
    compiler.async_cref(async move {
        let mut joins = Vec::new();
        for (jo, relation) in join_ops.into_iter().zip(join_rels.into_iter()) {
            let (names, join_operator) = cunwrap(jo.await?)?;
            table_names.extend(names);
            joins.push(sqlast::Join {
                relation,
                join_operator,
            });
        }
        Ok(cwrap((
            table_names,
            sqlast::TableWithJoins { relation, joins },
        )))
    })
}

pub fn compile_from(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    loc: &SourceLocation,
    from: &Vec<sqlast::TableWithJoins>,
) -> Result<(
    Ref<SQLScope>,
    CRef<CWrap<(CSQLNames, Vec<sqlast::TableWithJoins>)>>,
)> {
    let scope = SQLScope::empty();
    let from = match from.len() {
        0 => cwrap((CSQLNames::new(), Vec::new())),
        _ => {
            let tables = from
                .iter()
                .map(|table| compile_table_with_joins(compiler, schema, &scope, loc, table))
                .collect::<Result<Vec<_>>>()?;

            compiler.async_cref(async move {
                let mut all_names = CSQLNames::new();
                let mut all_tables = Vec::new();

                for table in tables.into_iter() {
                    let (names, table) = cunwrap(table.await?)?;
                    all_names.extend(names);
                    all_tables.push(table);
                }

                Ok(cwrap((all_names, all_tables)))
            })?
        }
    };

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
        compiled_order_by.push(compile_sqlarg(
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
                let resolved_expr = expr.sql.await?;
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

pub fn compile_select(
    compiler: Compiler,
    schema: Ref<Schema>,
    loc: &SourceLocation,
    select: &sqlast::Select,
) -> Result<(
    Ref<SQLScope>,
    CRef<MType>,
    CRef<CWrap<(CSQLNames, Box<sqlast::SetExpr>)>>,
)> {
    if select.distinct {
        return Err(CompileError::unimplemented(loc.clone(), "DISTINCT"));
    }

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

    let (scope, from) = compile_from(&compiler, &schema, loc, &select.from)?;

    let exprs = select
        .projection
        .iter()
        .map(|p| {
            Ok(match p {
                sqlast::SelectItem::UnnamedExpr(expr) => {
                    // If the expression is an identifier, then simply forward it along. In the case of a
                    // compound identifier (e.g. table.foo), SQL semantics are to pick the last element (i.e.
                    // foo) as the new name.
                    let name = match expr {
                        sqlast::Expr::Identifier(i) => i.value.clone(),
                        sqlast::Expr::CompoundIdentifier(c) => c
                            .last()
                            .expect("Compound identifiers should have at least one element")
                            .value
                            .clone(),
                        _ => format!("{}", expr),
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
                    let name = alias.value.clone();
                    let compiled =
                        compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, expr)?;
                    mkcref(vec![CTypedNameAndSQL {
                        name: Ident::with_location(loc.clone(), name),
                        type_: compiled.type_,
                        sql: compiled.sql,
                    }])
                }
                sqlast::SelectItem::Wildcard => {
                    let available = scope
                        .read()?
                        .get_available_references(compiler.clone(), loc)?;
                    available.then({
                        let loc = loc.clone();
                        move |available: Ref<InsertionOrderMap<String, FieldMatch>>| {
                            let mut ret = Vec::new();
                            for (_, m) in available.read()?.iter() {
                                let type_ = match &m.type_ {
                                    Some(t) => t.clone(),
                                    None => {
                                        return Err(CompileError::duplicate_entry(vec![m
                                            .field
                                            .replace_location(loc.clone())]))
                                    }
                                };
                                let sqlpath = vec![
                                    sqlast::Ident {
                                        value: m.relation.value.clone(),
                                        quote_style: None,
                                    },
                                    sqlast::Ident {
                                        value: m.field.value.clone(),
                                        quote_style: None,
                                    },
                                ];
                                ret.push(CTypedNameAndSQL {
                                    name: m.field.clone(),
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
                sqlast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(CompileError::unimplemented(loc.clone(), "Table wildcard"));
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
                        name: b.name.clone(),
                        type_: b.type_.clone(),
                        nullable: true,
                    });
                }
            }

            Ok(mkcref(MType::List(MListType {
                loc: loc.clone(),
                inner: mkcref(MType::Record(MRecordType {
                    loc: loc.clone(),
                    fields,
                })),
            })))
        }
    })?;

    let select = select.clone();
    let expr: CRef<_> = compiler.clone().async_cref({
        let scope = scope.clone();
        let loc = loc.clone();
        async move {
            let (from_names, from) = cunwrap(from.await?)?;

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
                    alias: ident(sqlexpr.name.value.clone()),
                    expr: sqlexpr.sql.body.as_expr(),
                });
            }
            names.extend(from_names);

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
                let compiled =
                    compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), &loc, gb)?;
                let sql = compiled.sql.await?.read()?.clone();
                names.extend(sql.names.clone());
                group_by.push(sql.body.as_expr());
            }

            let mut ret = select.clone();
            ret.from = from.clone();
            ret.projection = projection;
            ret.selection = selection;
            ret.group_by = group_by;

            let names = scope
                .read()?
                .remove_bound_references(compiler.clone(), names)?;
            let names = names.await?.read()?.clone();

            Ok(cwrap((
                names,
                Box::new(sqlast::SetExpr::Select(Box::new(ret.clone()))),
            )))
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
    loc: &SourceLocation,
    query: &sqlast::Query,
) -> Result<CTypedExpr> {
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

    if query.lock.is_some() {
        return Err(CompileError::unimplemented(
            loc.clone(),
            "FOR { UPDATE | SHARE }",
        ));
    }

    match query.body.as_ref() {
        sqlast::SetExpr::Select(s) => {
            let (scope, type_, select) = compile_select(compiler.clone(), schema.clone(), loc, s)?;

            let compiled_order_by =
                compile_order_by(&compiler, &schema, &scope, loc, &query.order_by)?;

            Ok(CTypedExpr {
                type_,
                expr: compiler.async_cref({
                    let loc = loc.clone();
                    let compiler = compiler.clone();
                    async move {
                        let (mut names, body) = cunwrap(select.await?)?;
                        let limit = match limit {
                            Some(limit) => {
                                Some(finish_sqlexpr(&loc, limit.expr, &mut names).await?)
                            }
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

                        Ok(mkcref(Expr::SQL(Arc::new(SQL {
                            names,
                            body: SQLBody::Query(sqlast::Query {
                                with: None,
                                body,
                                order_by,
                                limit,
                                offset,
                                fetch: None,
                                lock: None,
                            }),
                        }))))
                    }
                })?,
            })
        }
        sqlast::SetExpr::Query(q) => compile_sqlquery(compiler.clone(), schema.clone(), loc, q),
        sqlast::SetExpr::SetOperation { .. } => Err(CompileError::unimplemented(
            loc.clone(),
            "UNION | EXCEPT | INTERSECT",
        )),
        sqlast::SetExpr::Values(_) => Err(CompileError::unimplemented(loc.clone(), "VALUES")),
        sqlast::SetExpr::Insert(_) => Err(CompileError::unimplemented(loc.clone(), "INSERT")),
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

pub fn compile_sqlexpr(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    loc: &SourceLocation,
    expr: &sqlast::Expr,
) -> Result<CTypedExpr> {
    let c_sqlarg =
        |e: &sqlast::Expr| compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, e);

    let ret = match expr {
        sqlast::Expr::Value(v) => match v {
            sqlast::Value::Number(n, _) => CTypedExpr {
                type_: mkcref(MType::Atom(
                    loc.clone(),
                    parse_numeric_type(n).context(TypesystemSnafu { loc: loc.clone() })?,
                )),
                expr: mkcref(Expr::SQL(Arc::new(SQL {
                    names: CSQLNames::new(),
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
                mkcref(MType::Atom(loc.clone(), AtomicType::Null))
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
        sqlast::Expr::IsNotNull(expr) => {
            let compiled = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                expr.as_ref(),
            )?;
            CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "bool")?,
                expr: compiled.sql.then({
                    move |sqlexpr: Ref<SQL<CRef<MType>>>| {
                        Ok(mkcref(Expr::SQL(Arc::new(SQL {
                            names: sqlexpr.read()?.names.clone(),
                            body: SQLBody::Expr(sqlast::Expr::IsNotNull(Box::new(
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
            ..
        }) => {
            if *distinct {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    format!("Function calls with DISTINCT").as_str(),
                ));
            }

            if over.is_some() {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    format!("Function calls with OVER").as_str(),
                ));
            }

            let func_name = name.to_path(loc);
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
                        &MType::Fn(MFnType {
                            loc: loc.clone(),
                            args: Vec::new(),
                            ret: MType::new_unknown("ret"),
                        }),
                        &*func
                            .type_
                            .must()
                            .context(RuntimeSnafu { loc: loc.clone() })?
                            .read()?,
                    ))
                }
            };
            let mut compiled_args: BTreeMap<String, CTypedNameAndExpr> = BTreeMap::new();
            let mut pos: usize = 0;
            for arg in args {
                let (name, expr) = match arg {
                    sqlast::FunctionArg::Named { name, arg } => {
                        (Ident::with_location(loc.clone(), name.value.clone()), arg)
                    }
                    sqlast::FunctionArg::Unnamed(arg) => {
                        if pos >= fn_type.args.len() {
                            return Err(CompileError::no_such_entry(vec![Ident::with_location(
                                loc.clone(),
                                format!("argument {}", pos),
                            )]));
                        }
                        pos += 1;
                        (fn_type.args[pos - 1].name.clone(), arg)
                    }
                };

                let expr = match expr {
                    sqlast::FunctionArgExpr::Expr(e) => Cow::Borrowed(e),
                    sqlast::FunctionArgExpr::Wildcard
                    | sqlast::FunctionArgExpr::QualifiedWildcard(_) => {
                        // Wildcards (qualified or not) are only supported for certain functions
                        // (count as far as we know, and potentially others).
                        match func_name.as_slice().first().map(|s| s.value.as_str()) {
                            Some("count") => Cow::Owned(sqlast::Expr::Value(
                                sqlast::Value::Number("1".to_string(), false),
                            )),
                            _ => {
                                return Err(CompileError::unimplemented(
                                    loc.clone(),
                                    &format!("wildcard arguments for {:?} function", name),
                                ))
                            }
                        }
                    }
                };

                if compiled_args.get(&name.value).is_some() {
                    return Err(CompileError::duplicate_entry(vec![name]));
                }

                let compiled_arg =
                    compile_sqlexpr(compiler.clone(), schema.clone(), scope.clone(), loc, &expr)?;
                compiled_args.insert(
                    name.value.clone(),
                    CTypedNameAndExpr {
                        name: name.clone(),
                        type_: compiled_arg.type_,
                        expr: compiled_arg.expr,
                    },
                );
            }

            let mut arg_exprs = Vec::new();
            for arg in &fn_type.args {
                if let Some(compiled_arg) = compiled_args.get_mut(&arg.name.value) {
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
                    return Err(CompileError::missing_arg(vec![arg.name.clone()]));
                }
            }

            let type_ = fn_type.ret.clone();

            let expr = compiler.async_cref({
                let compiler = compiler.clone();
                let loc = loc.clone();
                let name = name.clone();
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
                    let args = arg_exprs
                        .read()?
                        .iter()
                        .map(|e| Ok(e.read()?.clone()))
                        .collect::<Result<Vec<_>>>()?;

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
                    let can_lift = !args.iter().any(|a| has_unbound_names(a.expr.clone()));
                    let should_lift = if compiler.allow_inlining() {
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
                            (FnKind::Expr, Some(fn_body)) if compiler.allow_inlining() => {
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
                                        .map(|a| (a.name.value.clone(), a.expr.clone()))
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
                                        name: arg.read()?.name.to_sqlident(),
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

                                        // TODO: We may want to forward these fields from the
                                        // expr (but doing that blindly causes ownership errors)
                                        over: None,
                                        distinct: false,
                                        special: false,
                                    })),
                                }))))
                            }
                        }
                    }
                }
            })?;

            CTypedExpr { type_, expr }
        }
        sqlast::Expr::CompoundIdentifier(sqlpath) => compile_sqlreference(
            compiler.clone(),
            schema.clone(),
            scope.clone(),
            loc,
            sqlpath,
        )?,
        sqlast::Expr::Identifier(ident) => compile_sqlreference(
            compiler.clone(),
            schema.clone(),
            scope.clone(),
            loc,
            &vec![ident.clone()],
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
