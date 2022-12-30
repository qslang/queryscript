use crate::ast::Pretty;
pub use arrow::datatypes::DataType as ArrowDataType;
use colored::*;
use snafu::prelude::*;
use sqlparser::ast as sqlast;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};

use crate::ast;
use crate::ast::SourceLocation;
use crate::compile::{
    coerce::{coerce_types, CoerceOp},
    error::*,
    inference::{mkcref, Constrainable, Constrained},
    sql::{ident, select_from, select_no_from, select_star_from, with_table_alias},
};
use crate::runtime;
use crate::types::{AtomicType, Field, FnType, Type};

pub use crate::compile::inference::CRef;

pub type Ident = ast::Ident;

#[derive(Debug, Clone)]
pub struct MField {
    pub name: Ident,
    pub type_: CRef<MType>,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct MFnType {
    pub args: Vec<MField>,
    pub ret: CRef<MType>,
}

impl MField {
    pub fn new_nullable(name: Ident, type_: CRef<MType>) -> MField {
        MField {
            name,
            type_,
            nullable: true,
        }
    }
}

// Here, "M" means monomorphic.  MTypes can contain free variables in the form of the "Name" branch
// below, but they can't contain universal types.  Universal types can only exist at the top-level
// of a schema declaration, and so are represented within `SType` below.
//
#[derive(Clone)]
pub enum MType {
    Atom(Located<AtomicType>),
    Record(Located<Vec<MField>>),
    List(Located<CRef<MType>>),
    Fn(Located<MFnType>),
    Name(Located<Ident>),
    External(Located<CRef<MType>>),
}

impl MType {
    pub fn new_unknown(debug_name: &str) -> CRef<MType> {
        CRef::new_unknown(debug_name)
    }

    pub fn to_runtime_type(&self) -> runtime::error::Result<Type> {
        match self {
            MType::Atom(a) => Ok(Type::Atom(a.get().clone())),
            MType::Record(fields) => Ok(Type::Record(
                fields
                    .iter()
                    .map(|f| {
                        Ok(Field {
                            name: f.name.value.clone(),
                            type_: f.type_.must()?.read()?.to_runtime_type()?,
                            nullable: f.nullable,
                        })
                    })
                    .collect::<runtime::error::Result<Vec<_>>>()?,
            )),
            MType::List(inner) => Ok(Type::List(Box::new(
                inner.must()?.read()?.to_runtime_type()?,
            ))),
            MType::Fn(fn_type) => Ok(Type::Fn(FnType {
                args: fn_type
                    .args
                    .iter()
                    .map(|a| {
                        Ok(Field {
                            name: a.name.value.clone(),
                            type_: a.type_.must()?.read()?.to_runtime_type()?,
                            nullable: a.nullable,
                        })
                    })
                    .collect::<runtime::error::Result<Vec<_>>>()?,
                ret: Box::new(fn_type.ret.must()?.read()?.to_runtime_type()?),
            })),
            MType::Name { .. } => {
                runtime::error::fail!("Unresolved type name cannot exist at runtime: {:?}", self)
            }
            MType::External(inner_type) => inner_type.must()?.read()?.to_runtime_type(),
        }
    }

    pub fn from_runtime_type(type_: &Type) -> Result<MType> {
        match type_ {
            Type::Atom(a) => Ok(MType::Atom(Located::new(
                a.clone(),
                SourceLocation::Unknown,
            ))),
            Type::Record(fields) => Ok(MType::Record(Located::new(
                fields
                    .iter()
                    .map(|f| {
                        Ok(MField {
                            name: Ident::without_location(f.name.clone()),
                            type_: mkcref(MType::from_runtime_type(&f.type_)?),
                            nullable: f.nullable,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                SourceLocation::Unknown,
            ))),
            Type::List(inner) => Ok(MType::List(Located::new(
                mkcref(MType::from_runtime_type(&inner)?),
                SourceLocation::Unknown,
            ))),
            Type::Fn(FnType { args, ret }) => Ok(MType::Fn(Located::new(
                MFnType {
                    args: args
                        .iter()
                        .map(|a| {
                            Ok(MField {
                                name: Ident::without_location(a.name.clone()),
                                type_: mkcref(MType::from_runtime_type(&a.type_)?),
                                nullable: a.nullable,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    ret: mkcref(MType::from_runtime_type(&ret)?),
                },
                SourceLocation::Unknown,
            ))),
        }
    }

    pub fn substitute(&self, variables: &BTreeMap<String, CRef<MType>>) -> Result<CRef<MType>> {
        let type_ = match self {
            MType::Atom(a) => mkcref(MType::Atom(a.clone())),
            MType::Record(fields) => mkcref(MType::Record(Located::new(
                fields
                    .iter()
                    .map(|f| {
                        Ok(MField {
                            name: f.name.clone(),
                            type_: f.type_.substitute(variables)?,
                            nullable: f.nullable,
                        })
                    })
                    .collect::<Result<_>>()?,
                fields.location().clone(),
            ))),
            MType::List(inner) => mkcref(MType::List(Located::new(
                inner.substitute(variables)?,
                inner.location().clone(),
            ))),
            MType::Fn(mfn) => {
                let MFnType { args, ret } = mfn.get();
                let location = mfn.location();
                mkcref(MType::Fn(Located::new(
                    MFnType {
                        args: args
                            .iter()
                            .map(|a| {
                                Ok(MField {
                                    name: a.name.clone(),
                                    type_: a.type_.substitute(variables)?,
                                    nullable: a.nullable,
                                })
                            })
                            .collect::<Result<_>>()?,
                        ret: ret.substitute(variables)?,
                    },
                    location.clone(),
                )))
            }
            MType::Name(n) => variables
                .get(&n.get().value)
                .ok_or_else(|| CompileError::no_such_entry(vec![n.get().clone()]))?
                .clone(),
            MType::External(inner) => mkcref(MType::External(Located::new(
                inner.substitute(variables)?,
                inner.location().clone(),
            ))),
        };

        Ok(type_)
    }

    pub fn location(&self) -> SourceLocation {
        match self {
            MType::Atom(t) => t.location().clone(),
            MType::Record(t) => t.location().clone(),
            MType::List(t) => t.location().clone(),
            MType::Fn(t) => t.location().clone(),
            MType::Name(t) => t.location().clone(),
            MType::External(t) => t.location().clone(),
        }
    }
}

impl Pretty for MType {
    fn pretty(&self) -> String {
        format!("{:?}", self).white().bold().to_string()
    }
}

impl Into<SType> for CRef<MType> {
    fn into(self) -> SType {
        SType {
            variables: BTreeSet::new(),
            body: self,
        }
    }
}

pub trait HasCExpr<E>
where
    E: Constrainable,
{
    fn expr(&self) -> &CRef<E>;
}

pub trait HasCType<T>
where
    T: Constrainable,
{
    fn type_(&self) -> &CRef<T>;
}

// Here, "C" means constrained.  In general, any structs prefixed with C indicate that there are
// structures that may be unknown within them.
//
#[derive(Clone, Debug)]
pub struct CTypedExpr {
    pub type_: CRef<MType>,
    pub expr: CRef<Expr<CRef<MType>>>,
}

impl CTypedExpr {
    pub fn to_runtime_type(&self) -> runtime::error::Result<TypedExpr<Ref<Type>>> {
        Ok(TypedExpr {
            type_: mkref(self.type_.must()?.read()?.to_runtime_type()?),
            expr: Arc::new(self.expr.must()?.read()?.to_runtime_type()?),
        })
    }
}

// Here, "C" means constrained.  In general, any structs prefixed with C indicate that there are
// structures that may be unknown within them.
//
#[derive(Clone, Debug)]
pub struct CTypedNameAndExpr {
    pub name: Ident,
    pub type_: CRef<MType>,
    pub expr: CRef<Expr<CRef<MType>>>,
}

struct DebugMFields<'a>(&'a Vec<MField>);

impl<'a> fmt::Debug for DebugMFields<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for i in 0..self.0.len() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str(self.0[i].name.value.as_str())?;
            f.write_str(" ")?;
            self.0[i].type_.fmt(f)?;
            if !self.0[i].nullable {
                f.write_str(" not null")?;
            }
        }
        f.write_str("}")?;
        Ok(())
    }
}

impl fmt::Debug for MType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MType::Atom(atom) => atom.fmt(f)?,
            MType::Record(fields) => DebugMFields(fields).fmt(f)?,
            MType::List(inner) => {
                f.write_str("[")?;
                inner.fmt(f)?;
                f.write_str("]")?;
            }
            MType::Fn(mfn) => {
                let MFnType { args, ret } = mfn.get();
                f.write_str("λ ")?;
                DebugMFields(&args).fmt(f)?;
                f.write_str(" -> ")?;
                ret.fmt(f)?;
            }
            MType::Name(n) => n.value.fmt(f)?,
            MType::External(t) => {
                f.write_str("External<")?;
                t.fmt(f)?;
                f.write_str(">")?;
            }
        }
        Ok(())
    }
}

impl Constrainable for MType {
    fn unify(&self, other: &MType) -> Result<()> {
        if !matches!(self, MType::External(..)) && matches!(other, MType::External(..)) {
            // If there is an external type, ensure it's on the LHS
            return other.unify(self);
        }

        match self {
            MType::Atom(la) => match other {
                MType::Atom(ra) => {
                    if la.get() != ra.get() {
                        return Err(CompileError::wrong_type(self, other));
                    }
                }
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::Record(rtype) => match other {
                MType::Record(ltype) => ltype.unify(rtype)?,
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::List(linner) => match other {
                MType::List(rinner) => linner.unify(rinner)?,
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::Fn(lmfn) => {
                let MFnType {
                    args: largs,
                    ret: lret,
                } = lmfn.get();
                let lloc = lmfn.location();
                match other {
                    MType::Fn(rmfn) => {
                        let MFnType {
                            args: rargs,
                            ret: rret,
                        } = rmfn.get();
                        let rloc = rmfn.location();
                        Located::new(largs.clone(), lloc.clone())
                            .unify(&Located::new(rargs.clone(), rloc.clone()))?;
                        lret.unify(rret)?;
                    }
                    _ => return Err(CompileError::wrong_type(self, other)),
                }
            }
            MType::Name(name) => {
                return Err(CompileError::internal(
                    name.location().clone(),
                    format!("Encountered free type variable: {}", name.get().value).as_str(),
                ))
            }
            MType::External(le) => match other {
                MType::External(re) => {
                    le.unify(re)?;
                }
                other => {
                    le.unify(&mkcref(other.clone()))?;
                }
            },
        }

        Ok(())
    }

    fn coerce(op: &CoerceOp, left: &Ref<Self>, right: &Ref<Self>) -> Result<CRef<Self>> {
        let left_type = left.read()?;
        let right_type = right.read()?;

        let left_loc = left_type.location();
        let right_loc = right_type.location();

        let left_rt = left_type.to_runtime_type().context(RuntimeSnafu {
            loc: left_loc.clone(),
        })?;
        let right_rt = right_type.to_runtime_type().context(RuntimeSnafu {
            loc: right_loc.clone(),
        })?;

        let coerced_type = match coerce_types(&left_rt, op, &right_rt) {
            Some(t) => t,
            None => {
                return Err(CompileError::coercion(
                    left_type.location(),
                    &left_type,
                    &right_type,
                ))
            }
        };

        Ok(mkcref(MType::from_runtime_type(&coerced_type)?))
    }
}

impl Constrainable for Located<Vec<MField>> {
    fn unify(&self, other: &Located<Vec<MField>>) -> Result<()> {
        let err = || {
            CompileError::wrong_type(&MType::Record(self.clone()), &MType::Record(other.clone()))
        };
        if self.len() != other.len() {
            return Err(err());
        }

        for i in 0..self.len() {
            if self[i].name.value != other[i].name.value {
                return Err(err());
            }

            if self[i].nullable != other[i].nullable {
                return Err(err());
            }

            self[i].type_.unify(&other[i].type_)?;
        }

        Ok(())
    }
}

impl CRef<MType> {
    pub fn substitute(&self, variables: &BTreeMap<String, CRef<MType>>) -> Result<CRef<MType>> {
        match &*self.read()? {
            Constrained::Known(t) => t.read()?.substitute(variables),
            Constrained::Unknown { .. } => Ok(self.clone()),
            Constrained::Ref(r) => r.substitute(variables),
        }
    }
}

impl<T> CRef<T>
where
    T: Constrainable + 'static,
{
    pub async fn clone_inner(&self) -> Result<T> {
        let expr = self.await?;
        let expr = expr.read()?;
        Ok(expr.clone())
    }
}

pub type Ref<T> = Arc<RwLock<T>>;

// Here, "S" means schema.  This is to distinguish types that can exist anywhere and be built
// recursively (i.e. monomorphic types) from ones that can only exist at the top-level of a schema
// declaration (i.e. polymorphic types)
//
#[derive(Clone)]
pub struct SType {
    pub variables: BTreeSet<String>,
    pub body: CRef<MType>,
}

impl SType {
    pub fn new_mono(body: CRef<MType>) -> CRef<SType> {
        mkcref(SType {
            variables: BTreeSet::new(),
            body,
        })
    }

    pub fn new_poly(body: CRef<MType>, variables: BTreeSet<String>) -> CRef<SType> {
        mkcref(SType { variables, body })
    }
}

impl fmt::Debug for SType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.variables.len() > 0 {
            f.write_str("∀ ")?;
            for (i, variable) in self.variables.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                variable.fmt(f)?;
            }
            f.write_str(" ")?;
        }
        self.body.fmt(f)
    }
}

impl Constrainable for SType {}

#[derive(Clone)]
pub struct SchemaInstance {
    pub schema: SchemaRef,
    pub id: Option<usize>,
}

impl fmt::Debug for SchemaInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(f.debug_struct("FnExpr")
            .field("id", &self.id)
            .finish_non_exhaustive()?)
    }
}

impl SchemaInstance {
    pub fn global(schema: SchemaRef) -> SchemaInstance {
        SchemaInstance { schema, id: None }
    }

    pub fn instance(schema: SchemaRef, id: usize) -> SchemaInstance {
        SchemaInstance {
            schema,
            id: Some(id),
        }
    }
}

pub type Value = crate::types::Value;

pub type Params<TypeRef> = BTreeMap<String, TypedExpr<TypeRef>>;
pub type UnboundPaths = BTreeSet<Vec<String>>;

#[derive(Clone)]
pub enum SQLBody {
    Expr(sqlast::Expr),
    Query(sqlast::Query),
    Table(sqlast::TableFactor),
}

impl SQLBody {
    pub fn as_expr(&self) -> sqlast::Expr {
        match self {
            SQLBody::Expr(expr) => expr.clone(),
            SQLBody::Query(_) | SQLBody::Table(_) => {
                sqlast::Expr::Subquery(Box::new(sqlast::Query {
                    with: None,
                    body: Box::new(sqlast::SetExpr::Select(Box::new(sqlast::Select {
                        distinct: false,
                        top: None,
                        projection: vec![sqlast::SelectItem::ExprWithAlias {
                            expr: sqlast::Expr::Function(sqlast::Function {
                                name: sqlast::ObjectName(vec![ident("array_agg".to_string())]),
                                args: vec![sqlast::FunctionArg::Unnamed(
                                    sqlast::FunctionArgExpr::Expr(sqlast::Expr::Identifier(ident(
                                        "subquery".to_string(),
                                    ))),
                                )],
                                over: None,
                                distinct: false,
                                special: false,
                            }),
                            alias: sqlast::Ident::new("value"),
                        }],
                        into: None,
                        from: vec![sqlast::TableWithJoins {
                            relation: self.as_table(Some(sqlast::TableAlias {
                                name: ident("subquery".to_string()),
                                columns: Vec::new(),
                            })),
                            joins: Vec::new(),
                        }],
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
                }))
            }
        }
    }

    // Returns a query that can be run as-is, but violates the array vs. result set invariants.
    //
    pub fn as_query(&self) -> sqlast::Query {
        match self {
            SQLBody::Expr(expr) => select_no_from(expr.clone(), None),
            SQLBody::Query(query) => query.clone(),
            SQLBody::Table(table) => select_star_from(table.clone()),
        }
    }

    // Assumes that if the SQL body is an expression, then it's of an array type.
    //
    pub fn as_table(&self, alias: Option<sqlast::TableAlias>) -> sqlast::TableFactor {
        match self {
            SQLBody::Expr(expr) => SQLBody::Query(select_from(
                // The result of the inner subquery is a relation of rows, each with a record field
                // called "value".  This extra subquery splats the fields of "value" onto the top
                // level of the query.
                vec![sqlast::SelectItem::QualifiedWildcard(sqlast::ObjectName(
                    vec![ident("value".to_string())],
                ))],
                vec![sqlast::TableWithJoins {
                    // In order to get around a binder bug in duckdb, we have to put the unnest
                    // call in a project list instead of as a table factor.  This results in a set
                    // of rows, each with a single record field.
                    //
                    relation: SQLBody::Query(select_no_from(
                        sqlast::Expr::Function(sqlast::Function {
                            name: sqlast::ObjectName(vec![ident("unnest".to_string())]),
                            args: vec![sqlast::FunctionArg::Unnamed(
                                sqlast::FunctionArgExpr::Expr(expr.clone()),
                            )],
                            over: None,
                            distinct: false,
                            special: false,
                        }),
                        Some(ident("value".to_string())),
                    ))
                    .as_table(Some(sqlast::TableAlias {
                        name: ident("unnest".to_string()),
                        columns: Vec::new(),
                    })),
                    joins: Vec::new(),
                }],
            ))
            .as_table(alias),
            SQLBody::Query(query) => sqlast::TableFactor::Derived {
                lateral: false,
                subquery: Box::new(query.clone()),
                alias,
            },
            SQLBody::Table(table) => with_table_alias(table, alias),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SQLNames<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    // A mapping of names to expressions that must be computed and provided in order for the SQL
    // body to run correctly.  This enables us to reference the results of non-SQL expressions from
    // within the SQL (e.g. the output of a native function that can't be inlined).
    //
    pub params: Params<TypeRef>,

    // The set of identifiers in the body that refer to objects not defined within the parameters or
    // the body itself (i.e. builtin function or sql references to tables outside the contained
    // expression or query).  A SQL expression with unbound variables cannot be executed directly,
    // and must be inlined into a broader query that provides definitions for the unbound names.
    //
    pub unbound: UnboundPaths,
}

impl<TypeRef> Constrainable for SQLNames<TypeRef> where TypeRef: Clone + fmt::Debug + Send + Sync {}

impl<TypeRef> SQLNames<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    pub fn new() -> SQLNames<TypeRef> {
        SQLNames {
            params: BTreeMap::new(),
            unbound: BTreeSet::new(),
        }
    }

    pub fn from_unbound(sqlpath: &Vec<sqlast::Located<sqlast::Ident>>) -> SQLNames<TypeRef> {
        SQLNames {
            params: BTreeMap::new(),
            unbound: BTreeSet::from([sqlpath.iter().map(|i| i.value.clone()).collect()]),
        }
    }

    pub fn extend(&mut self, other: SQLNames<TypeRef>) {
        self.params.extend(other.params);
        self.unbound.extend(other.unbound);
    }
}

#[derive(Clone)]
pub struct SQL<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    // Descriptions of all externally defined names within the SQL body.
    //
    pub names: SQLNames<TypeRef>,

    // The AST representing the actual body of the SQL query or expression.
    //
    pub body: SQLBody,
}

impl<T: Clone + fmt::Debug + Send + Sync> fmt::Debug for SQL<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body = match &self.body {
            SQLBody::Expr(expr) => expr.to_string(),
            SQLBody::Query(query) => query.to_string(),
            SQLBody::Table(table) => table.to_string(),
        };
        f.debug_struct("SQL")
            .field("names", &self.names)
            .field("body", &body)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum FnKind {
    SQLBuiltin,
    Native,
    Expr,
}

#[derive(Debug, Clone)]
pub enum FnBody<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    SQLBuiltin,
    Expr(Arc<Expr<TypeRef>>),
}

impl FnBody<CRef<MType>> {
    pub fn to_runtime_type(&self) -> runtime::error::Result<FnBody<Ref<Type>>> {
        Ok(match self {
            FnBody::SQLBuiltin => FnBody::SQLBuiltin,
            FnBody::Expr(e) => FnBody::Expr(Arc::new(e.to_runtime_type()?)),
        })
    }
}

#[derive(Clone)]
pub struct FnExpr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    pub inner_schema: Ref<Schema>,
    pub body: FnBody<TypeRef>,
}

impl<TypeRef: Clone + fmt::Debug + Send + Sync> fmt::Debug for FnExpr<TypeRef> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(f.debug_struct("FnExpr")
            .field("body", &self.body)
            .finish_non_exhaustive()?)
    }
}

#[derive(Clone, Debug)]
pub struct FnCallExpr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    pub func: Arc<TypedExpr<TypeRef>>,
    pub args: Vec<TypedExpr<TypeRef>>,
    pub ctx_folder: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Expr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    SQL(Arc<SQL<TypeRef>>),
    SchemaEntry(STypedExpr),
    Fn(FnExpr<TypeRef>),
    FnCall(FnCallExpr<TypeRef>),
    NativeFn(String),
    ContextRef(String),
    Unknown,
}

impl Expr<CRef<MType>> {
    pub fn to_runtime_type(&self) -> runtime::error::Result<Expr<Ref<Type>>> {
        match self {
            Expr::SQL(e) => {
                let SQL { names, body } = e.as_ref();
                Ok(Expr::SQL(Arc::new(SQL {
                    names: SQLNames {
                        params: names
                            .params
                            .iter()
                            .map(|(name, param)| Ok((name.clone(), param.to_runtime_type()?)))
                            .collect::<runtime::error::Result<_>>()?,
                        unbound: names.unbound.clone(),
                    },
                    body: body.clone(),
                })))
            }
            Expr::Fn(FnExpr { inner_schema, body }) => Ok(Expr::Fn(FnExpr {
                inner_schema: inner_schema.clone(),
                body: body.to_runtime_type()?,
            })),
            Expr::FnCall(FnCallExpr {
                func,
                args,
                ctx_folder,
            }) => Ok(Expr::FnCall(FnCallExpr {
                func: Arc::new(func.to_runtime_type()?),
                args: args
                    .iter()
                    .map(|a| Ok(a.to_runtime_type()?))
                    .collect::<runtime::error::Result<_>>()?,
                ctx_folder: ctx_folder.clone(),
            })),
            Expr::SchemaEntry(e) => Ok(Expr::SchemaEntry(e.clone())),
            Expr::NativeFn(f) => Ok(Expr::NativeFn(f.clone())),
            Expr::ContextRef(r) => Ok(Expr::ContextRef(r.clone())),
            Expr::Unknown => Ok(Expr::Unknown),
        }
    }

    pub async fn unwrap_schema_entry(
        self: &Arc<Expr<CRef<MType>>>,
    ) -> Result<Arc<Expr<CRef<MType>>>> {
        let mut ret = self.clone();
        loop {
            match ret.as_ref() {
                Expr::SchemaEntry(STypedExpr { expr, .. }) => {
                    ret = Arc::new(expr.await?.read()?.clone())
                }
                _ => return Ok(ret),
            }
        }
    }
}

impl<Ty: Clone + fmt::Debug + Send + Sync> Constrainable for Expr<Ty> {}

#[derive(Clone, Debug)]
pub struct TypedExpr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    pub type_: TypeRef,
    pub expr: Arc<Expr<TypeRef>>,
}

impl TypedExpr<CRef<MType>> {
    pub fn to_runtime_type(&self) -> runtime::error::Result<TypedExpr<Ref<Type>>> {
        Ok(TypedExpr::<Ref<Type>> {
            type_: mkref(self.type_.must()?.read()?.to_runtime_type()?),
            expr: Arc::new(self.expr.to_runtime_type()?),
        })
    }
}

impl Constrainable for TypedExpr<CRef<MType>> {}

#[derive(Clone)]
pub struct STypedExpr {
    pub type_: CRef<SType>,
    pub expr: CRef<Expr<CRef<MType>>>,
}

impl STypedExpr {
    pub fn new_unknown(debug_name: &str) -> STypedExpr {
        STypedExpr {
            type_: CRef::new_unknown(&format!("{} type", debug_name)),
            expr: CRef::new_unknown(&format!("{} expr", debug_name)),
        }
    }

    pub fn to_runtime_type(&self) -> runtime::error::Result<TypedExpr<Ref<Type>>> {
        Ok(TypedExpr::<Ref<Type>> {
            type_: mkref(
                self.type_
                    .must()?
                    .read()?
                    .body
                    .must()?
                    .read()?
                    .to_runtime_type()?,
            ),
            expr: Arc::new(self.expr.must()?.read()?.to_runtime_type()?),
        })
    }
}

impl fmt::Debug for STypedExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("STypedExpr")
            .field("type_", &*self.type_.read().unwrap())
            .field("expr", &self.expr)
            .finish()
    }
}

impl Constrainable for STypedExpr {
    fn unify(&self, other: &Self) -> Result<()> {
        self.expr.unify(&other.expr)?;
        self.type_.unify(&other.type_)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum SchemaEntry {
    Schema(ast::Path),
    Type(CRef<MType>),
    Expr(STypedExpr),
}

impl SchemaEntry {
    pub fn kind(&self) -> String {
        match self {
            SchemaEntry::Schema(_) => "schema".to_string(),
            SchemaEntry::Type(_) => "type".to_string(),
            SchemaEntry::Expr(_) => "value".to_string(),
        }
    }
}

pub fn mkref<T>(t: T) -> Ref<T> {
    Arc::new(RwLock::new(t))
}

#[derive(Clone, Debug)]
pub struct Decl {
    pub public: bool,
    pub extern_: bool,
    pub name: Ident,
    pub value: SchemaEntry,
}

#[derive(Clone, Debug)]
pub struct TypedNameAndExpr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    pub name: Ident,
    pub type_: TypeRef,
    pub expr: Arc<Expr<TypeRef>>,
}

impl<TypeRef> TypedNameAndExpr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    pub fn to_typed_expr(&self) -> TypedExpr<TypeRef> {
        TypedExpr {
            type_: self.type_.clone(),
            expr: self.expr.clone(),
        }
    }
}

impl Constrainable for TypedNameAndExpr<CRef<MType>> {}

pub type SchemaRef = Ref<Schema>;

#[derive(Clone, Debug)]
pub struct TypedName<TypeRef> {
    pub name: Ident,
    pub type_: TypeRef,
}

#[derive(Clone, Debug)]
pub struct ImportedSchema {
    pub args: Option<Vec<BTreeMap<String, TypedNameAndExpr<CRef<MType>>>>>,
    pub schema: SchemaRef,
}

pub type Located<T> = sqlast::Located<T, SourceLocation>;

impl<T> Pretty for Located<T> {
    fn pretty(&self) -> String {
        self.location().pretty()
    }
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub file: String,
    pub folder: Option<String>,
    pub parent_scope: Option<Ref<Schema>>,
    pub externs: BTreeMap<String, CRef<MType>>,
    pub decls: BTreeMap<String, Located<Decl>>,
    pub imports: BTreeMap<Vec<String>, Ref<ImportedSchema>>,
    pub exprs: Vec<Located<CTypedExpr>>,
}

impl Schema {
    pub fn new(file: String, folder: Option<String>) -> Ref<Schema> {
        mkref(Schema {
            file,
            folder,
            parent_scope: None,
            externs: BTreeMap::new(),
            decls: BTreeMap::new(),
            imports: BTreeMap::new(),
            exprs: Vec::new(),
        })
    }
}

pub const SCHEMA_EXTENSIONS: &[&str] = &["tql"];
