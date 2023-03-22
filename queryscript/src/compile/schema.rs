use crate::ast::{Pretty, ToIdents};
pub use arrow::datatypes::DataType as ArrowDataType;
use colored::*;
use sqlparser::ast::{self as sqlast, WildcardAdditionalOptions};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};

use crate::ast;
pub use crate::ast::Located;
use crate::ast::SourceLocation;
use crate::compile::{
    compile::SymbolKind,
    connection::{ConnectionSchema, ConnectionString},
    error::*,
    generics::Generic,
    inference::{mkcref, Constrainable, Constrained},
    sql::{select_from, select_no_from, select_star_from, with_table_alias},
};
use crate::runtime;
use crate::types::{AtomicType, Field, FnType, Type};

pub use crate::compile::inference::CRef;
pub use ast::Ident;

use super::generics::as_generic;

#[derive(Debug, Clone)]
pub struct MField {
    pub name: Ident,
    pub type_: CRef<MType>,
    pub nullable: bool,
}

impl Constrainable for MField {
    fn unify(&self, other: &MField) -> Result<()> {
        if self.name != other.name || self.nullable != other.nullable {
            // TODO: Make the name located so we can use it here.
            //
            return Err(CompileError::wrong_type(
                &MType::Record(Located::new(vec![self.clone()], SourceLocation::Unknown)),
                &MType::Record(Located::new(vec![other.clone()], SourceLocation::Unknown)),
            ));
        }
        self.type_.unify(&other.type_)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MFnType {
    pub args: Vec<MField>,
    pub variadic_arg: Option<MField>,
    pub ret: CRef<MType>,
}

impl Constrainable for MFnType {
    fn unify(&self, other: &MFnType) -> Result<()> {
        let MFnType {
            args: largs,
            ret: lret,
            variadic_arg: lvariadic_arg,
        } = self;
        let MFnType {
            args: rargs,
            ret: rret,
            variadic_arg: rvariadic_arg,
        } = other;
        largs.unify(&rargs)?;
        lret.unify(&rret)?;
        lvariadic_arg.unify(&rvariadic_arg)?;
        Ok(())
    }
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
    Generic(Located<Arc<dyn Generic>>),
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
                            name: f.name.clone(),
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
                            name: a.name.clone(),
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
            MType::Generic(generic) => generic.to_runtime_type(),
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
                            name: f.name.clone(),
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
                                name: a.name.clone(),
                                type_: mkcref(MType::from_runtime_type(&a.type_)?),
                                nullable: a.nullable,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    ret: mkcref(MType::from_runtime_type(&ret)?),
                    variadic_arg: None,
                },
                SourceLocation::Unknown,
            ))),
        }
    }

    pub fn resolve_generics(&self) -> Result<CRef<MType>> {
        match self {
            MType::Atom(a) => Ok(mkcref(MType::Atom(a.clone()))),
            MType::Record(fields) => {
                let mut resolved_fields = Vec::new();
                for f in fields.get() {
                    let field_type = f.type_.then(|t: Ref<MType>| t.read()?.resolve_generics())?;
                    resolved_fields.push(MField {
                        name: f.name.clone(),
                        type_: field_type,
                        nullable: f.nullable,
                    });
                }
                Ok(mkcref(MType::Record(Located::new(
                    resolved_fields,
                    SourceLocation::Unknown,
                ))))
            }
            MType::List(inner) => Ok(mkcref(MType::List(Located::new(
                inner.then(|t: Ref<MType>| t.read()?.resolve_generics())?,
                SourceLocation::Unknown,
            )))),
            MType::Fn(fn_type) => {
                let mut resolved_args = Vec::new();
                for a in &fn_type.args {
                    let arg_type = a.type_.then(|t: Ref<MType>| t.read()?.resolve_generics())?;
                    resolved_args.push(MField {
                        name: a.name.clone(),
                        type_: arg_type,
                        nullable: a.nullable,
                    });
                }
                let resolved_variadic = match &fn_type.variadic_arg {
                    Some(a) => {
                        let arg_type =
                            a.type_.then(|t: Ref<MType>| t.read()?.resolve_generics())?;
                        Some(MField {
                            name: a.name.clone(),
                            type_: arg_type,
                            nullable: a.nullable,
                        })
                    }
                    None => None,
                };
                let ret_type = fn_type
                    .ret
                    .then(|t: Ref<MType>| t.read()?.resolve_generics())?;
                Ok(mkcref(MType::Fn(Located::new(
                    MFnType {
                        args: resolved_args,
                        variadic_arg: resolved_variadic,
                        ret: ret_type,
                    },
                    SourceLocation::Unknown,
                ))))
            }
            MType::Name(n) => Ok(mkcref(MType::Name(n.clone()))),
            MType::Generic(generic) => Ok(generic.resolve(generic.location())?),
        }
    }

    pub fn substitute(&self, variables: &BTreeMap<Ident, CRef<MType>>) -> Result<CRef<MType>> {
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
                let MFnType {
                    args,
                    variadic_arg,
                    ret,
                } = mfn.get();
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
                        variadic_arg: match variadic_arg {
                            Some(a) => Some(MField {
                                name: a.name.clone(),
                                type_: a.type_.substitute(variables)?,
                                nullable: a.nullable,
                            }),
                            None => None,
                        },
                        ret: ret.substitute(variables)?,
                    },
                    location.clone(),
                )))
            }
            MType::Name(n) => variables
                .get(n.get())
                .map(Clone::clone)
                .unwrap_or_else(|| mkcref(MType::Name(n.clone()))),
            MType::Generic(generic) => {
                let location = generic.location();
                mkcref(MType::Generic(Located::new(
                    generic.substitute(variables)?,
                    location.clone(),
                )))
            }
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
            MType::Generic(t) => t.location().clone(),
        }
    }

    pub fn as_generic<T: Generic + 'static>(&self) -> Option<&T> {
        match self {
            MType::Generic(generic) => as_generic(generic.get().as_ref()),
            _ => None,
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

    pub fn split(expr: CRef<Self>) -> Result<Self> {
        Ok(CTypedExpr {
            type_: expr.then(|e: Arc<RwLock<CTypedExpr>>| Ok(e.read()?.type_.clone()))?,
            expr: expr.then(|e: Arc<RwLock<CTypedExpr>>| Ok(e.read()?.expr.clone()))?,
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

struct DebugMFields<'a>(&'a Vec<MField>, bool);

impl<'a> fmt::Debug for DebugMFields<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        if f.alternate() {
            f.write_str("\n")?;
        }
        for i in 0..self.0.len() {
            if f.alternate() {
                f.write_str("\t")?;
            }
            f.write_str(self.0[i].name.as_str())?;
            f.write_str(" ")?;
            self.0[i].type_.fmt(f)?;
            if !self.0[i].nullable {
                f.write_str(" not null")?;
            }
            if f.alternate() {
                f.write_str(",\n")?;
            } else if i < self.0.len() - 1 {
                f.write_str(", ")?;
            }
        }
        if self.1 {
            f.write_str("...")?;
        }
        f.write_str("}")?;
        Ok(())
    }
}

impl fmt::Debug for MType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MType::Atom(atom) => atom.fmt(f)?,
            MType::Record(fields) => DebugMFields(fields, false).fmt(f)?,
            MType::List(inner) => {
                f.write_str("[")?;
                inner.fmt(f)?;
                f.write_str("]")?;
            }
            MType::Fn(mfn) => {
                let MFnType {
                    args,
                    ret,
                    variadic_arg,
                } = mfn.get();
                f.write_str("λ ")?;
                DebugMFields(&args, variadic_arg.is_some()).fmt(f)?;
                f.write_str(" -> ")?;
                ret.fmt(f)?;
            }
            MType::Name(n) => n.get().fmt(f)?,
            MType::Generic(t) => {
                t.get().fmt(f)?;
            }
        }
        Ok(())
    }
}

impl Constrainable for MType {
    fn unify(&self, other: &MType) -> Result<()> {
        if !matches!(self, MType::Generic(..)) && matches!(other, MType::Generic(..)) {
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
            MType::Fn(lmfn) => match other {
                MType::Fn(rmfn) => lmfn.unify(rmfn)?,
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::Name(name) => {
                return Err(CompileError::internal(
                    name.location().clone(),
                    format!("Encountered free type variable: {}", name.get()).as_str(),
                ))
            }
            MType::Generic(generic) => {
                generic.unify(other)?;
            }
        }

        Ok(())
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
            if self[i].name != other[i].name {
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
    pub fn substitute(&self, variables: &BTreeMap<Ident, CRef<MType>>) -> Result<CRef<MType>> {
        match &*self.read()? {
            Constrained::Known { value, .. } => value.read()?.substitute(variables),
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
    pub variables: BTreeSet<Ident>,
    pub body: CRef<MType>,
}

impl SType {
    pub fn new_mono(body: CRef<MType>) -> CRef<SType> {
        mkcref(SType {
            variables: BTreeSet::new(),
            body,
        })
    }

    pub fn new_poly(body: CRef<MType>, variables: BTreeSet<Ident>) -> CRef<SType> {
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

pub type Params<TypeRef> = BTreeMap<Ident, TypedExpr<TypeRef>>;
pub type UnboundPaths = BTreeSet<Vec<Ident>>;

#[derive(Clone)]
pub enum SQLBody {
    Expr(sqlast::Expr),
    Query(sqlast::Query),
    Table(sqlast::TableFactor),
}

impl SQLBody {
    pub fn as_expr(&self) -> Result<sqlast::Expr> {
        Ok(match self {
            SQLBody::Expr(expr) => expr.clone(),
            SQLBody::Query(_) | SQLBody::Table(_) => {
                sqlast::Expr::Subquery(Box::new(sqlast::Query {
                    with: None,
                    body: Box::new(sqlast::SetExpr::Select(Box::new(sqlast::Select {
                        distinct: false,
                        top: None,
                        projection: vec![sqlast::SelectItem::ExprWithAlias {
                            expr: sqlast::Expr::Function(sqlast::Function {
                                name: sqlast::ObjectName(vec![sqlast::Ident::new("array_agg")]),
                                args: vec![sqlast::FunctionArg::Unnamed(
                                    sqlast::FunctionArgExpr::Expr(sqlast::Expr::Identifier(
                                        sqlast::Ident::new("subquery"),
                                    )),
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
                                name: sqlast::Ident::new("subquery"),
                                columns: Vec::new(),
                            }))?,
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
                    locks: Vec::new(),
                }))
            }
        })
    }

    pub fn as_statement(&self) -> Result<sqlast::Statement> {
        Ok(sqlast::Statement::Query(Box::new(self.as_query()?)))
    }

    // Returns a query that can be run as-is, but violates the array vs. result set invariants.
    //
    pub fn as_query(&self) -> Result<sqlast::Query> {
        Ok(match self {
            SQLBody::Expr(expr) => select_no_from(expr.clone(), None),
            SQLBody::Query(query) => query.clone(),
            SQLBody::Table(table) => select_star_from(table.clone()),
        })
    }

    // Assumes that if the SQL body is an expression, then it's of an array type.
    //
    pub fn as_table(&self, alias: Option<sqlast::TableAlias>) -> Result<sqlast::TableFactor> {
        Ok(match self {
            SQLBody::Expr(expr) => SQLBody::Query(select_from(
                // The result of the inner subquery is a relation of rows, each with a record field
                // called "value".  This extra subquery splats the fields of "value" onto the top
                // level of the query.
                vec![sqlast::SelectItem::QualifiedWildcard(
                    sqlast::ObjectName(vec![sqlast::Ident::new("value".to_string())]),
                    WildcardAdditionalOptions {
                        opt_exclude: None,
                        opt_except: None,
                        opt_rename: None,
                        opt_replace: None,
                    },
                )],
                vec![sqlast::TableWithJoins {
                    // In order to get around a binder bug in duckdb, we have to put the unnest
                    // call in a project list instead of as a table factor.  This results in a set
                    // of rows, each with a single record field.
                    //
                    relation: SQLBody::Query(select_no_from(
                        sqlast::Expr::Function(sqlast::Function {
                            name: sqlast::ObjectName(vec![sqlast::Ident::new(
                                "unnest".to_string(),
                            )]),
                            args: vec![sqlast::FunctionArg::Unnamed(
                                sqlast::FunctionArgExpr::Expr(expr.clone()),
                            )],
                            over: None,
                            distinct: false,
                            special: false,
                        }),
                        Some(sqlast::Ident::new("value".to_string())),
                    ))
                    .as_table(Some(sqlast::TableAlias {
                        name: sqlast::Ident::new("unnest".to_string()),
                        columns: Vec::new(),
                    }))?,
                    joins: Vec::new(),
                }],
            ))
            .as_table(alias)?,
            SQLBody::Query(query) => sqlast::TableFactor::Derived {
                lateral: false,
                subquery: Box::new(query.clone()),
                alias,
            },
            SQLBody::Table(table) => with_table_alias(table, alias),
        })
    }
}

impl fmt::Debug for SQLBody {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match &self {
                SQLBody::Expr(expr) => expr.to_string(),
                SQLBody::Query(query) => query.to_string(),
                SQLBody::Table(table) => table.to_string(),
            }
        )
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
            unbound: BTreeSet::from([sqlpath.iter().map(|i| i.value.clone().into()).collect()]),
        }
    }

    pub fn extend(&mut self, other: SQLNames<TypeRef>) {
        self.params.extend(other.params);
        self.unbound.extend(other.unbound);
    }
}

#[derive(Clone)]
pub struct SQLSnippet<TypeRef, SQLAst>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    // Descriptions of all externally defined names within the SQL body.
    //
    pub names: SQLNames<TypeRef>,

    // The AST representing the actual body of the SQL query or expression.
    //
    pub body: SQLAst,
}

impl<TypeRef: Clone + fmt::Debug + Send + Sync, SQLAst> SQLSnippet<TypeRef, SQLAst> {
    pub fn wrap(names: SQLNames<TypeRef>, body: SQLAst) -> CRef<super::inference::CWrap<Self>>
    where
        TypeRef: 'static,
        SQLAst: Clone + fmt::Debug + Send + Sync + 'static,
    {
        super::inference::cwrap(Self { names, body })
    }
}

impl<T: Clone + fmt::Debug + Send + Sync, SQLAst: fmt::Debug> fmt::Debug for SQLSnippet<T, SQLAst> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body = format!("{:?}", &self.body);
        f.debug_struct("SQL")
            .field("names", &self.names)
            .field("body", &body)
            .finish()
    }
}

pub type SQL<TypeRef> = SQLSnippet<TypeRef, SQLBody>;

#[derive(Debug, Clone)]
pub enum FnKind {
    SQLBuiltin(Ident),
    Native,
    Expr,
}

#[derive(Debug, Clone)]
pub enum FnBody<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    SQLBuiltin(Ident),
    Expr(Arc<Expr<TypeRef>>),
}

impl FnBody<CRef<MType>> {
    pub fn to_runtime_type(&self) -> runtime::error::Result<FnBody<Ref<Type>>> {
        Ok(match self {
            FnBody::SQLBuiltin(name) => FnBody::SQLBuiltin(name.clone()),
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
pub struct MaterializeExpr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    pub key: String,
    pub decl_name: Ident,
    pub expr: TypedExpr<TypeRef>,
    pub url: Option<Arc<ConnectionString>>,
    pub inlined: bool,
}

#[derive(Clone, Debug)]
pub enum Expr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    SQL(Arc<SQL<TypeRef>>, Option<Arc<ConnectionString>>),
    SchemaEntry(STypedExpr),
    Fn(FnExpr<TypeRef>),
    FnCall(FnCallExpr<TypeRef>),
    NativeFn(Ident),
    ContextRef(Ident),
    Connection(Arc<ConnectionString>),
    Materialize(MaterializeExpr<TypeRef>),
    UncompiledFn(ast::FnDef),
    Unknown,
}

impl Expr<CRef<MType>> {
    pub fn to_runtime_type(&self) -> runtime::error::Result<Expr<Ref<Type>>> {
        match self {
            Expr::SQL(e, url) => {
                let SQL { names, body } = e.as_ref();
                Ok(Expr::SQL(
                    Arc::new(SQL {
                        names: SQLNames {
                            params: names
                                .params
                                .iter()
                                .map(|(name, param)| Ok((name.clone(), param.to_runtime_type()?)))
                                .collect::<runtime::error::Result<_>>()?,
                            unbound: names.unbound.clone(),
                        },
                        body: body.clone(),
                    }),
                    url.clone(),
                ))
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
            Expr::SchemaEntry(e) => e.expr.must()?.read()?.to_runtime_type(),
            Expr::NativeFn(f) => Ok(Expr::NativeFn(f.clone())),
            Expr::ContextRef(r) => Ok(Expr::ContextRef(r.clone())),
            Expr::Connection(c) => Ok(Expr::Connection(c.clone())),
            Expr::Materialize(MaterializeExpr {
                key,
                expr,
                url: target_url,
                decl_name,
                inlined,
            }) => Ok(Expr::Materialize(MaterializeExpr {
                key: key.clone(),
                expr: expr.to_runtime_type()?,
                url: target_url.clone(),
                decl_name: decl_name.clone(),
                inlined: inlined.clone(),
            })),
            Expr::UncompiledFn(def) => Ok(Expr::UncompiledFn(def.clone())),
            Expr::Unknown => Ok(Expr::Unknown),
        }
    }

    pub async fn unwrap_schema_entry(self: &Expr<CRef<MType>>) -> Result<Expr<CRef<MType>>> {
        let mut ret = self.clone();
        loop {
            match ret {
                Expr::SchemaEntry(STypedExpr { expr, .. }) => ret = expr.await?.read()?.clone(),
                _ => return Ok(ret),
            }
        }
    }

    pub fn native_sql(sql: Arc<SQL<CRef<MType>>>) -> Expr<CRef<MType>> {
        Expr::SQL(sql, None)
    }
}

impl Constrainable for Type {}

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

    pub fn to_ctyped_expr(&self) -> runtime::error::Result<CTypedExpr> {
        Ok(CTypedExpr {
            type_: self.type_.must()?.read()?.body.clone(),
            expr: self.expr.clone(),
        })
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

pub fn mkref<T>(t: T) -> Ref<T> {
    Arc::new(RwLock::new(t))
}

#[derive(Clone, Debug)]
pub enum SchemaPath {
    Schema(ast::Path),
    Connection(Located<Arc<ConnectionString>>),
}

// Implement PartialEq, etc. so that we strip the locations away before doing comparisons
impl PartialEq for SchemaPath {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SchemaPath::Schema(p1), SchemaPath::Schema(p2)) => p1.to_idents() == p2.to_idents(),
            (SchemaPath::Connection(c1), SchemaPath::Connection(c2)) => {
                c1.get_url() == c2.get_url()
            }
            _ => false,
        }
    }
}
impl Eq for SchemaPath {}

impl PartialOrd for SchemaPath {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (SchemaPath::Schema(p1), SchemaPath::Schema(p2)) => {
                p1.to_idents().partial_cmp(&p2.to_idents())
            }
            (SchemaPath::Connection(c1), SchemaPath::Connection(c2)) => {
                c1.get_url().partial_cmp(c2.get_url())
            }
            (SchemaPath::Connection(..), SchemaPath::Schema(..)) => Some(Ordering::Less),
            (SchemaPath::Schema(..), SchemaPath::Connection(..)) => Some(Ordering::Greater),
        }
    }
}

impl Ord for SchemaPath {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub trait Entry: Clone {
    fn kind() -> &'static str;
    fn run_on_info(&self) -> Option<(SymbolKind, CRef<SType>)>;

    fn get_map(schema: &Schema) -> &DeclMap<Self>;
    fn get_conn_decl(
        _compiler: &super::Compiler,
        _schema: &mut ConnectionSchema,
        _ident: &Located<Ident>,
        _check_visibility: bool,
        _full_path: &ast::Path,
    ) -> Result<Option<Located<Decl<Self>>>> {
        Ok(None)
    }
}

pub type TypeEntry = CRef<MType>;
pub type ExprEntry = STypedExpr;

impl Entry for SchemaPath {
    fn run_on_info(&self) -> Option<(SymbolKind, CRef<SType>)> {
        None
    }
    fn get_map(schema: &Schema) -> &DeclMap<SchemaPath> {
        &schema.schema_decls
    }
    fn kind() -> &'static str {
        "schema"
    }
}
impl Entry for TypeEntry {
    fn run_on_info(&self) -> Option<(SymbolKind, CRef<SType>)> {
        Some((SymbolKind::Type, SType::new_mono(self.clone())))
    }
    fn get_map(schema: &Schema) -> &DeclMap<TypeEntry> {
        &schema.type_decls
    }
    fn kind() -> &'static str {
        "type"
    }
}
impl Entry for ExprEntry {
    fn run_on_info(&self) -> Option<(SymbolKind, CRef<SType>)> {
        Some((SymbolKind::Value, self.type_.clone()))
    }
    fn get_map(schema: &Schema) -> &DeclMap<ExprEntry> {
        &schema.expr_decls
    }
    fn kind() -> &'static str {
        "type"
    }
    fn get_conn_decl(
        compiler: &super::Compiler,
        schema: &mut ConnectionSchema,
        ident: &Located<Ident>,
        check_visibility: bool,
        full_path: &ast::Path,
    ) -> Result<Option<Located<Decl<Self>>>> {
        schema.get_decl(compiler, ident, check_visibility, full_path)
    }
}

#[derive(Clone, Debug)]
pub struct Decl<Entry: Clone> {
    pub public: bool,
    pub extern_: bool,
    pub is_arg: bool,
    pub name: Located<Ident>,
    pub value: Entry,
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

// We could potentially make this a trait instead, and make lookup_path generic on it. However, that
// makes it hard to dynamically return an Importer (we'd have to return an Arc<dyn Importer>), which
// doesn't seem worth it.
#[derive(Clone, Debug)]
pub enum Importer {
    Schema(SchemaRef),
    Connection(Ref<ConnectionSchema>),
}

impl Importer {
    pub fn location(&self) -> Result<SourceLocation> {
        Ok(match &self {
            Importer::Schema(schema) => SourceLocation::File(schema.read()?.file.clone()),
            Importer::Connection(url) => SourceLocation::File(format!("{:?}", url)),
        })
    }

    pub fn get_and_check<E: Entry>(
        &self,
        compiler: &super::Compiler,
        ident: &Located<Ident>,
        check_visibility: bool,
        full_path: &ast::Path,
    ) -> Result<Option<Located<Decl<E>>>> {
        Ok(match &self {
            Importer::Schema(schema) => schema
                .read()?
                .get_and_check(ident, check_visibility, full_path)?
                .cloned(),
            Importer::Connection(schema) => E::get_conn_decl(
                compiler,
                &mut *schema.write()?,
                ident,
                check_visibility,
                full_path,
            )?,
        })
    }

    pub fn as_schema(&self) -> Result<SchemaRef> {
        match &self {
            Importer::Schema(schema) => Ok(schema.clone()),
            Importer::Connection(..) => Err(CompileError::internal(
                SourceLocation::Unknown,
                "Connection, not a schema",
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ImportedSchema {
    pub args: Option<Vec<BTreeMap<String, TypedNameAndExpr<CRef<MType>>>>>,
    pub schema: Importer,
}

impl<T> Pretty for Located<T> {
    fn pretty(&self) -> String {
        self.location().pretty()
    }
}

pub type DeclMap<Entry> = BTreeMap<Ident, Located<Decl<Entry>>>;

#[derive(Clone, Debug)]
pub struct Schema {
    pub file: String,
    pub folder: Option<String>,
    pub parent_scope: Option<Ref<Schema>>,
    pub externs: BTreeMap<Ident, CRef<MType>>,

    pub schema_decls: DeclMap<SchemaPath>,
    pub type_decls: DeclMap<CRef<MType>>,
    pub expr_decls: DeclMap<STypedExpr>,

    pub imports: BTreeMap<SchemaPath, Ref<ImportedSchema>>,
    pub exprs: Vec<Located<CTypedExpr>>,
}

impl Schema {
    pub fn new(file: String, folder: Option<String>) -> Ref<Schema> {
        mkref(Schema {
            file,
            folder,
            parent_scope: None,
            externs: BTreeMap::new(),
            schema_decls: BTreeMap::new(),
            type_decls: BTreeMap::new(),
            expr_decls: BTreeMap::new(),
            imports: BTreeMap::new(),
            exprs: Vec::new(),
        })
    }

    pub fn get_decls<E: Entry>(&self) -> &DeclMap<E> {
        E::get_map(self)
    }

    pub fn get_and_check<E: Entry>(
        &self,
        ident: &Ident,
        check_visibility: bool,
        full_path: &ast::Path,
    ) -> Result<Option<&Located<Decl<E>>>> {
        match self.get_decls::<E>().get(ident) {
            Some(decl) => {
                if check_visibility && !decl.public {
                    return Err(CompileError::wrong_kind(
                        full_path.clone(),
                        "public",
                        E::kind(),
                    ));
                } else {
                    Ok(Some(decl))
                }
            }
            None => Ok(None),
        }
    }

    /// Creates a child schema with this schema as a parent scope. This is useful
    /// when entering scope contexts (e.g. functions, loops).
    pub fn derive(schema: Ref<Schema>) -> Result<Ref<Schema>> {
        let (file, folder) = {
            let schema = schema.read()?;
            (schema.file.clone(), schema.folder.clone())
        };
        let inner_schema = Schema::new(file, folder);
        inner_schema.write()?.parent_scope = Some(schema.clone());
        Ok(inner_schema)
    }
}

pub const SCHEMA_EXTENSIONS: &[&str] = &["qs"];
