use crate::ast::Pretty;
pub use arrow::datatypes::DataType as ArrowDataType;
use colored::*;
use snafu::prelude::*;
use sqlparser::ast as sqlast;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::ast;
use crate::ast::SourceLocation;
use crate::compile::{
    coerce::{coerce_types, CoerceOp},
    error::*,
    inference::{mkcref, Constrainable, Constrained},
    traverse::{Visit, Visitor},
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
pub struct MListType {
    pub loc: SourceLocation,
    pub inner: CRef<MType>,
}

#[derive(Debug, Clone)]
pub struct MRecordType {
    pub loc: SourceLocation,
    pub fields: Vec<MField>,
}

#[derive(Debug, Clone)]
pub struct MFnType {
    pub loc: SourceLocation,
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

#[derive(Clone)]
pub enum MType {
    Atom(SourceLocation, AtomicType),
    Record(MRecordType),
    List(MListType),
    Fn(MFnType),
    Name(Ident),
}

impl MType {
    pub fn new_unknown(debug_name: &str) -> CRef<MType> {
        CRef::new_unknown(debug_name)
    }

    pub fn to_runtime_type(&self) -> runtime::error::Result<Type> {
        match self {
            MType::Atom(_, a) => Ok(Type::Atom(a.clone())),
            MType::Record(MRecordType { fields, .. }) => Ok(Type::Record(
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
            MType::List(MListType { inner, .. }) => Ok(Type::List(Box::new(
                inner.must()?.read()?.to_runtime_type()?,
            ))),
            MType::Fn(MFnType { args, ret, .. }) => Ok(Type::Fn(FnType {
                args: args
                    .iter()
                    .map(|a| {
                        Ok(Field {
                            name: a.name.value.clone(),
                            type_: a.type_.must()?.read()?.to_runtime_type()?,
                            nullable: a.nullable,
                        })
                    })
                    .collect::<runtime::error::Result<Vec<_>>>()?,
                ret: Box::new(ret.must()?.read()?.to_runtime_type()?),
            })),
            MType::Name { .. } => {
                runtime::error::fail!("Unresolved type name cannot exist at runtime: {:?}", self)
            }
        }
    }

    pub fn from_runtime_type(type_: &Type) -> Result<MType> {
        match type_ {
            Type::Atom(a) => Ok(MType::Atom(SourceLocation::Unknown, a.clone())),
            Type::Record(fields) => Ok(MType::Record(MRecordType {
                loc: SourceLocation::Unknown,
                fields: fields
                    .iter()
                    .map(|f| {
                        Ok(MField {
                            name: Ident::without_location(f.name.clone()),
                            type_: mkcref(MType::from_runtime_type(&f.type_)?),
                            nullable: f.nullable,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })),
            Type::List(inner) => Ok(MType::List(MListType {
                loc: SourceLocation::Unknown,
                inner: mkcref(MType::from_runtime_type(&inner)?),
            })),
            Type::Fn(FnType { args, ret }) => Ok(MType::Fn(MFnType {
                loc: SourceLocation::Unknown,
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
            })),
        }
    }

    pub fn substitute(&self, variables: &BTreeMap<String, CRef<MType>>) -> Result<CRef<MType>> {
        let type_ = match self {
            MType::Atom(loc, a) => mkcref(MType::Atom(loc.clone(), a.clone())),
            MType::Record(MRecordType { loc, fields }) => mkcref(MType::Record(MRecordType {
                loc: loc.clone(),
                fields: fields
                    .iter()
                    .map(|f| {
                        Ok(MField {
                            name: f.name.clone(),
                            type_: f.type_.substitute(variables)?,
                            nullable: f.nullable,
                        })
                    })
                    .collect::<Result<_>>()?,
            })),
            MType::List(MListType { loc, inner }) => mkcref(MType::List(MListType {
                loc: loc.clone(),
                inner: inner.substitute(variables)?,
            })),
            MType::Fn(MFnType { loc, args, ret }) => mkcref(MType::Fn(MFnType {
                loc: loc.clone(),
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
            })),
            MType::Name(n) => variables
                .get(&n.value)
                .ok_or_else(|| CompileError::no_such_entry(vec![n.clone()]))?
                .clone(),
        };

        Ok(type_)
    }

    pub fn location(&self) -> SourceLocation {
        match self {
            MType::Atom(loc, _) => loc.clone(),
            MType::Record(t) => t.loc.clone(),
            MType::List(t) => t.loc.clone(),
            MType::Fn(t) => t.loc.clone(),
            MType::Name(t) => t.loc.clone(),
        }
    }
}

impl Pretty for MType {
    fn pretty(&self) -> String {
        format!("{:?}", self).white().bold().to_string()
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
            MType::Atom(_, atom) => atom.fmt(f)?,
            MType::Record(MRecordType { fields, .. }) => DebugMFields(fields).fmt(f)?,
            MType::List(MListType { inner, .. }) => {
                f.write_str("[")?;
                inner.fmt(f)?;
                f.write_str("]")?;
            }
            MType::Fn(MFnType { args, ret, .. }) => {
                f.write_str("λ ")?;
                DebugMFields(&args).fmt(f)?;
                f.write_str(" -> ")?;
                ret.fmt(f)?;
            }
            MType::Name(n) => n.value.fmt(f)?,
        }
        Ok(())
    }
}

impl Constrainable for MType {
    fn unify(&self, other: &MType) -> Result<()> {
        match self {
            MType::Atom(_, la) => match other {
                MType::Atom(_, ra) => {
                    if la != ra {
                        return Err(CompileError::wrong_type(self, other));
                    }
                }
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::Record(rtype) => match other {
                MType::Record(ltype) => ltype.unify(rtype)?,
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::List(MListType { inner: linner, .. }) => match other {
                MType::List(MListType { inner: rinner, .. }) => linner.unify(rinner)?,
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::Fn(MFnType {
                args: largs,
                ret: lret,
                loc: lloc,
            }) => match other {
                MType::Fn(MFnType {
                    args: rargs,
                    ret: rret,
                    loc: rloc,
                }) => {
                    MRecordType {
                        loc: lloc.clone(),
                        fields: largs.clone(),
                    }
                    .unify(&MRecordType {
                        loc: rloc.clone(),
                        fields: rargs.clone(),
                    })?;
                    lret.unify(rret)?;
                }
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::Name(name) => {
                return Err(CompileError::internal(
                    name.loc.clone(),
                    format!("Encountered free type variable: {}", name.value).as_str(),
                ))
            }
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

impl Constrainable for MRecordType {
    fn unify(&self, other: &MRecordType) -> Result<()> {
        let err = || {
            CompileError::wrong_type(&MType::Record(self.clone()), &MType::Record(other.clone()))
        };
        if self.fields.len() != other.fields.len() {
            return Err(err());
        }

        for i in 0..self.fields.len() {
            if self.fields[i].name.value != other.fields[i].name.value {
                return Err(err());
            }

            if self.fields[i].nullable != other.fields[i].nullable {
                return Err(err());
            }

            self.fields[i].type_.unify(&other.fields[i].type_)?;
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

#[derive(Clone)]
pub enum SQLBody {
    Expr(sqlast::Expr),
    Query(sqlast::Query),
}

impl SQLBody {
    pub fn as_expr(&self) -> runtime::error::Result<sqlast::Expr> {
        match self {
            SQLBody::Expr(expr) => Ok(expr.clone()),
            SQLBody::Query(_) => Err(runtime::error::RuntimeError::unimplemented(
                "Scalar subqueries",
            )),
        }
    }

    pub fn as_query(&self) -> runtime::error::Result<sqlast::Query> {
        match self {
            SQLBody::Expr(expr) => Ok(sqlast::Query {
                with: None,
                body: Box::new(sqlast::SetExpr::Select(Box::new(sqlast::Select {
                    distinct: false,
                    top: None,
                    projection: vec![sqlast::SelectItem::ExprWithAlias {
                        expr: expr.clone(),
                        alias: sqlast::Ident {
                            value: "value".to_string(),
                            quote_style: None,
                        },
                    }],
                    into: None,
                    from: Vec::new(),
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
            }),
            SQLBody::Query(query) => Ok(query.clone()),
        }
    }
}

#[derive(Clone)]
pub struct SQL<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    pub params: Params<TypeRef>,
    pub body: SQLBody,
}

impl<T: Clone + fmt::Debug + Send + Sync> fmt::Debug for SQL<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body = match &self.body {
            SQLBody::Expr(expr) => expr.to_string(),
            SQLBody::Query(query) => query.to_string(),
        };
        f.debug_struct("SQL")
            .field("params", &self.params)
            .field("body", &body)
            .finish()
    }
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
                let SQL { params, body } = e.as_ref();
                Ok(Expr::SQL(Arc::new(SQL {
                    params: params
                        .iter()
                        .map(|(name, param)| Ok((name.clone(), param.to_runtime_type()?)))
                        .collect::<runtime::error::Result<_>>()?,
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
}

impl<TypeRef, V: Visitor> Visit<V> for Expr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    fn visit(&self, visitor: &V) -> Self {
        match self {
            Expr::SQL(e) => {
                let SQL { params, body } = e.as_ref();
                Expr::SQL(Arc::new(SQL {
                    params: params
                        .iter()
                        .map(|(name, param)| (name.clone(), param.visit(visitor)))
                        .collect(),
                    body: match body {
                        SQLBody::Expr(expr) => SQLBody::Expr(expr.visit(visitor)),
                        SQLBody::Query(query) => SQLBody::Query(query.visit(visitor)),
                    },
                }))
            }
            Expr::Fn(FnExpr { inner_schema, body }) => Expr::Fn(FnExpr {
                inner_schema: inner_schema.clone(),
                body: match body {
                    FnBody::SQLBuiltin => FnBody::SQLBuiltin,
                    FnBody::Expr(expr) => FnBody::Expr(Arc::new(expr.visit(visitor))),
                },
            }),
            Expr::FnCall(FnCallExpr {
                func,
                args,
                ctx_folder,
            }) => Expr::FnCall(FnCallExpr {
                func: Arc::new(func.visit(visitor)),
                args: args.iter().map(|a| a.visit(visitor)).collect(),
                ctx_folder: ctx_folder.clone(),
            }),
            Expr::SchemaEntry(e) => Expr::SchemaEntry(e.clone()),
            Expr::NativeFn(f) => Expr::NativeFn(f.clone()),
            Expr::ContextRef(r) => Expr::ContextRef(r.clone()),
            Expr::Unknown => Expr::Unknown,
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

impl<TypeRef, V: Visitor> Visit<V> for TypedExpr<TypeRef>
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    fn visit(&self, visitor: &V) -> Self {
        TypedExpr {
            type_: self.type_.clone(),
            expr: Arc::new(self.expr.visit(visitor)),
        }
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

// XXX We should implement a cheaper Eq / PartialEq over Schema, because it's
// currently used to check if two types are equal.
#[derive(Clone, Debug)]
pub struct Schema {
    pub file: String,
    pub folder: Option<String>,
    pub parent_scope: Option<Ref<Schema>>,
    pub externs: BTreeMap<String, CRef<MType>>,
    pub decls: BTreeMap<String, Decl>,
    pub imports: BTreeMap<Vec<String>, Ref<ImportedSchema>>,
    pub exprs: Vec<CTypedExpr>,
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
