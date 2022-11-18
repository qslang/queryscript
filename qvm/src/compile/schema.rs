use crate::ast;
use crate::compile::{
    error::{CompileError, Result},
    inference::{mkcref, CRef, Constrainable, Constrained},
};
use crate::runtime;
use crate::types::{AtomicType, Field, FnType, Type};
use sqlparser::ast as sqlast;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;

pub type Ident = ast::Ident;

#[derive(Debug, Clone)]
pub struct MFnType {
    pub args: Vec<MField>,
    pub ret: CRef<MType>,
}

#[derive(Debug, Clone)]
pub struct MField {
    pub name: String,
    pub type_: CRef<MType>,
    pub nullable: bool,
}

impl MField {
    pub fn new_nullable(name: String, type_: CRef<MType>) -> MField {
        MField {
            name,
            type_,
            nullable: true,
        }
    }
}

#[derive(Clone)]
pub enum MType {
    Atom(AtomicType),
    Record(Vec<MField>),
    List(CRef<MType>),
    Fn(MFnType),
    Name(String),
}

impl MType {
    pub fn new_unknown(debug_name: &str) -> CRef<MType> {
        CRef::new_unknown(debug_name)
    }

    pub fn to_runtime_type(&self) -> runtime::error::Result<Type> {
        match self {
            MType::Atom(a) => Ok(Type::Atom(a.clone())),
            MType::Record(fields) => Ok(Type::Record(
                fields
                    .iter()
                    .map(|f| {
                        Ok(Field {
                            name: f.name.clone(),
                            type_: f.type_.must()?.borrow().to_runtime_type()?,
                            nullable: f.nullable,
                        })
                    })
                    .collect::<runtime::error::Result<Vec<_>>>()?,
            )),
            MType::List(inner) => Ok(Type::List(Box::new(
                inner.must()?.borrow().to_runtime_type()?,
            ))),
            MType::Fn(MFnType { args, ret }) => Ok(Type::Fn(FnType {
                args: args
                    .iter()
                    .map(|a| {
                        Ok(Field {
                            name: a.name.clone(),
                            type_: a.type_.must()?.borrow().to_runtime_type()?,
                            nullable: a.nullable,
                        })
                    })
                    .collect::<runtime::error::Result<Vec<_>>>()?,
                ret: Box::new(ret.must()?.borrow().to_runtime_type()?),
            })),
            MType::Name(_) => {
                runtime::error::fail!("Unresolved type name cannot exist at runtime: {:?}", self)
            }
        }
    }

    pub fn substitute(&self, variables: &BTreeMap<String, CRef<MType>>) -> Result<CRef<MType>> {
        let type_ = match self {
            MType::Atom(a) => mkcref(MType::Atom(a.clone())),
            MType::Record(fields) => mkcref(MType::Record(
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
            )),
            MType::List(i) => mkcref(MType::List(i.substitute(variables)?)),
            MType::Fn(MFnType { args, ret }) => mkcref(MType::Fn(MFnType {
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
                .get(n)
                .ok_or_else(|| CompileError::no_such_entry(vec![n.clone()]))?
                .clone(),
        };

        Ok(type_)
    }
}

pub type CTypedExpr = TypedExpr<CRef<MType>>;

struct DebugMFields<'a>(&'a Vec<MField>);

impl<'a> fmt::Debug for DebugMFields<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for i in 0..self.0.len() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str(self.0[i].name.as_str())?;
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
            MType::Fn(func) => {
                f.write_str("λ ")?;
                DebugMFields(&func.args).fmt(f)?;
                f.write_str(" -> ")?;
                func.ret.fmt(f)?;
            }
            MType::Name(n) => n.fmt(f)?,
        }
        Ok(())
    }
}

impl Constrainable for MType {
    fn unify(&self, other: &MType) -> Result<()> {
        match self {
            MType::Atom(la) => match other {
                MType::Atom(ra) => {
                    if la != ra {
                        return Err(CompileError::wrong_type(self, other));
                    }
                }
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::Record(lfields) => match other {
                MType::Record(rfields) => lfields.unify(rfields)?,
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::List(linner) => match other {
                MType::List(rinner) => linner.unify(rinner)?,
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::Fn(MFnType {
                args: largs,
                ret: lret,
            }) => match other {
                MType::Fn(MFnType {
                    args: rargs,
                    ret: rret,
                }) => {
                    largs.unify(rargs)?;
                    lret.unify(rret)?;
                }
                _ => return Err(CompileError::wrong_type(self, other)),
            },
            MType::Name(name) => {
                return Err(CompileError::internal(
                    format!("Encountered free type variable: {}", name).as_str(),
                ))
            }
        }

        Ok(())
    }
}

impl Constrainable for Vec<MField> {
    fn unify(&self, other: &Vec<MField>) -> Result<()> {
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
    pub fn substitute(&self, variables: &BTreeMap<String, CRef<MType>>) -> Result<CRef<MType>> {
        match &*self.borrow() {
            Constrained::Known(t) => t.borrow().substitute(variables),
            Constrained::Unknown { .. } => Ok(self.clone()),
            Constrained::Ref(r) => r.substitute(variables),
        }
    }
}

pub type Ref<T> = Rc<RefCell<T>>;

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

impl Constrainable for SType {
    fn unify(&self, other: &SType) -> Result<()> {
        return Err(CompileError::internal(
            format!(
                "Polymorphic types cannot be unified:\n{:#?}\n{:#?}",
                self, other
            )
            .as_str(),
        ));
    }
}

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

pub type Params<TypeRef> = BTreeMap<ast::Ident, TypedExpr<TypeRef>>;

#[derive(Clone)]
pub struct SQLExpr<TypeRef> {
    pub params: Params<TypeRef>,
    pub expr: sqlast::Expr,
}

impl<T: fmt::Debug> fmt::Debug for SQLExpr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SQLExpr")
            .field("params", &self.params)
            .field("expr", &self.expr.to_string())
            .finish()
    }
}

#[derive(Clone)]
pub struct SQLQuery<TypeRef> {
    pub params: Params<TypeRef>,
    pub query: sqlast::Query,
}

impl<T: fmt::Debug> fmt::Debug for SQLQuery<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SQLQuery")
            .field("params", &self.params)
            .field("query", &self.query.to_string())
            .finish()
    }
}

#[derive(Clone)]
pub struct FnExpr<TypeRef> {
    pub inner_schema: Ref<Schema>,
    pub body: Rc<Expr<TypeRef>>,
}

impl<TypeRef: fmt::Debug> fmt::Debug for FnExpr<TypeRef> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(f.debug_struct("FnExpr")
            .field("body", &self.body)
            .finish_non_exhaustive()?)
    }
}

#[derive(Clone, Debug)]
pub struct FnCallExpr<TypeRef> {
    pub func: Rc<TypedExpr<TypeRef>>,
    pub args: Vec<TypedExpr<TypeRef>>,
}

#[derive(Clone)]
pub struct SchemaEntryExpr {
    pub debug_name: String,
    pub entry: SchemaEntry,
}

impl fmt::Debug for SchemaEntryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaEntryExpr")
            .field("debug_name", &self.debug_name)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub enum Expr<TypeRef> {
    SQLQuery(Rc<SQLQuery<TypeRef>>),
    SQLExpr(Rc<SQLExpr<TypeRef>>),
    SchemaEntry(SchemaEntryExpr),
    Fn(FnExpr<TypeRef>),
    FnCall(FnCallExpr<TypeRef>),
    NativeFn(String),
    Unknown,
}

impl Expr<CRef<MType>> {
    pub fn to_runtime_type(&self) -> runtime::error::Result<Expr<Ref<Type>>> {
        match self {
            Expr::SQLQuery(q) => {
                let SQLQuery { params, query } = q.as_ref();
                Ok(Expr::SQLQuery(Rc::new(SQLQuery {
                    params: params
                        .iter()
                        .map(|(name, param)| Ok((name.clone(), param.to_runtime_type()?)))
                        .collect::<runtime::error::Result<_>>()?,
                    query: query.clone(),
                })))
            }
            Expr::SQLExpr(e) => {
                let SQLExpr { params, expr } = e.as_ref();
                Ok(Expr::SQLExpr(Rc::new(SQLExpr {
                    params: params
                        .iter()
                        .map(|(name, param)| Ok((name.clone(), param.to_runtime_type()?)))
                        .collect::<runtime::error::Result<_>>()?,
                    expr: expr.clone(),
                })))
            }
            Expr::Fn(FnExpr { inner_schema, body }) => Ok(Expr::Fn(FnExpr {
                inner_schema: inner_schema.clone(),
                body: Rc::new(body.to_runtime_type()?),
            })),
            Expr::FnCall(FnCallExpr { func, args }) => Ok(Expr::FnCall(FnCallExpr {
                func: Rc::new(func.to_runtime_type()?),
                args: args
                    .iter()
                    .map(|a| Ok(a.to_runtime_type()?))
                    .collect::<runtime::error::Result<_>>()?,
            })),
            Expr::SchemaEntry(e) => Ok(Expr::SchemaEntry(e.clone())),
            Expr::NativeFn(f) => Ok(Expr::NativeFn(f.clone())),
            Expr::Unknown => Ok(Expr::Unknown),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TypedNameAndSQLExpr<TypeRef> {
    pub name: String,
    pub type_: TypeRef,
    pub expr: Rc<SQLExpr<TypeRef>>,
}

#[derive(Clone, Debug)]
pub struct TypedSQLExpr<TypeRef> {
    pub type_: TypeRef,
    pub expr: Rc<SQLExpr<TypeRef>>,
}

#[derive(Clone, Debug)]
pub struct TypedSQLQuery<TypeRef> {
    pub type_: TypeRef,
    pub query: Rc<SQLQuery<TypeRef>>,
}

#[derive(Clone, Debug)]
pub struct TypedExpr<TypeRef> {
    pub type_: TypeRef,
    pub expr: Rc<Expr<TypeRef>>,
}

impl TypedExpr<CRef<MType>> {
    pub fn to_runtime_type(&self) -> runtime::error::Result<TypedExpr<Ref<Type>>> {
        Ok(TypedExpr::<Ref<Type>> {
            type_: mkref(self.type_.must()?.borrow().to_runtime_type()?),
            expr: Rc::new(self.expr.to_runtime_type()?),
        })
    }
}

impl Constrainable for TypedExpr<CRef<MType>> {
    fn unify(&self, other: &TypedExpr<CRef<MType>>) -> Result<()> {
        return Err(CompileError::internal(
            format!("Typed exprs cannot be unified:\n{:#?}\n{:#?}", self, other).as_str(),
        ));
    }
}

#[derive(Clone)]
pub struct STypedExpr {
    pub type_: CRef<SType>,
    pub expr: Rc<Expr<CRef<MType>>>,
}

impl STypedExpr {
    pub fn new_unknown(debug_name: &str) -> CRef<STypedExpr> {
        CRef::new_unknown(debug_name)
    }

    pub fn to_runtime_type(&self) -> runtime::error::Result<TypedExpr<Ref<Type>>> {
        Ok(TypedExpr::<Ref<Type>> {
            type_: mkref(
                self.type_
                    .must()?
                    .borrow()
                    .body
                    .must()?
                    .borrow()
                    .to_runtime_type()?,
            ),
            expr: Rc::new(self.expr.to_runtime_type()?),
        })
    }
}

impl fmt::Debug for STypedExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("STypedExpr")
            .field("type_", &*self.type_.borrow())
            .field("expr", &self.expr)
            .finish()
    }
}

impl Constrainable for STypedExpr {
    fn unify(&self, other: &STypedExpr) -> Result<()> {
        return Err(CompileError::internal(
            format!("Declarations cannot be unified:\n{:#?}\n{:#?}", self, other).as_str(),
        ));
    }
}

#[derive(Clone, Debug)]
pub enum SchemaEntry {
    Schema(ast::Path),
    Type(CRef<MType>),
    Expr(CRef<STypedExpr>),
}

pub fn mkref<T>(t: T) -> Ref<T> {
    Rc::new(RefCell::new(t))
}

#[derive(Clone, Debug)]
pub struct Decl {
    pub public: bool,
    pub extern_: bool,
    pub name: String,
    pub value: SchemaEntry,
}

#[derive(Clone, Debug)]
pub struct TypedNameAndExpr<TypeRef> {
    pub name: String,
    pub type_: TypeRef,
    pub expr: Rc<Expr<TypeRef>>,
}

pub type SchemaRef = Ref<Schema>;

#[derive(Clone, Debug)]
pub struct TypedName<TypeRef> {
    pub name: String,
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
    pub folder: Option<String>,
    pub parent_scope: Option<Ref<Schema>>,
    pub next_placeholder: usize,
    pub externs: BTreeMap<String, CRef<MType>>,
    pub decls: BTreeMap<String, Decl>,
    pub imports: BTreeMap<ast::Path, Ref<ImportedSchema>>,
    pub queries: Vec<TypedExpr<CRef<MType>>>,
}

impl Schema {
    pub fn new(folder: Option<String>) -> Ref<Schema> {
        mkref(Schema {
            folder,
            parent_scope: None,
            next_placeholder: 1,
            externs: BTreeMap::new(),
            decls: BTreeMap::new(),
            imports: BTreeMap::new(),
            queries: Vec::new(),
        })
    }

    pub fn new_global_schema() -> Ref<Schema> {
        let ret = Schema::new(None);
        ret.borrow_mut().decls = BTreeMap::from([
            (
                "number".to_string(),
                Decl {
                    public: true,
                    extern_: false,
                    name: "number".to_string(),
                    value: SchemaEntry::Type(mkcref(MType::Atom(AtomicType::Float64))),
                },
            ),
            (
                "string".to_string(),
                Decl {
                    public: true,
                    extern_: false,
                    name: "string".to_string(),
                    value: SchemaEntry::Type(mkcref(MType::Atom(AtomicType::Utf8))),
                },
            ),
            (
                "bool".to_string(),
                Decl {
                    public: true,
                    extern_: false,
                    name: "string".to_string(),
                    value: SchemaEntry::Type(mkcref(MType::Atom(AtomicType::Boolean))),
                },
            ),
            (
                "null".to_string(),
                Decl {
                    public: true,
                    extern_: false,
                    name: "string".to_string(),
                    value: SchemaEntry::Type(mkcref(MType::Atom(AtomicType::Null))),
                },
            ),
            (
                "load_json".to_string(),
                Decl {
                    public: true,
                    extern_: false,
                    name: "load_json".to_string(),
                    value: SchemaEntry::Expr(mkcref(STypedExpr {
                        type_: mkcref(SType {
                            variables: BTreeSet::from(["R".to_string()]),
                            body: mkcref(MType::Fn(MFnType {
                                args: vec![MField {
                                    name: "file".to_string(),
                                    type_: mkcref(MType::Atom(AtomicType::Utf8)),
                                    nullable: false,
                                }],
                                ret: mkcref(MType::List(mkcref(MType::Name("R".to_string())))),
                            })),
                        }),
                        expr: Rc::new(Expr::NativeFn("load_json".to_string())),
                    })),
                },
            ),
        ]);

        ret
    }
}
