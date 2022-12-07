use crate::compile::error::*;
use crate::compile::schema::{mkref, Ref};
use crate::runtime;
use snafu::prelude::*;

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub trait Constrainable: Clone + fmt::Debug + Send + Sync {
    fn unify(&self, other: &Self) -> Result<()> {
        Err(CompileError::internal(
            ErrorLocation::Unknown,
            format!(
                "{} cannot be unified:\n{:#?}\n{:#?}",
                std::any::type_name::<Self>(),
                self,
                other
            )
            .as_str(),
        ))
    }

    fn coerce(
        _op: &sqlparser::ast::BinaryOperator,
        left: &Ref<Self>,
        right: &Ref<Self>,
    ) -> Result<CRef<Self>>
    where
        Self: 'static,
    {
        left.unify(right)?;
        Ok(mkcref(left.read()?.clone()))
    }
}

pub trait Constraint<T: Constrainable>: FnMut(Ref<T>) -> Result<()> + Send + Sync {}
pub trait Then<T: Constrainable, R: Constrainable>:
    FnMut(Ref<T>) -> Result<CRef<R>> + Send + Sync
{
}

impl<T, F> Constraint<T> for F
where
    T: Constrainable,
    F: FnMut(Ref<T>) -> Result<()> + Send + Sync,
{
}

impl<T, R, F> Then<T, R> for F
where
    T: Constrainable,
    R: Constrainable,
    F: FnMut(Ref<T>) -> Result<CRef<R>> + Send + Sync,
{
}

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl Constrainable for String {}
impl<T> Constrainable for Vec<T> where T: Constrainable {}
impl<K, V> Constrainable for BTreeMap<K, V>
where
    K: Constrainable,
    V: Constrainable,
{
}
impl<T> Constrainable for Ref<T> where T: Constrainable {}

#[derive(Clone, Debug)]
pub struct CWrap<T>(Option<T>)
where
    T: Clone + fmt::Debug + Send + Sync;

impl<T> CWrap<T>
where
    T: Clone + fmt::Debug + Send + Sync + 'static,
{
    pub fn wrap(t: T) -> CRef<CWrap<T>> {
        mkcref(CWrap(Some(t)))
    }

    pub fn take(self: &mut Self) -> T {
        self.0.take().unwrap()
    }

    pub async fn clone_inner(cref: &CRef<CWrap<T>>) -> Result<T> {
        let resolved = cref.await?;
        let unlocked = resolved.read()?;
        Ok(unlocked.0.as_ref().unwrap().clone())
    }
}

pub fn cwrap<T: Clone + fmt::Debug + Send + Sync + 'static>(t: T) -> CRef<CWrap<T>> {
    CWrap::wrap(t)
}

pub fn cunwrap<T: Clone + fmt::Debug + Send + Sync + 'static>(t: Ref<CWrap<T>>) -> Result<T> {
    let mut cref = t.write()?;
    Ok(cref.take())
}

impl<T> Constrainable for CWrap<T> where T: Clone + fmt::Debug + Send + Sync {}

pub enum Constrained<T>
where
    T: Constrainable,
{
    Known(Ref<T>),
    Unknown {
        debug_names: Vec<String>,
        error: Option<CompileError>,
        constraints: Vec<Ref<dyn Constraint<T>>>,
    },
    Ref(CRef<T>),
}

impl<T: Constrainable> fmt::Debug for Constrained<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constrained::Known(t) => t.read().unwrap().fmt(f),
            Constrained::Unknown { debug_names, .. } => {
                f.write_str(format!("Unknown({:?})", debug_names).as_str())
            }
            Constrained::Ref(r) => r.fmt(f),
        }
    }
}

pub fn mkcref<T: 'static + Constrainable>(t: T) -> CRef<T> {
    CRef::new_known(mkref(t))
}

#[derive(Clone)]
pub struct CRef<T>(Ref<Constrained<T>>)
where
    T: Constrainable;

impl<T: Constrainable> fmt::Debug for CRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.read().unwrap().fmt(f)
    }
}

impl<T: 'static + Constrainable> CRef<T> {
    pub fn new_unknown(debug_name: &str) -> CRef<T> {
        CRef(mkref(Constrained::Unknown {
            debug_names: vec![debug_name.to_string()],
            error: None,
            constraints: Vec::new(),
        }))
    }

    pub fn new_error(e: CompileError) -> CRef<T> {
        CRef(mkref(Constrained::Unknown {
            debug_names: vec!["error".to_string()],
            error: Some(e),
            constraints: Vec::new(),
        }))
    }

    pub fn new_known(t: Ref<T>) -> CRef<T> {
        CRef(mkref(Constrained::Known(t)))
    }

    pub fn new_result(r: Result<Ref<T>>) -> CRef<T> {
        match r {
            Ok(t) => CRef::new_known(t),
            Err(e) => CRef::new_error(e),
        }
    }

    pub fn read(&self) -> Result<std::sync::RwLockReadGuard<'_, Constrained<T>>> {
        Ok(self.0.read()?)
    }

    pub fn write(&self) -> Result<std::sync::RwLockWriteGuard<'_, Constrained<T>>> {
        Ok(self.0.write()?)
    }

    pub fn must(&self) -> runtime::error::Result<Ref<T>> {
        match &*self.find().unwrap().0.read()? {
            Constrained::Known(t) => Ok(t.clone()),
            Constrained::Unknown { .. } => {
                runtime::error::fail!("Unknown type cannot exist at runtime ({:?})", self)
            }
            Constrained::Ref(_) => runtime::error::fail!("Canon value should never be a ref"),
        }
    }

    pub fn is_known(&self) -> Result<bool> {
        match &*self.find()?.read()? {
            Constrained::Unknown { .. } => Ok(false),
            Constrained::Known(_) => Ok(true),
            _ => Err(CompileError::internal(
                ErrorLocation::Unknown,
                "Canon value should never be a ref",
            )),
        }
    }

    pub fn constrain<F: 'static + Clone + Send + Sync + FnMut(Ref<T>) -> Result<()>>(
        &self,
        constraint: F,
    ) -> Result<()> {
        self.add_constraint(mkref(constraint.clone()))
    }

    pub fn then<R: 'static + Constrainable, F: 'static + Clone + Send + Sync + Then<T, R>>(
        &self,
        mut callback: F,
    ) -> Result<CRef<R>> {
        let slot = CRef::<R>::new_unknown("slot");
        let ret = slot.clone();
        let constraint = move |t: Ref<T>| -> Result<()> {
            slot.unify(&callback(t)?)?;
            Ok(())
        };
        self.constrain(constraint)?;

        Ok(ret)
    }

    pub fn unify(&self, other: &CRef<T>) -> Result<()> {
        let us = self.find()?;
        let them = other.find()?;

        if Arc::ptr_eq(&us.0, &them.0) {
            return Ok(());
        }

        if !us.is_known()? || !them.is_known()? {
            us.union(&them)?;
        } else {
            us.must()
                .context(RuntimeSnafu {
                    loc: ErrorLocation::Unknown,
                })?
                .read()?
                .unify(
                    &*them
                        .must()
                        .context(RuntimeSnafu {
                            loc: ErrorLocation::Unknown,
                        })?
                        .read()?,
                )?;
        }

        Ok(())
    }

    // Private methods
    //
    fn add_constraint(&self, constraint: Ref<dyn Constraint<T>>) -> Result<()> {
        match &mut *self.find()?.write()? {
            Constrained::Known(t) => {
                constraint.write()?(t.clone())?;
            }
            Constrained::Unknown { constraints, .. } => {
                constraints.push(constraint);
            }
            _ => {
                return Err(CompileError::internal(
                    ErrorLocation::Unknown,
                    "Canon value should never be a ref",
                ))
            }
        }

        Ok(())
    }

    fn find(&self) -> Result<CRef<T>> {
        let new = match &mut *self.write()? {
            Constrained::Ref(r) => r.find()?,
            _ => return Ok(self.clone()),
        };

        *self.0.write()? = Constrained::Ref(new.clone());
        return Ok(new);
    }

    fn union(&self, other: &CRef<T>) -> Result<()> {
        if !self.is_known()? && other.is_known()? {
            return other.union(self);
        }

        let us = self.find()?;
        let them = other.find()?;

        if !Arc::ptr_eq(&us.0, &them.0) {
            match &mut *them.write()? {
                Constrained::Unknown {
                    constraints,
                    debug_names,
                    ..
                } => {
                    for constraint in constraints.drain(..) {
                        us.add_constraint(constraint)?;
                    }
                    match &mut *us.write()? {
                        Constrained::Unknown {
                            debug_names: our_debug_names,
                            ..
                        } => {
                            for debug_name in debug_names.drain(..) {
                                our_debug_names.push(debug_name);
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }

            *them.write()? = Constrained::Ref(us.clone());
        }

        Ok(())
    }
}

macro_rules! ConstrainableImpl {
    () => {
        type Output = Result<Ref<T>>;
        fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match || -> Result<_> {
                let canon = self.find()?;
                let mut guard = canon.write()?;
                match &mut *guard {
                    Constrained::Known(t) => Ok(Poll::Ready(Ok(t.clone()))),
                    Constrained::Unknown { constraints, .. } => {
                        let waker = cx.waker().clone();
                        constraints.push(mkref(move |_: Ref<T>| {
                            let waker = waker.clone();
                            waker.wake();
                            Ok(())
                        }));
                        Ok(Poll::Pending)
                    }
                    _ => panic!("Canon value should never be a ref"),
                }
            }() {
                Ok(p) => p,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    };
}

impl<T: Constrainable + 'static> Future for &CRef<T> {
    ConstrainableImpl!();
}

impl<T: Constrainable + 'static> Future for CRef<T> {
    ConstrainableImpl!();
}
