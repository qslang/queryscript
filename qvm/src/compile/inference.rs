use crate::compile::error::*;
use crate::compile::schema::{mkref, Ref};
use crate::runtime;
use std::fmt;
use std::rc::Rc;

pub trait Constrainable: Clone + fmt::Debug {
    fn unify(&self, other: &Self) -> Result<()>;
}

pub trait Constraint<T: Constrainable>: FnMut(Ref<T>) -> Result<()> {}
pub trait Then<T: Constrainable, R: Constrainable>: FnMut(Ref<T>) -> Result<CRef<R>> {}

impl<T, F> Constraint<T> for F
where
    T: Constrainable,
    F: FnMut(Ref<T>) -> Result<()>,
{
}

impl<T, R, F> Then<T, R> for F
where
    T: Constrainable,
    R: Constrainable,
    F: FnMut(Ref<T>) -> Result<CRef<R>>,
{
}

pub enum Constrained<T>
where
    T: Constrainable,
{
    Known(Ref<T>),
    Unknown {
        debug_name: String,
        constraints: Vec<Box<dyn Constraint<T>>>,
    },
    Ref(CRef<T>),
}

impl<T: Constrainable> fmt::Debug for Constrained<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constrained::Known(t) => t.borrow().fmt(f),
            Constrained::Unknown { debug_name, .. } => {
                f.write_str(format!("Unknown({:?})", debug_name).as_str())
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
        self.0.borrow().fmt(f)
    }
}

impl<T: 'static + Constrainable> CRef<T> {
    pub fn new_unknown(debug_name: &str) -> CRef<T> {
        CRef(mkref(Constrained::Unknown {
            debug_name: debug_name.to_string(),
            constraints: Vec::new(),
        }))
    }

    pub fn new_known(t: Ref<T>) -> CRef<T> {
        CRef(mkref(Constrained::Known(t)))
    }

    pub fn borrow(&self) -> std::cell::Ref<Constrained<T>> {
        self.0.borrow()
    }

    pub fn borrow_mut(&self) -> std::cell::RefMut<Constrained<T>> {
        self.0.borrow_mut()
    }

    pub fn must(&self) -> runtime::error::Result<Ref<T>> {
        match &*self.find().borrow() {
            Constrained::Known(t) => Ok(t.clone()),
            Constrained::Unknown { .. } => runtime::error::fail!("Unknown cannot exist at runtime"),
            Constrained::Ref(_) => runtime::error::fail!("Canon value should never be a ref"),
        }
    }

    pub fn is_known(&self) -> Result<bool> {
        match &*self.find().borrow() {
            Constrained::Unknown { .. } => Ok(false),
            Constrained::Known(_) => Ok(true),
            _ => Err(CompileError::internal("Canon value should never be a ref")),
        }
    }

    pub fn constrain<F: 'static + Clone + FnMut(Ref<T>) -> Result<()>>(
        &self,
        constraint: F,
    ) -> Result<()> {
        self.add_constraint(Box::new(constraint.clone()))
    }

    pub fn then<R: 'static + Constrainable, F: 'static + Clone + Then<T, R>>(
        &self,
        mut callback: F,
    ) -> Result<CRef<R>> {
        let slot = CRef::<R>::new_unknown("slot");
        let ret = CRef::<R>::new_unknown("then");
        ret.unify(&slot)?;
        let constraint = move |t: Ref<T>| -> Result<()> {
            slot.unify(&callback(t)?)?;
            Ok(())
        };
        self.constrain(constraint)?;

        Ok(ret)
    }

    pub fn unify(&self, other: &CRef<T>) -> Result<()> {
        let us = self.find();
        let them = other.find();

        if Rc::ptr_eq(&us.0, &them.0) {
            return Ok(());
        }

        if !us.is_known()? || !them.is_known()? {
            us.union(&them)?;
        } else {
            us.must()?.borrow().unify(&*them.must()?.borrow())?;
        }

        Ok(())
    }

    // Private methods
    //
    fn add_constraint(&self, mut constraint: Box<dyn Constraint<T>>) -> Result<()> {
        match &mut *self.find().borrow_mut() {
            Constrained::Known(t) => {
                constraint(t.clone())?;
            }
            Constrained::Unknown { constraints, .. } => {
                constraints.push(constraint);
            }
            _ => return Err(CompileError::internal("Canon value should never be a ref")),
        }

        Ok(())
    }

    fn find(&self) -> CRef<T> {
        let new = match &mut *self.borrow_mut() {
            Constrained::Ref(r) => r.find(),
            _ => return self.clone(),
        };

        *self.0.borrow_mut() = Constrained::Ref(new.clone());
        return new;
    }

    fn union(&self, other: &CRef<T>) -> Result<()> {
        if !self.is_known()? && other.is_known()? {
            return other.union(self);
        }

        let us = self.find();
        let them = other.find();

        if !Rc::ptr_eq(&us.0, &them.0) {
            match &mut *them.borrow_mut() {
                Constrained::Unknown { constraints, .. } => {
                    for constraint in constraints.drain(..) {
                        us.add_constraint(constraint)?;
                    }
                }
                _ => {}
            }

            *them.borrow_mut() = Constrained::Ref(us.clone());
        }

        Ok(())
    }
}
