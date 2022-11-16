use crate::compile::error::*;
use crate::compile::schema::Ref;
use std::rc::Rc;

pub trait Constrainable: Clone {}

pub type Constraint<T> = fn(Ref<T>) -> Result<()>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Constrained<T>
where
    T: Constrainable,
{
    Known(Ref<T>),
    Unknown { constraints: Vec<Constraint<T>> },
    Ref(CRef<T>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CRef<T>(Ref<Constrained<T>>)
where
    T: Constrainable;

impl<T: Constrainable> CRef<T> {
    pub fn borrow(&self) -> std::cell::Ref<Constrained<T>> {
        self.0.borrow()
    }

    pub fn borrow_mut(&self) -> std::cell::RefMut<Constrained<T>> {
        self.0.borrow_mut()
    }

    pub fn is_known(&mut self) -> Result<bool> {
        match &*self.find().borrow() {
            Constrained::Unknown { .. } => Ok(false),
            Constrained::Known(_) => Ok(true),
            _ => Err(CompileError::internal("Canon value should never be a ref")),
        }
    }

    pub fn then(&mut self, constraint: Constraint<T>) -> Result<CRef<T>> {
        match &mut *self.find().borrow_mut() {
            Constrained::Known(t) => {
                constraint(t.clone())?;
            }
            Constrained::Unknown { constraints } => {
                constraints.push(constraint);
            }
            _ => return Err(CompileError::internal("Canon value should never be a ref")),
        }

        Ok(self.clone())
    }

    pub fn find(&mut self) -> CRef<T> {
        let new = match &mut *self.borrow_mut() {
            Constrained::Ref(r) => r.find(),
            _ => return self.clone(),
        };

        self.0 = new.0.clone();
        return new;
    }

    pub fn union(&mut self, other: &mut CRef<T>) -> Result<()> {
        if !self.is_known()? && other.is_known()? {
            return other.union(self);
        }

        let mut us = self.find();
        let them = other.find();

        if !Rc::ptr_eq(&us.0, &them.0) {
            match &mut *them.borrow_mut() {
                Constrained::Unknown { constraints } => {
                    for constraint in constraints.drain(..) {
                        us.then(constraint)?;
                    }
                }
                _ => {}
            }

            *them.borrow_mut() = Constrained::Ref(us.clone());
        }

        Ok(())
    }
}
