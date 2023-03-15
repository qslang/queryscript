use sqlparser::ast as sqlast;

use std::collections::{btree_map, BTreeMap};
use std::sync::Arc;

use crate::ast::{SourceLocation, ToSqlIdent};

use crate::compile::casync;
use crate::compile::compile::Compiler;
use crate::compile::error::*;
use crate::compile::inference::*;
use crate::compile::schema::*;
use crate::compile::sql::{combine_crefs, get_rowtype, CSQLNames, CTypedSQL};
use crate::compile::util::InsertionOrderMap;

#[derive(Clone, Debug)]
pub struct FieldMatch {
    pub field: Located<Ident>,
    pub type_: Option<CRef<MType>>,
    pub sql: SQL<CRef<MType>>,
}

impl FieldMatch {
    pub fn from_field(
        relation: Located<Ident>,
        field: Located<Ident>,
        type_: CRef<MType>,
    ) -> FieldMatch {
        let sqlpath = vec![relation.to_sqlident(), field.to_sqlident()];
        FieldMatch {
            field,
            type_: Some(type_),
            sql: SQL {
                names: CSQLNames::from_unbound(&sqlpath),
                body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(sqlpath)),
            },
        }
    }
}
impl Constrainable for FieldMatch {}

#[derive(Clone, Debug)]
pub struct SQLScope {
    parent: Option<Ref<SQLScope>>,
    relations: BTreeMap<Ident, (CRef<MType>, SourceLocation)>,
    pub projection_terms: BTreeMap<Ident, CTypedSQL>,
}

impl SQLScope {
    pub fn new(parent: Option<Ref<SQLScope>>) -> Ref<SQLScope> {
        mkref(SQLScope {
            parent,
            relations: BTreeMap::new(),
            projection_terms: BTreeMap::new(),
        })
    }

    pub fn empty() -> Ref<SQLScope> {
        Self::new(None)
    }

    pub fn deep_copy(scope: &Ref<SQLScope>) -> Result<Ref<SQLScope>> {
        Ok(mkref(scope.read()?.clone()))
    }

    pub fn get_relation(&self, name: &Ident) -> Result<Option<(CRef<MType>, SourceLocation)>> {
        Ok(match self.relations.get(name) {
            Some((t, loc)) => Some((t.clone(), loc.clone())),
            None => match &self.parent {
                Some(p) => p.read()?.get_relation(name)?,
                None => None,
            },
        })
    }

    pub fn get_available_references(
        &self,
        compiler: Compiler,
        loc: &SourceLocation,
        relation: Option<Ident>,
    ) -> Result<CRef<AvailableReferences>> {
        let crelations = combine_crefs(
            self.relations
                .iter()
                .filter(|(n, _)| match &relation {
                    Some(r) => *n == r,
                    None => true,
                })
                .map(|(n, (te, loc))| {
                    let n = Ident::with_location(loc.clone(), n.clone());
                    get_rowtype(compiler.clone(), te.clone())?.then(move |rowtype: Ref<MType>| {
                        let rowtype = rowtype.read()?.clone();
                        match &rowtype {
                            MType::Record(fields) => Ok(mkcref(
                                fields
                                    .iter()
                                    .map(|field| {
                                        FieldMatch::from_field(
                                            n.clone(),
                                            Ident::without_location(field.name.clone()),
                                            field.type_.clone(),
                                        )
                                    })
                                    .collect(),
                            )),
                            _ => Ok(mkcref(vec![FieldMatch::from_field(
                                n.clone(),
                                n.clone(),
                                mkcref(rowtype),
                            )])),
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        )?;

        let cprojection_terms = combine_crefs(
            self.projection_terms
                .iter()
                .map(|(n, te)| {
                    let n = Ident::with_location(loc.clone(), n.clone());
                    let type_ = Some(te.type_.clone());
                    let sql = te.sql.clone();
                    compiler.async_cref(casync!({
                        let sql = sql.clone().await?;
                        let sql = sql.read()?.clone();
                        Ok(mkcref(FieldMatch {
                            field: n.clone(),
                            type_,
                            sql,
                        }))
                    }))
                })
                .collect::<Result<Vec<_>>>()?,
        )?;

        let parent = match &self.parent {
            Some(parent) => Some(parent.read()?.get_available_references(
                compiler.clone(),
                loc,
                relation.clone(),
            )?),
            None => None,
        };

        compiler.async_cref(casync!({
            let mut ret = match parent {
                Some(parent) => match Arc::try_unwrap(parent.await?) {
                    Ok(parent) => parent.into_inner()?,
                    Err(parent_ref) => parent_ref.read()?.clone(),
                },
                None => AvailableReferences::empty(),
            };

            let mut references = InsertionOrderMap::<Ident, FieldMatch>::new();
            let relations = crelations.await?;
            let projection_terms = cprojection_terms.await?;

            for a in &*relations.read()? {
                for b in &*a.read()? {
                    if let Some(existing) = references.get_mut(&b.field) {
                        existing.type_ = None;
                    } else {
                        references.insert(b.field.get().clone(), b.clone());
                    }
                }
            }

            for a in &*projection_terms.read()? {
                let a = &*a.read()?;
                if !references.contains_key(&a.field) {
                    // Only insert the projection term if it's not a field reference
                    references.insert(a.field.get().clone(), a.clone());
                }
            }

            ret.push(references);
            Ok(mkcref(ret))
        }))
    }

    pub fn remove_bound_references(
        &self,
        compiler: Compiler,
        names: CSQLNames,
    ) -> Result<CRef<CSQLNames>> {
        let relations = self.relations.clone();
        compiler.async_cref({
            let compiler = compiler.clone();
            casync!({
                let mut names = names.clone();
                for (relation, (type_, _)) in &relations {
                    names.unbound.remove(&vec![relation.clone()]);
                    let rowtype = get_rowtype(compiler.clone(), type_.clone())?.await?;
                    match &*rowtype.read()? {
                        MType::Record(fields) => {
                            for field in fields.iter() {
                                names
                                    .unbound
                                    .remove(&vec![relation.clone(), field.name.clone()]);
                                names.unbound.remove(&vec![field.name.clone()]);
                            }
                        }
                        _ => {}
                    };
                }
                Ok(mkcref(names))
            })
        })
    }

    pub fn add_reference(
        &mut self,
        name: &Ident,
        loc: &SourceLocation,
        type_: CRef<MType>,
    ) -> Result<()> {
        match self.relations.entry(name.clone()) {
            btree_map::Entry::Occupied(_) => {
                return Err(CompileError::duplicate_entry(vec![Ident::with_location(
                    loc.clone(),
                    name.clone(),
                )]))
            }
            btree_map::Entry::Vacant(e) => {
                e.insert((type_, loc.clone()));
            }
        };
        Ok(())
    }
}

impl Constrainable for SQLScope {}

#[derive(Debug, Clone)]
pub struct AvailableReferences {
    scopes: Vec<InsertionOrderMap<Ident, FieldMatch>>,
}

impl AvailableReferences {
    fn empty() -> AvailableReferences {
        AvailableReferences { scopes: vec![] }
    }

    fn push(&mut self, scope: InsertionOrderMap<Ident, FieldMatch>) {
        self.scopes.push(scope);
    }

    pub fn get(&self, name: &Ident) -> Option<&FieldMatch> {
        for scope in self.scopes.iter().rev() {
            if let Some(field) = scope.get(name) {
                return Some(field);
            }
        }
        None
    }

    pub fn current_level(&self) -> Option<&InsertionOrderMap<Ident, FieldMatch>> {
        self.scopes.last()
    }
}

impl Constrainable for AvailableReferences {}
