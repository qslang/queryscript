use snafu::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

use crate::ast::{Ident, Located, SourceLocation};
use crate::runtime::SQLEngineType;

use super::compile::ExternalTypeRank;
use super::external::schema_infer_expr_fn;
use super::generics::{ExternalType, GenericConstructor};
use super::inference::mkcref;
use super::schema::{CRef, Decl, Expr, MType, SQLBody, SQLNames, SQLSnippet, SType, STypedExpr};
use super::sql::{select_limit_0, IntoTableFactor};
use super::{
    error::{Result, RuntimeSnafu},
    schema::{DeclMap, ExprEntry},
    CompileError,
};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ConnectionString(Url);

impl ConnectionString {
    pub fn maybe_parse(
        folder: Option<String>,
        p: &str,
        loc: &SourceLocation,
    ) -> Result<Option<Arc<ConnectionString>>> {
        let mut url = match Url::parse(p) {
            Ok(url) => url,
            Err(_) => return Ok(None),
        };

        match url.scheme() {
            "file" => {
                // NOTE: Eventually file URLs can allow you to reference schemas located
                // elsewhere on the filesystem.
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    "file:// URLs are not supported. Use a relative path instead.",
                ));
            }
            scheme => {
                let _ = crate::runtime::SQLEngineType::from_name(scheme)
                    .context(RuntimeSnafu { loc: loc.clone() })?;
            }
        };

        if matches!(url.scheme(), "duckdb") && matches!(url.host_str(), Some(_)) {
            // Allow relative paths for schemes that are on the filesystem
            let host_str = url.host_str().unwrap();
            let mut new_path = PathBuf::new();
            if let Some(folder) = folder {
                new_path.push(std::fs::canonicalize(if folder == "" {
                    "."
                } else {
                    &folder
                })?);
            }
            new_path.push(host_str);
            if url.path() != "" {
                new_path = new_path.join(url.path().trim_start_matches('/'));
            }

            url.set_host(None).unwrap();
            url.set_path(new_path.to_str().unwrap());
        }

        let mut valid_db_name = false;
        if let Some(segments) = url.path_segments() {
            valid_db_name = segments.count() > 0;
        }
        if !valid_db_name {
            return Err(CompileError::invalid_conn(
                loc.clone(),
                "missing a database name",
            ));
        }

        Ok(Some(Arc::new(ConnectionString(url))))
    }

    pub fn db_name(&self) -> Ident {
        let path = Path::new(self.0.path());
        path.file_stem().unwrap().to_str().unwrap().into()
    }

    pub fn engine_type(&self) -> SQLEngineType {
        SQLEngineType::from_name(self.0.scheme())
            .expect("Engine type should have been validated in constructor")
    }

    pub fn get_url(&self) -> &Url {
        &self.0
    }
}

impl std::fmt::Debug for ConnectionString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConnectionString(")?;
        write!(f, "{}://", self.0.scheme())?;
        match self.0.host() {
            Some(h) => write!(f, "{}", h)?,
            None => {}
        };
        write!(f, "{}", self.0.path())?;
        write!(f, ")")
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionSchema {
    pub url: Arc<ConnectionString>,
    pub location: SourceLocation,
    expr_decls: DeclMap<ExprEntry>,
}

impl ConnectionSchema {
    pub fn new(url: Arc<ConnectionString>, location: SourceLocation) -> Self {
        Self {
            url,
            location,
            expr_decls: DeclMap::new(),
        }
    }

    pub fn get_decl(
        &mut self,
        compiler: &super::Compiler,
        ident: &Located<Ident>,
    ) -> Result<Option<Located<Decl<ExprEntry>>>> {
        match self.expr_decls.entry(ident.get().clone()) {
            std::collections::btree_map::Entry::Occupied(e) => {
                let decl = e.get();
                Ok(Some(decl.clone()))
            }
            std::collections::btree_map::Entry::Vacant(e) => {
                let expr_type =
                    CRef::new_unknown(&format!("connection {:?} -> {}", &self.url, &ident));

                let body = SQLBody::Table(ident.to_table_factor());

                let resolve = schema_infer_expr_fn(
                    None,
                    mkcref(Expr::SQL(
                        Arc::new(SQLSnippet {
                            names: SQLNames::new(),
                            body: SQLBody::Query(select_limit_0(body.as_query()?)),
                        }),
                        Some(self.url.clone()),
                    )),
                    expr_type.clone(),
                );
                compiler.add_external_type(resolve, expr_type.clone(), ExternalTypeRank::Load)?;

                let expr = mkcref(Expr::SQL(
                    Arc::new(SQLSnippet {
                        names: SQLNames::new(),
                        body,
                    }),
                    Some(self.url.clone()),
                ));

                let external_type = mkcref(MType::Generic(Located::new(
                    ExternalType::new(ident.location(), vec![expr_type])?,
                    ident.location().clone(),
                )));

                let ret = Located::new(
                    Decl {
                        public: true,
                        extern_: false,
                        is_arg: false,
                        name: ident.clone(),
                        value: STypedExpr {
                            type_: SType::new_mono(external_type),
                            expr,
                        },
                    },
                    self.location.clone(),
                );

                // Ideally we use the entry interface for this, but it's a little dicey with ownership
                Ok(Some(e.insert(ret).clone()))
            }
        }
    }
}
