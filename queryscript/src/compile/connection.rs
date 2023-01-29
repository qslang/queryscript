use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

use crate::ast::{Ident, Located};

use super::{
    error::{ErrorLocation, Result},
    CompileError,
};

pub struct ConnectionString(Located<Url>);

impl ConnectionString {
    pub fn maybe_parse(
        folder: Option<String>,
        p: &str,
        loc: &ErrorLocation,
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
            "duckdb" => {}
            other => {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    &format!("{} URLs", other),
                ));
            }
        };

        if matches!(url.scheme(), "duckdb") && matches!(url.host_str(), Some(_)) {
            // Allow relative paths for schemes that are on the filesystem

            let host_str = url.host_str().unwrap();
            let mut new_path = PathBuf::new();
            if let Some(folder) = folder {
                new_path.push(folder);
            }
            new_path.push(host_str);
            new_path = new_path.join(url.path().trim_start_matches('/'));

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

        Ok(Some(Arc::new(ConnectionString(Located::new(
            url,
            loc.clone(),
        )))))
    }

    pub fn db_name(&self) -> Located<Ident> {
        let path = Path::new(self.0.path());
        Located::new(
            path.file_stem().unwrap().to_str().unwrap().into(),
            self.0.location().clone(),
        )
    }
}

impl std::fmt::Debug for ConnectionString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConnectionString(")?;
        write!(f, "{:?}://", self.0.scheme())?;
        match self.0.host() {
            Some(h) => write!(f, "{}", h)?,
            None => {}
        };
        write!(f, "{:?}", self.0.path())?;
        write!(f, ")")
    }
}

fn connection_infer_fn() -> impl std::future::Future<Output = Result<()>> + Send + 'static {
    async move {
        return Err(CompileError::unimplemented(
            ErrorLocation::Unknown,
            "connection inference",
        ));
    }
}
