use anyhow::{bail, Result};
use pyo3::prelude::*;
use std::path::Path;

use std::collections::BTreeMap;

#[cfg_attr(rustfmt, rustfmt_skip)]
use ::queryscript::{
    error::QSError,
    compile,
    ast::Ident,
    integrations::dbt::{extract_metadata, SimpleSchema},
    parser::error::PrettyError,
};

#[pyclass]
struct Compiler {
    compiler: compile::Compiler,
}

#[pymethods]
impl Compiler {
    #[new]
    fn new(dbt_alias: Option<String>) -> Result<Self> {
        let mut import_aliases = BTreeMap::new();
        if let Some(alias) = dbt_alias {
            import_aliases.insert(Ident::from("dbt"), alias);
        }

        let compiler = compile::Compiler::new_with_config(compile::CompilerConfig {
            import_aliases,
            ..Default::default()
        })?;

        Ok(Compiler { compiler })
    }

    fn compile(&self, file: &str) -> Result<SimpleSchema> {
        let schema = match self
            .compiler
            .compile_schema_from_file(&Path::new(&file))
            .as_result()
        {
            Ok(s) => s.unwrap(),
            Err(err) => {
                let errs = QSError::from(err).format_without_backtrace();
                let contents = self.compiler.file_contents()?;
                let err_strs = errs
                    .iter()
                    .map(|e| e.pretty_with_code(&contents.files))
                    .collect::<Vec<_>>();
                bail!(err_strs.join("\n"));
            }
        };

        Ok(extract_metadata(schema)?)
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn queryscript(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Compiler>()?;
    Ok(())
}
