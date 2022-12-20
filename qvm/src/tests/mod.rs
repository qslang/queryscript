#[cfg(test)]
mod tests {
    use difference::assert_diff;
    use std::collections::BTreeMap;
    use std::ffi::OsStr;
    use std::fmt;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::compile;
    use crate::parser;
    use crate::runtime;
    use crate::types;

    fn is_schema_file(ext: Option<&str>) -> bool {
        match ext {
            Some(ext) => compile::schema::SCHEMA_EXTENSIONS.iter().any(|e| &ext == e),
            None => false,
        }
    }

    fn test_directory(rt: &runtime::Runtime, dir: &PathBuf) {
        println!("Running tests in {}", dir.display());
        for entry in fs::read_dir(dir).expect(format!("Could not read {}", dir.display()).as_str())
        {
            let entry = entry.expect(format!("Could not read {}", dir.display()).as_str());
            let path = entry.path();
            if path.is_file() && is_schema_file(path.extension().and_then(OsStr::to_str)) {
                let rel_path = path.strip_prefix(std::env::current_dir().unwrap()).unwrap();
                println!("Running {}", rel_path.display());
                test_schema(&rt, &rel_path);
            } else {
                println!("Skipping {}", path.display());
            }
        }
    }

    fn execute_test_schema(
        rt: &runtime::Runtime,
        path: &std::path::Path,
    ) -> BTreeMap<String, Box<dyn fmt::Debug>> {
        let mut result = BTreeMap::<String, Box<dyn fmt::Debug>>::new();

        let compiler = compile::Compiler::new().expect("Failed to create compiler");

        let mut schema_result = compiler.compile_schema_from_file(path);

        result.insert(
            "compile_errors".to_string(),
            Box::new(schema_result.errors.drain(..).collect::<Vec<_>>()),
        );

        let schema = match schema_result.result {
            Some(schema) => schema,
            None => return result,
        };

        let mut decls = BTreeMap::<String, Box<dyn fmt::Debug>>::new();
        for (name, decl) in &schema.read().unwrap().decls {
            match &decl.value {
                compile::schema::SchemaEntry::Type(t) => {
                    decls.insert(format!("type {}", name), Box::new(t.clone()));
                }
                compile::schema::SchemaEntry::Expr(e) => {
                    decls.insert(format!("let {}", name), Box::new(e.type_.clone()));
                }
                _ => {}
            }
        }

        let engine_type = runtime::SQLEngineType::DuckDB;
        let mut exprs = Vec::new();
        for expr in &schema.read().unwrap().exprs {
            #[derive(Debug)]
            #[allow(dead_code)]
            struct TypedValue {
                pub type_: types::Type,
                pub value: String,
            }

            let expr = match expr.to_runtime_type() {
                Ok(e) => e,
                Err(e) => {
                    exprs.push(Err(e));
                    continue;
                }
            };
            let async_ctx = crate::runtime::build_context(&schema, engine_type);
            let async_expr = expr.clone();

            let value =
                match rt.block_on(async move { runtime::eval(&async_ctx, &async_expr).await }) {
                    Ok(e) => e,
                    Err(e) => {
                        exprs.push(Err(e));
                        continue;
                    }
                };

            exprs.push(Ok(TypedValue {
                type_: expr.type_.read().unwrap().clone(),
                value: format!("{}", value),
            }));
        }

        result.insert("decls".to_string(), Box::new(decls));
        result.insert("queries".to_string(), Box::new(exprs));
        result
    }

    fn test_schema(rt: &runtime::Runtime, path: &std::path::Path) {
        let result = execute_test_schema(rt, path);
        let result_str = format!("{:#?}", result);

        let mut expected_file = PathBuf::from(path);
        expected_file.set_extension("expected");
        if expected_file.exists() {
            let expected_str = fs::read_to_string(&expected_file)
                .expect(format!("Could not read {}", expected_file.display()).as_str());

            assert_diff!(expected_str.as_str(), result_str.as_str(), "\n", 0);
        } else {
            fs::write(&expected_file, result_str.as_bytes())
                .expect(format!("Could not write {}", expected_file.display()).as_str());
        }
    }

    #[test]
    fn test_schemas() {
        let tests = vec![Path::new(env!("CARGO_MANIFEST_DIR")), Path::new("tests/")]
            .iter()
            .collect::<PathBuf>();
        assert!(tests.is_dir());

        let rt = runtime::build().expect("Failed to build runtime");
        println!("Running tests in {}", tests.display());
        for entry in
            fs::read_dir(&tests).expect(format!("Could not read {}", tests.display()).as_str())
        {
            let entry = entry.expect(format!("Could not read {}", tests.display()).as_str());
            test_directory(&rt, &entry.path());
        }
    }

    #[test]
    fn test_double_schemas() {
        let prefix = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/jaffle/");

        let compiler = compile::Compiler::new().expect("Failed to create compiler");

        for fname in ["staging.tql", "queries.tql"].iter() {
            let fpath = prefix.join(fname);
            let schema = compile::Schema::new(
                String::from(fpath.to_str().unwrap()),
                Some(String::from(prefix.to_str().unwrap())),
            );

            // Read contents of file at f1 into c1
            let text = fs::read_to_string(&fpath).expect("Failed to read file");
            let ast = parser::parse_schema(fpath.to_str().unwrap(), &text).unwrap();
            compiler
                .compile_schema_ast(schema.clone(), &ast)
                .as_result()
                .expect("Compiled first file");

            for expr in schema.read().unwrap().exprs.iter() {
                let _ = expr
                    .to_runtime_type()
                    .expect("Failed to convert to runtime type");
            }
        }
    }
}
