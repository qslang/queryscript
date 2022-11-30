#[cfg(test)]
mod tests {
    use difference::assert_diff;
    use std::collections::BTreeMap;
    use std::ffi::OsStr;
    use std::fmt;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::compile;
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
                println!("Running {}", path.display());
                test_schema(&rt, &path);
            } else {
                println!("Skipping {}", path.display());
            }
        }
    }

    fn execute_test_schema(
        rt: &runtime::Runtime,
        path: &PathBuf,
    ) -> BTreeMap<String, Box<dyn fmt::Debug>> {
        let mut result = BTreeMap::<String, Box<dyn fmt::Debug>>::new();

        let compiler = compile::Compiler::new().expect("Failed to create compiler");

        let schema = match compiler.compile_schema_from_file(path) {
            Ok(schema) => schema,
            Err(e) => {
                result.insert("compile_error".to_string(), Box::new(e));
                return result;
            }
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

        let mut exprs = Vec::new();
        for expr in &schema.read().unwrap().exprs {
            #[derive(Debug)]
            #[allow(dead_code)]
            struct TypedValue {
                pub type_: types::Type,
                pub value: types::Value,
            }

            let expr = expr
                .to_runtime_type()
                .expect(format!("Could not convert expression {:?}", expr).as_str());
            let async_ctx = (&schema).into();
            let async_expr = expr.clone();
            let value = rt
                .block_on(async move { runtime::eval(&async_ctx, &async_expr).await })
                .expect("Failed to evaluate expression");

            exprs.push(TypedValue {
                type_: expr.type_.read().unwrap().clone(),
                value,
            });
        }

        result.insert("decls".to_string(), Box::new(decls));
        result.insert("queries".to_string(), Box::new(exprs));
        result
    }

    fn test_schema(rt: &runtime::Runtime, path: &PathBuf) {
        let result = execute_test_schema(rt, path);
        let result_str = format!("{:#?}", result);

        let mut expected_file = path.clone();
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
}
