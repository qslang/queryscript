#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsStr;
    use std::fmt;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::compile;
    use crate::runtime;
    use crate::types;

    fn test_directory(dir: &PathBuf) {
        println!("Running tests in {}", dir.display());
        for entry in fs::read_dir(dir).expect(format!("Could not read {}", dir.display()).as_str())
        {
            let entry = entry.expect(format!("Could not read {}", dir.display()).as_str());
            let path = entry.path();
            if path.is_file() && path.extension().and_then(OsStr::to_str) == Some("co") {
                println!("Running {}", path.display());
                test_schema(&path);
            } else {
                println!("Skipping {}", path.display());
            }
        }
    }

    fn test_schema(path: &PathBuf) {
        let schema = compile::compile_schema_from_file(path).expect("Failed to compile schema");
        let mut decls = BTreeMap::<String, Box<dyn fmt::Debug>>::new();
        for (name, decl) in &schema.borrow().decls {
            match &decl.value {
                compile::schema::SchemaEntry::Type(t) => {
                    decls.insert(format!("type {}", name), Box::new(t.clone()));
                }
                compile::schema::SchemaEntry::Expr(e) => {
                    decls.insert(
                        format!("let {}", name),
                        Box::new(
                            e.must()
                                .expect("Unresolved declaration after compilation")
                                .borrow()
                                .type_
                                .clone(),
                        ),
                    );
                }
                _ => {}
            }
        }

        let mut exprs = Vec::new();
        for expr in &schema.borrow().exprs {
            #[derive(Debug)]
            #[allow(dead_code)]
            struct TypedValue {
                pub type_: types::Type,
                pub value: types::Value,
            }

            let expr = expr
                .to_runtime_type()
                .expect(format!("Could not convert expression {:?}", expr).as_str());
            let value = runtime::eval(schema.clone(), &expr)
                .expect(format!("Could not run expression {:?}", expr).as_str());

            exprs.push(TypedValue {
                type_: expr.type_.borrow().clone(),
                value,
            });
        }

        let mut result = BTreeMap::<String, Box<dyn fmt::Debug>>::new();
        result.insert("decls".to_string(), Box::new(decls));
        result.insert("queries".to_string(), Box::new(exprs));

        let result_str = format!("{:#?}", result);

        let mut expected_file = path.clone();
        expected_file.set_extension("expected");
        if expected_file.exists() {
            let expected_str = fs::read_to_string(&expected_file)
                .expect(format!("Could not read {}", expected_file.display()).as_str());

            assert_eq!(result_str, expected_str);
        } else {
            fs::write(&expected_file, result_str.as_bytes())
                .expect(format!("Could not write {}", expected_file.display()).as_str());
        }
    }

    #[test]
    fn test_schemas() {
        let tests = vec![
            Path::new(env!("CARGO_MANIFEST_DIR")),
            Path::new("src/tests/schemas/"),
        ]
        .iter()
        .collect::<PathBuf>();
        assert!(tests.is_dir());

        println!("Running tests in {}", tests.display());
        for entry in
            fs::read_dir(&tests).expect(format!("Could not read {}", tests.display()).as_str())
        {
            let entry = entry.expect(format!("Could not read {}", tests.display()).as_str());
            test_directory(&entry.path());
        }
    }
}
