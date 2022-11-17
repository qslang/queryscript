#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::compile;
    use crate::runtime;

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
        let expr_decl = if let Some(test_output) = schema.borrow().decls.get("__test_output") {
            if !test_output.extern_ {
                if let compile::schema::SchemaEntry::Expr(expr_decl) = &test_output.value {
                    Some(expr_decl.clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        match expr_decl {
            Some(expr_decl) => {
                let expr = expr_decl
                    .borrow()
                    .to_runtime_type()
                    .expect(format!("Could not convert expression {:?}", expr_decl).as_str());
                let result = runtime::eval(schema.clone(), &expr)
                    .expect(format!("Could not run expression {:?}", expr).as_str());
                println!("Checking output for {}", path.display());

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
            None => {
                println!("No output for {}", path.display());
            }
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
