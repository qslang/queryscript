#[cfg(test)]
mod tests {
    use difference::assert_diff;
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::ffi::OsStr;
    use std::fmt;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::ast;
    use crate::parser;
    use crate::runtime;
    use crate::types;
    use crate::{
        compile,
        compile::{Schema, SchemaRef},
    };

    trait TestTransformer {
        fn transform_ast<'a>(&self, ast: &'a ast::Schema) -> Cow<'a, ast::Schema>;
        fn fallback_expr(&self, idx: usize) -> Option<compile::schema::CTypedExpr>;
    }

    struct IdentityTransformer();
    impl TestTransformer for IdentityTransformer {
        fn transform_ast<'a>(&self, ast: &'a ast::Schema) -> Cow<'a, ast::Schema> {
            Cow::Borrowed(ast)
        }

        fn fallback_expr(&self, _idx: usize) -> Option<compile::schema::CTypedExpr> {
            None
        }
    }

    struct UnsafeTransformer {
        pub original: SchemaRef,
    }
    impl TestTransformer for UnsafeTransformer {
        fn transform_ast<'a>(&self, ast: &'a ast::Schema) -> Cow<'a, ast::Schema> {
            let mut transformed_ast = ast::Schema { stmts: Vec::new() };
            for stmt in ast.stmts.iter() {
                transformed_ast.stmts.push(match stmt {
                    ast::Stmt {
                        body: ast::StmtBody::Expr(e),
                        ..
                    } => ast::Stmt {
                        body: ast::StmtBody::Expr(ast::Expr {
                            is_unsafe: true,
                            ..e.clone()
                        }),
                        ..stmt.clone()
                    },
                    o => o.clone(),
                });
            }
            Cow::Owned(transformed_ast)
        }

        fn fallback_expr(&self, idx: usize) -> Option<compile::schema::CTypedExpr> {
            Some(self.original.read().unwrap().exprs[idx].get().clone())
        }
    }

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
            if path.is_file()
                && is_schema_file(path.extension().and_then(OsStr::to_str))
                && !entry
                    .file_name()
                    .as_os_str()
                    .to_str()
                    .unwrap()
                    .contains("ignore")
            {
                let rel_path = path.strip_prefix(std::env::current_dir().unwrap()).unwrap();
                println!("Running {}", rel_path.display());
                test_schema(&rt, &rel_path);
            }
        }
    }
    #[allow(unused)]
    #[derive(Debug)]
    struct TypedValue {
        pub type_: types::Type,
        pub value: String,
    }
    fn eval_expr(
        rt: &runtime::Runtime,
        expr: &compile::schema::CTypedExpr,
        schema: &SchemaRef,
    ) -> Result<TypedValue, runtime::RuntimeError> {
        let expr = expr.to_runtime_type()?;

        let engine_type = runtime::SQLEngineType::DuckDB;
        let async_ctx = crate::runtime::Context::new(&schema, engine_type);
        let async_expr = expr.clone();

        let value = rt.block_on(async move { runtime::eval(&async_ctx, &async_expr).await })?;

        let type_ = expr.type_.read().unwrap();
        Ok(TypedValue {
            type_: type_.clone(),
            value: format!("{}", value),
        })
    }

    fn execute_test_schema(
        rt: &runtime::Runtime,
        path: &std::path::Path,
        transformer: impl TestTransformer,
    ) -> (SchemaRef, BTreeMap<String, Box<dyn fmt::Debug>>) {
        let parsed_path = Path::new(path);
        let file = parsed_path.to_str().unwrap().to_string();
        let parent_path = parsed_path.parent();
        let folder = match parent_path {
            Some(p) => p.to_str().map(|f| f.to_string()),
            None => None,
        };
        let schema = Schema::new(file, folder);

        let mut result = BTreeMap::<String, Box<dyn fmt::Debug>>::new();

        let contents = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                result.insert("parse_errors".to_string(), Box::new(vec![e]));
                return (schema, result);
            }
        };

        let ast = match parser::parse_schema(path.to_str().unwrap(), &contents).as_result() {
            Ok(ast) => ast,
            Err(e) => {
                result.insert("parse_errors".to_string(), Box::new(vec![e]));
                return (schema, result);
            }
        };

        let ast = transformer.transform_ast(&ast);

        let compiler = compile::Compiler::new().expect("Failed to create compiler");
        let mut schema_result = compiler.compile_schema_ast(schema.clone(), &ast);

        result.insert(
            "compile_errors".to_string(),
            Box::new(schema_result.errors.drain(..).collect::<Vec<_>>()),
        );

        let mut decls = BTreeMap::<String, Box<dyn fmt::Debug>>::new();
        for (name, t) in &schema.read().unwrap().type_decls {
            decls.insert(format!("type {}", name), Box::new(t.value.clone()));
        }
        for (name, e) in &schema.read().unwrap().expr_decls {
            decls.insert(format!("let {}", name), Box::new(e.value.type_.clone()));
        }

        let mut exprs = Vec::new();
        for (idx, expr) in (&schema.read().unwrap().exprs).iter().enumerate() {
            match eval_expr(&rt, &expr.get(), &schema) {
                Ok(e) => exprs.push(Ok(e)),
                Err(e) => match transformer.fallback_expr(idx) {
                    Some(expr) => match eval_expr(&rt, &expr, &schema) {
                        Ok(e) => exprs.push(Ok(e)),
                        Err(e) => exprs.push(Err(e)),
                    },
                    None => exprs.push(Err(e)),
                },
            }
        }

        result.insert("decls".to_string(), Box::new(decls));
        result.insert("queries".to_string(), Box::new(exprs));
        (schema, result)
    }

    fn test_schema(rt: &runtime::Runtime, path: &std::path::Path) {
        let (_schema, result) = execute_test_schema(rt, path, IdentityTransformer());
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

        // Unfortunately transforming blindly to unsafe mode doesn't quite work, because the types
        // are not preserved and the aliases can be different. However this is a WIP and still useful
        // to run manually to see what the differences are.
        /*
        let unsafe_transformer = UnsafeTransformer { original: schema };
        let (_, unsafe_result) = execute_test_schema(rt, path, unsafe_transformer);
        let unsafe_result_str = format!("{:#?}", unsafe_result);
        assert_diff!(result_str.as_str(), unsafe_result_str.as_str(), "\n", 0);
        */
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
