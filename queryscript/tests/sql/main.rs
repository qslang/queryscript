mod db;

#[cfg(test)]
mod tests {
    use queryscript::types::arrow::ArrowRecordBatchRelation;
    use sqllogictest::{parse, Record};
    use std::path::{Path, PathBuf};
    use walkdir;

    use super::db::{duckdb_serialize, DuckDB, QueryScript};

    fn update_sqllogic_file(path: &PathBuf) {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let duckdb_tester = sqllogictest::Runner::new(DuckDB::new(conn.try_clone().unwrap()));

        futures::executor::block_on(sqllogictest::runner::update_test_file(
            path,
            duckdb_tester,
            " ",
            |_, _| false, /* always use DuckDB's values */
        ))
        .unwrap();
    }

    fn test_sqllogic_file(path: &PathBuf) {
        let conn = duckdb::Connection::open_in_memory().unwrap();

        let contents = std::fs::read_to_string(path).expect("Could not read file");

        let records =
            parse(&contents).expect(format!("Could not parse file {}", path.display()).as_str());

        // Read up to the last statement, save each table to a parquet file, and then run the remaining
        // SELECT statements against DuckDB directly and QueryScript and compare the results.
        let mut last_statement = -1;
        for (i, record) in records.iter().enumerate() {
            match record {
                Record::Statement { .. } => {
                    last_statement = i as i64;
                }
                _ => {}
            }
        }

        let mut duckdb_tester = sqllogictest::Runner::new(DuckDB::new(conn.try_clone().unwrap()));

        let test_prefix = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
        let test_suffix = path
            .strip_prefix(&test_prefix)
            .unwrap()
            .file_stem()
            .unwrap();

        let target_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/generated")
            .join(test_suffix);

        let mut qs_tester = None;
        let rt = queryscript::runtime::build().unwrap();
        let _ = rt.enter();

        for (i, record) in records.into_iter().enumerate() {
            let i = i as i64;

            if i <= last_statement {
                duckdb_tester.run(record).unwrap();
                continue;
            }

            if i == last_statement + 1 {
                qs_tester = Some(sqllogictest::Runner::new(QueryScript::new(
                    duckdb_serialize(&conn, &target_dir).unwrap(),
                )));
            }

            futures::executor::block_on(async {
                qs_tester.as_mut().unwrap().run_async(record).await
            })
            .unwrap();
        }
    }

    #[test]
    fn test_sqllogic() {
        let tests = vec![
            Path::new(env!("CARGO_MANIFEST_DIR")),
            Path::new("tests/sql/"),
        ]
        .iter()
        .collect::<PathBuf>();
        assert!(tests.is_dir());

        let mut test_files = Vec::new();
        for entry in walkdir::WalkDir::new(&tests) {
            let entry = entry.expect(format!("Could not read {}", tests.display()).as_str());

            // Find all files whose extension is .test
            if entry.file_name().to_str().unwrap().ends_with(".test") {
                test_files.push(entry.path().to_path_buf());
            }
        }

        for test_file in test_files {
            update_sqllogic_file(&test_file); // TODO: We should figure out how to only update this when necessary
            test_sqllogic_file(&test_file);
        }
    }

    #[test]
    fn test_duckdb_stmts() {
        use parquet::arrow::arrow_writer::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let conn = duckdb::Connection::open_in_memory().unwrap();
        let serialize_conn = conn.try_clone().unwrap();

        for sql in [
            "CREATE TABLE t (a int)",
            "INSERT INTO t VALUES (1)",
            "CREATE TABLE empty (b text)",
        ]
        .iter()
        {
            let mut stmt = conn.prepare(sql).unwrap();
            let rbs = ArrowRecordBatchRelation::from_duckdb(stmt.query_arrow([]).unwrap());

            let schema = rbs.schema();
            assert!(
                schema.len() == 1
                    && schema[0].name.as_str() == "Count"
                    && schema[0].type_
                        == queryscript::types::Type::Atom(queryscript::types::AtomicType::Int64)
            );
        }

        // Shadow the conn variable
        let conn = serialize_conn;

        // Ensure the tables show up in show tables
        let mut show_tables = conn.prepare("SHOW TABLES").unwrap();
        let tables = show_tables
            .query_and_then([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(tables, vec!["empty", "t"]);

        let target_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/generated/duckdb-sanity");
        // Make the target directory if it does not exist
        std::fs::create_dir_all(&target_dir).unwrap();

        // Extract each table as a parquet file
        for table in tables {
            let mut stmt = conn
                .prepare(format!("SELECT * FROM \"{}\"", table).as_str())
                .unwrap();
            let arrow_result = stmt.query_arrow([]).unwrap();

            let target_file = std::fs::File::create(
                target_dir
                    .join(format!("{table}.parquet"))
                    .to_str()
                    .unwrap(),
            )
            .unwrap();

            let props = WriterProperties::builder().build();
            let mut writer =
                ArrowWriter::try_new(target_file, arrow_result.get_schema(), Some(props)).unwrap();

            for batch in arrow_result {
                writer.write(&batch).unwrap();
            }

            // For some reason this isn't called in the Drop trait. We may want to wrap it?
            writer.close().unwrap();
        }
    }
}
