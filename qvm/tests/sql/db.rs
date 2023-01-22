// NOTE: We should have one of these implementations per backend engine that
// we support. QVM's responsibility is to, when run against a particular engine,
// mimic that engines' semantics. A secondary (stretch) goal is to make the semantics
// identical across engines, which we could test here as well.

use std::pin::Pin;

use futures::{future::FutureExt, Future};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use snafu::prelude::*;
use sqllogictest::{DBOutput, DB};

use qvm::ast::SourceLocation;
use qvm::runtime;
use qvm::types::{arrow::ArrowRecordBatchRelation, Value};
use qvm::{compile, compile::error::RuntimeSnafu};

pub struct DuckDB {
    conn: duckdb::Connection,
}

impl DuckDB {
    pub fn new(conn: duckdb::Connection) -> DuckDB {
        DuckDB { conn }
    }
}

impl DB for DuckDB {
    type Error = duckdb::Error;

    fn engine_name(&self) -> &'static str {
        "duckdb"
    }

    fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error> {
        let mut stmt = self.conn.prepare(sql)?;
        let query_result = stmt.query_arrow([])?;

        let relation = ArrowRecordBatchRelation::from_duckdb(query_result);
        Ok(relation.as_ref().into_db_output())
    }
}

pub struct QueryScript {
    compiler: compile::Compiler,
    schema: compile::SchemaRef,
}

impl QueryScript {
    pub fn new(schema: compile::SchemaRef) -> QueryScript {
        QueryScript {
            compiler: compile::Compiler::new().unwrap(),
            schema,
        }
    }
}

impl sqllogictest::AsyncDB for QueryScript {
    type Error = compile::error::CompileError;

    fn engine_name(&self) -> &'static str {
        "queryscript"
    }

    fn run<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        sql: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = Result<DBOutput, Self::Error>> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        let query = format!("{};", sql);
        async move {
            // NOTE: We should work out a way to compile queries against a
            // schema without adding it to the schema's persistent list of expressions
            let starting_num_exprs = self.schema.read()?.exprs.len();
            self.compiler
                .compile_string(self.schema.clone(), &query)
                .as_result()?;

            let num_exprs = self.schema.read()?.exprs.len();
            if num_exprs == starting_num_exprs {
                return Err(compile::error::CompileError::internal(
                    SourceLocation::Unknown,
                    "no query to run",
                ));
            }

            let expr = self.schema.read()?.exprs[num_exprs - 1]
                .to_runtime_type()
                .context(RuntimeSnafu {
                    loc: SourceLocation::Unknown,
                })?;

            let ctx = runtime::Context::new(&self.schema, runtime::SQLEngineType::DuckDB);

            let value = runtime::eval(&ctx, &expr).await.context(RuntimeSnafu {
                loc: SourceLocation::Unknown,
            })?;

            match value {
                Value::Relation(relation) => Ok(relation.as_ref().into_db_output()),
                _ => Err(compile::error::CompileError::internal(
                    SourceLocation::Unknown,
                    "query did not return a relation",
                )),
            }
        }
        .boxed()
    }
}

struct SafeWriter<W: std::io::Write>(ArrowWriter<W>);

impl<W: std::io::Write> std::ops::Deref for SafeWriter<W> {
    type Target = ArrowWriter<W>;
    fn deref(&self) -> &ArrowWriter<W> {
        &self.0
    }
}

impl<W: std::io::Write> std::ops::DerefMut for SafeWriter<W> {
    fn deref_mut(&mut self) -> &mut ArrowWriter<W> {
        &mut self.0
    }
}

impl<W: std::io::Write> std::ops::Drop for SafeWriter<W> {
    fn drop(&mut self) {
        self.0.flush().unwrap();
    }
}

pub fn duckdb_serialize(
    conn: &duckdb::Connection,
    target_dir: &std::path::PathBuf,
) -> compile::error::Result<compile::SchemaRef> {
    let compiler = compile::Compiler::new().expect("Failed to instantiate compiler");
    let schema_path = duckdb_write_tables(conn, target_dir).context(RuntimeSnafu {
        loc: SourceLocation::Unknown,
    })?;

    let schema = compiler
        .compile_schema_from_file(&schema_path)
        .as_result()?;

    Ok(schema.unwrap())
}

fn duckdb_write_tables(
    conn: &duckdb::Connection,
    target_dir: &std::path::PathBuf,
) -> runtime::error::Result<std::path::PathBuf> {
    let mut schema_stmts = Vec::new();

    // Ensure the tables show up in show tables
    let mut show_tables = conn.prepare("SHOW TABLES")?;
    let tables = show_tables
        .query_and_then([], |row| row.get::<_, String>(0))?
        .map(|r| Ok(r?))
        .collect::<qvm::runtime::error::Result<Vec<_>>>()?;

    // Make the target directory if it does not exist
    std::fs::create_dir_all(&target_dir)?;

    // Extract each table as a parquet file
    for table in tables {
        let mut stmt = conn.prepare(format!("SELECT * FROM \"{}\"", table).as_str())?;
        let arrow_result = stmt.query_arrow([])?;

        let target_file = std::fs::File::create(
            target_dir
                .join(format!("{table}.parquet"))
                .to_str()
                .unwrap(),
        )?;

        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(target_file, arrow_result.get_schema(), Some(props))?;

        for batch in arrow_result {
            writer.write(&batch)?;
        }

        writer.close()?;

        // TODO: We should be able to serialize the type here and avoid schema inference
        schema_stmts.push(format!("let {table} = load('{table}.parquet');"));
    }

    let schema_contents = schema_stmts.join("\n");
    let schema_path = target_dir.join("schema.qs");
    std::fs::write(&schema_path, schema_contents)?;

    Ok(schema_path)
}

trait IntoDBOutput {
    fn into_db_output(self) -> DBOutput;
}

impl IntoDBOutput for &dyn qvm::types::Relation {
    fn into_db_output(self) -> DBOutput {
        let schema = self.schema();
        let records = self.records();

        // This is a bit of a hack to try to guess that it's a statement
        if schema.len() == 1
            && schema[0].name.as_str() == "Count"
            && schema[0].type_ == qvm::types::Type::Atom(qvm::types::AtomicType::Int64)
        {
            let count: i64 = records.get(0).map_or(0, |r| {
                r.column(0)
                    .clone()
                    .try_into()
                    .expect("Failed to convert i64")
            });
            assert!(
                count >= 0,
                "The count returned from a statement should be non-negative ({})",
                count
            );
            DBOutput::StatementComplete(count as u64)
        } else {
            let types = schema
                .iter()
                .map(|f| to_columntype(&f.type_))
                .collect::<Vec<_>>();

            let rows = records
                .iter()
                .map(|r| {
                    (0..schema.len())
                        .map(|i| format!("{}", r.column(i)))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            DBOutput::Rows { types, rows }
        }
    }
}

fn to_columntype(type_: &qvm::types::Type) -> sqllogictest::runner::ColumnType {
    use qvm::types::AtomicType::*;
    use qvm::types::Type::*;
    use sqllogictest::runner::ColumnType;
    match type_ {
        Atom(Null) => ColumnType::Unknown('n'),
        Atom(Boolean) => ColumnType::Unknown('b'),
        Atom(Int8) | Atom(Int16) | Atom(Int32) | Atom(Int64) | Atom(UInt8) | Atom(UInt16)
        | Atom(UInt32) | Atom(UInt64) => ColumnType::Integer,

        Atom(Float16) | Atom(Float32) | Atom(Float64) => ColumnType::FloatingPoint,

        Atom(Timestamp(..)) | Atom(Date32) | Atom(Date64) | Atom(Time32(..)) | Atom(Time64(..))
        | Atom(Interval(..)) => ColumnType::Unknown('t'),
        Atom(Binary)
        | Atom(FixedSizeBinary(..))
        | Atom(LargeBinary)
        | Atom(Utf8)
        | Atom(LargeUtf8) => ColumnType::Text,

        Atom(Decimal128(..)) | Atom(Decimal256(..)) => ColumnType::FloatingPoint,

        List(_data_type) => ColumnType::Unknown('l'),
        Record(_) => ColumnType::Unknown('r'),
        Fn(_) => ColumnType::Unknown('f'),
    }
}
