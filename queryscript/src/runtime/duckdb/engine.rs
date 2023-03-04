use lazy_static::lazy_static;
use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_void, CStr, CString};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::{
    datatypes::SchemaRef as ArrowSchemaRef, ffi::FFI_ArrowSchema, ffi_stream::FFI_ArrowArrayStream,
    record_batch::RecordBatch, record_batch::RecordBatchReader,
};
use cxx::{CxxString, CxxVector};
use duckdb::{ffi as cffi, Connection};
use sqlparser::ast as sqlast;

use crate::ast::Ident;
use crate::compile::sql::{create_table_as, select_star_from};
use crate::compile::ConnectionString;
use crate::runtime::{
    self,
    error::{rt_unimplemented, Result},
    normalize::Normalizer,
    sql::{SQLEngine, SQLEnginePool, SQLParam},
};
use crate::runtime::{SQLEmbedded, SQLEngineType};
use crate::types::Type;
use crate::types::{arrow::ArrowRecordBatchRelation, Relation, Value};

#[cxx::bridge]
pub mod cppffi {
    extern "Rust" {
        unsafe fn rust_build_array_stream(
            data: *mut u32,
            fields: &CxxVector<CxxString>,
            dest: *mut u32,
        );
    }
    unsafe extern "C++" {
        include!("queryscript/include/duckdb-extra.hpp");

        type ArrowArrayStreamWrapper;
        type Value;

        unsafe fn get_create_stream_fn() -> *mut u32;
        unsafe fn duckdb_create_pointer(value: *mut u32) -> *mut Value;
        unsafe fn init_arrow_scan(connection_ptr: *mut u32);
    }
}

pub struct DuckDBNormalizer {
    params: HashMap<String, String>,
}

static mut NEXT_DUCKDB_PLACEHOLDER: AtomicUsize = AtomicUsize::new(0);
impl DuckDBNormalizer {
    pub fn new(scalar_params: &[Ident], relations: &HashSet<String>) -> DuckDBNormalizer {
        let mut params: HashMap<String, String> = scalar_params
            .iter()
            .enumerate()
            .map(|(i, s)| (s.to_string(), format!("${}", i + 1)))
            .collect();

        for relation in relations {
            params.insert(
                relation.to_string(),
                format!("__qs_duck_{}", unsafe {
                    NEXT_DUCKDB_PLACEHOLDER.fetch_add(1, Ordering::SeqCst)
                }),
            );
        }

        DuckDBNormalizer { params }
    }
}

impl Normalizer for DuckDBNormalizer {
    fn quote_style(&self) -> Option<char> {
        Some('"')
    }

    fn param(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(|s| s.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct ArrowRelation {
    pub relation: Arc<dyn Relation>,
    pub schema: ArrowSchemaRef,
}

type RelationMap = HashMap<String, ArrowRelation>;

struct LocalRelations(HashSet<String>, Arc<Mutex<RelationMap>>);

impl LocalRelations {
    pub fn new(relations: Arc<Mutex<RelationMap>>) -> LocalRelations {
        LocalRelations(HashSet::new(), relations)
    }
}

impl std::ops::Deref for LocalRelations {
    type Target = HashSet<String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for LocalRelations {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::ops::Drop for LocalRelations {
    fn drop(&mut self) {
        let mut relations = self.1.lock().unwrap();
        for relation in self.0.iter() {
            relations.remove(relation);
        }
    }
}

#[derive(Debug)]
pub struct DuckDBEngine {
    conn: ExclusiveConnection,
}

impl DuckDBEngine {
    fn new_conn(url: Option<Arc<crate::compile::ConnectionString>>) -> Result<Box<dyn SQLEngine>> {
        use std::collections::hash_map::Entry;
        let mut conns = DUCKDB_CONNS.lock().unwrap();
        let wrapper = match conns.entry(url) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let conn_wrapper = ConnectionSingleton::new(e.key().clone())?;
                e.insert(conn_wrapper)
            }
        };
        let base_conn = wrapper.try_clone()?;
        Ok(Box::new(DuckDBEngine::build(base_conn)))
    }

    fn build(conn: ExclusiveConnection) -> DuckDBEngine {
        DuckDBEngine { conn }
    }

    fn eval_in_place(
        &mut self,
        query: &sqlast::Statement,
        params: HashMap<Ident, SQLParam>,
    ) -> Result<Arc<dyn Relation>> {
        // The code below works by (globally within the connection) installing a replacement
        // scan that accesses the relations referenced in the query parameters. I did some light
        // benchmarking and the cost to create a connection seemed relatively low, but we could
        // potentially pool or concurrently use these connections if necessary.
        let conn_state = self.conn.get_state();

        let mut scalar_params = Vec::new();

        let mut relation_params = LocalRelations::new(conn_state.relations.clone());
        for (key, param) in params.iter() {
            match &param.value {
                Value::Relation(_) => {
                    relation_params.insert(key.to_string());
                }
                Value::Fn(_) => {
                    return rt_unimplemented!("Function parameters");
                }
                _ => {
                    scalar_params.push(key.clone());
                }
            }
        }

        scalar_params.sort();
        let normalizer = DuckDBNormalizer::new(&scalar_params, &relation_params);
        let query = normalizer.normalize(&query).as_result()?;

        {
            let relations = &mut conn_state.relations.lock()?;

            for (key, param) in params.iter() {
                match &param.value {
                    Value::Relation(r) => {
                        relations.insert(
                            normalizer.params.get(&key.to_string()).unwrap().clone(),
                            ArrowRelation {
                                relation: r.clone(),
                                schema: Arc::new((&param.type_).try_into()?),
                            },
                        );
                    }
                    _ => {}
                }
            }
        }

        let query_string = format!("{}", query);

        let duckdb_params: Vec<&dyn duckdb::ToSql> = scalar_params
            .iter()
            .map(|k| &params.get(k).unwrap().value as &dyn duckdb::ToSql)
            .collect();

        let mut stmt = conn_state.conn.prepare(&query_string)?;

        let query_result = stmt.query_arrow(duckdb_params.as_slice())?;

        // NOTE: We could probably avoid collecting the whole result here and instead make
        // ArrowRecordBatchRelation accept an iterator as input.
        Ok(ArrowRecordBatchRelation::from_duckdb(query_result))
    }
}

impl ArrowRecordBatchRelation {
    pub fn from_duckdb(query_result: duckdb::Arrow) -> Arc<dyn Relation> {
        // NOTE: We could probably avoid collecting the whole result here and instead make
        // ArrowRecordBatchRelation accept an iterator as input.
        ArrowRecordBatchRelation::new(
            query_result.get_schema(),
            Arc::new(query_result.collect::<Vec<RecordBatch>>()),
        )
    }
}

#[async_trait::async_trait]
impl SQLEngine for DuckDBEngine {
    async fn query(
        &mut self,
        query: &sqlast::Statement,
        params: HashMap<Ident, SQLParam>,
    ) -> Result<Arc<dyn Relation>> {
        // We call block_in_place here because DuckDB may perform computationally expensive work,
        // and it's not hooked into the async coroutines that the runtime uses (and therefore cannot
        // yield work). block_in_place() tells Tokio to expect this thread to spend a while working on
        // this stuff and use other threads for other work.
        runtime::expensive(|| self.eval_in_place(query, params))
    }

    async fn exec(
        &mut self,
        stmt: &sqlast::Statement,
        params: HashMap<Ident, SQLParam>,
    ) -> Result<()> {
        self.query(stmt, params).await?;
        Ok(())
    }

    async fn load(
        &mut self,
        table: &sqlast::ObjectName,
        value: Value,
        type_: Type,
        temporary: bool,
    ) -> Result<()> {
        let param_name = "__qs_load";

        let query = create_table_as(
            table.clone(),
            select_star_from(sqlast::TableFactor::Table {
                name: sqlast::ObjectName(vec![sqlast::Located::new(param_name.into(), None)]),
                alias: None,
                args: None,
                columns_definition: None,
                with_hints: vec![],
            }),
            temporary,
        );
        let params = vec![(
            param_name.into(),
            SQLParam {
                name: param_name.into(),
                value,
                type_,
            },
        )]
        .into_iter()
        .collect();
        self.query(&query, params).await?;
        Ok(())
    }

    async fn table_exists(&mut self, table: &sqlast::ObjectName) -> Result<bool> {
        let conn_state = self.conn.get_state();
        let mut stmt = conn_state
            .conn
            .prepare("SELECT 1 FROM information_schema.tables WHERE table_name = ?")?;
        let result = stmt
            .query_map(&[&table.to_string()], |row| row.get::<usize, u32>(0))
            .unwrap();
        Ok(result.count() > 0)
    }

    fn engine_type(&self) -> SQLEngineType {
        SQLEngineType::DuckDB
    }
}

fn initialize_duckdb_connection(conn_state: &mut ConnectionState) {
    unsafe {
        // This follows suggestion [B] outlined in
        // https://blog.knoldus.com/safe-way-to-access-private-fields-in-rust/
        // to access the fields inside of Connection.
        let conn: &duckdb_repr::Connection = std::mem::transmute(&mut conn_state.conn);

        let db_wrapper = conn.db.borrow();
        cppffi::init_arrow_scan(db_wrapper.con as *mut u32);

        // This block installs a replacement scan (https://duckdb.org/docs/api/c/replacement_scans.html)
        // that calls back into our code (replacement_scan_callback) when duckdb encounters a table name
        // it does not know about (i.e. any of our __qsrel_N relations). The replacement for these scans
        // is a function call (arrow_scan_qs) that scans arrow data. This technique is the same method
        // that duckdb uses internally to automatically query python variables (backed by Arrow, Pandas, etc.).
        cffi::duckdb_add_replacement_scan(
            db_wrapper.db,
            Some(replacement_scan_callback),
            &mut conn_state.relations as *mut _ as *mut c_void,
            None,
        );
    }
}
#[derive(Debug)]
struct ConnectionState {
    conn: Connection,
    relations: Arc<Mutex<RelationMap>>,
}
/// This is a Pinned connection which is guaranteed (via the typesystem) to only be accessed
/// by one connection. Therefore, we allow it to be both Send and Sync. Note that the relations
/// state is pinned alongside the connection. That's because we initialize the replacement scan
/// once, while creating the connection, and need its memory to never move while we clear/replace
/// the relations to execute queries with parameters..
#[derive(Debug)]
struct ExclusiveConnection(Pin<Box<ConnectionState>>);
impl ExclusiveConnection {
    fn new(conn: Connection) -> ExclusiveConnection {
        let mut conn = Box::pin(ConnectionState {
            conn,
            relations: Arc::new(Mutex::new(HashMap::new())),
        });
        initialize_duckdb_connection(&mut conn);
        ExclusiveConnection(conn)
    }

    fn try_clone(&mut self) -> Result<ExclusiveConnection> {
        let state = self.0.borrow_mut();
        Ok(ExclusiveConnection(Box::pin(ConnectionState {
            conn: state.conn.try_clone()?,
            relations: state.relations.clone(),
        })))
    }

    fn get_state(&mut self) -> &mut ConnectionState {
        self.0.borrow_mut()
    }
}

unsafe impl Send for ExclusiveConnection {}
unsafe impl Sync for ExclusiveConnection {}

/// DuckDB has a very precarious ownership model, where two concurrent threads "opening" a connection
/// to the same file can cause concurrency problems (namely, they may not see DDL updates). Instead, you
/// need to "clone" from an original connection. Therefore, we funnel all access within the process (and assume
/// that users are careful to not have other threads access the file directly). Luckily, other processes will
/// be blocked via filesystem locks. This singleton class allows us to implement that ownership model, by
/// keeping a single connection open and cloning it as needed for subquent connections.
#[derive(Debug)]
struct ConnectionSingleton(ExclusiveConnection);
impl ConnectionSingleton {
    fn new(url: Option<Arc<crate::compile::ConnectionString>>) -> Result<ConnectionSingleton> {
        let conn = match url {
            Some(url) => Connection::open(url.get_url().path()),
            None => Connection::open_in_memory(),
        }?;

        // This allows us to use JSON functions (e.g. json_extract_string). It should only need to be run once
        // while establishing a new connection.
        conn.execute("INSTALL JSON", [])?;
        conn.execute("LOAD JSON", [])?;

        Ok(Self(ExclusiveConnection::new(conn)))
    }

    fn try_clone(&mut self) -> Result<ExclusiveConnection> {
        self.0.try_clone()
    }
}

lazy_static! {
    static ref DUCKDB_CONNS: Mutex<HashMap<Option<Arc<crate::compile::ConnectionString>>, ConnectionSingleton>> =
        Mutex::new(HashMap::new());
}

#[async_trait::async_trait]
impl SQLEnginePool for DuckDBEngine {
    async fn new(url: Arc<ConnectionString>) -> Result<Box<dyn SQLEngine>> {
        DuckDBEngine::new_conn(Some(url))
    }

    async fn create(url: Arc<ConnectionString>) -> Result<()> {
        // DuckDB will create the database if it doesn't exist.
        let conn = Self::new(url).await?;
        Ok(())
    }
}

impl SQLEmbedded for DuckDBEngine {
    fn new_embedded() -> Result<Box<dyn SQLEngine>> {
        DuckDBEngine::new_conn(None)
    }
}

unsafe fn cast_relation_data(data: *mut u32) -> &'static ArrowRelation {
    &*(data as *const ArrowRelation)
}

#[no_mangle]
pub unsafe extern "C" fn replacement_scan_callback(
    info: cffi::duckdb_replacement_scan_info,
    table_name: *const c_char,
    relation_map: *mut c_void,
) {
    let c_str: &CStr = unsafe { CStr::from_ptr(table_name) };
    let table_str: &str = match c_str.to_str() {
        Ok(s) => s,
        Err(_e) => return,
    };

    let relations = unsafe { &mut *(relation_map as *mut Arc<Mutex<RelationMap>>) };
    let relations = relations.lock().unwrap();
    let relation = match relations.get(table_str) {
        Some(relation) => relation,
        None => return,
    };

    let fn_name = CString::new("arrow_scan_qs").unwrap();
    cffi::duckdb_replacement_scan_set_function_name(info, fn_name.as_ptr());
    unsafe {
        let get_data_fn = cppffi::get_create_stream_fn();

        let mut data_ptr =
            cppffi::duckdb_create_pointer(relation as *const _ as *mut u32) as cffi::duckdb_value;
        let mut get_data_ptr = cppffi::duckdb_create_pointer(get_data_fn) as cffi::duckdb_value;
        let mut get_schema_ptr =
            cppffi::duckdb_create_pointer(get_schema as *mut u32) as cffi::duckdb_value;
        cffi::duckdb_replacement_scan_add_parameter(info, data_ptr);
        cffi::duckdb_replacement_scan_add_parameter(info, get_data_ptr);
        cffi::duckdb_replacement_scan_add_parameter(info, get_schema_ptr);
        cffi::duckdb_destroy_value(&mut data_ptr);
        cffi::duckdb_destroy_value(&mut get_data_ptr);
        cffi::duckdb_destroy_value(&mut get_schema_ptr);
    }
}

#[no_mangle]
pub extern "C" fn get_schema(data: *mut u32, schema_ptr: *mut u32) {
    let relation = unsafe { cast_relation_data(data) };

    let schema_c = FFI_ArrowSchema::try_from(relation.schema.as_ref());
    let schema_c = match schema_c {
        Ok(s) => s,
        Err(e) => {
            // I don't think there's a way to communicate errors to DuckDB in this case
            panic!("Failed to convert to arrow FFI schema: {:?}", e);
        }
    };

    let dest_schema = unsafe { &mut *(schema_ptr as *mut FFI_ArrowSchema) };
    let _old = std::mem::replace(dest_schema, schema_c);
}

// This function is called back through the cppffi bridge from C++ code, to convert the opaque pointer
// into an FFI_ArrowArrayStream
fn rust_build_array_stream(data: *mut u32, fields: &CxxVector<CxxString>, dest: *mut u32) {
    let relation = unsafe { cast_relation_data(data) };
    let mut batch_reader = VecRecordBatchReader::new(relation.clone());

    let schema = batch_reader.schema();
    let field_map = schema
        .all_fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().clone(), i))
        .collect::<std::collections::BTreeMap<String, usize>>();

    let indices: Vec<usize> = fields
        .iter()
        .map(|f| *field_map.get(f.to_str().unwrap()).unwrap())
        .collect();
    batch_reader.set_projection(indices);

    let record_batch = Box::new(batch_reader) as Box<dyn arrow::record_batch::RecordBatchReader>;
    let record_batch_c = *Box::new(FFI_ArrowArrayStream::new(record_batch));

    let dest_record_batch = unsafe { &mut *(dest as *mut FFI_ArrowArrayStream) };
    let _old = std::mem::replace(dest_record_batch, record_batch_c);
}

// We need an object that implements arrow::record_batch::RecordBatchReader to run the iteration
// for the FFI_ArrowArrayStream
struct VecRecordBatchReader {
    data: ArrowRelation,
    idx: usize,
    projection: Option<Vec<usize>>,
}

impl VecRecordBatchReader {
    pub fn new(data: ArrowRelation) -> VecRecordBatchReader {
        VecRecordBatchReader {
            data,
            idx: 0,
            projection: None,
        }
    }

    pub fn set_projection(&mut self, indices: Vec<usize>) {
        self.projection = if indices.len() > 0 {
            Some(indices)
        } else {
            None
        };
    }
}

impl Iterator for VecRecordBatchReader {
    type Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>;

    fn next(
        &mut self,
    ) -> Option<Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>> {
        let rbs = &self.data.relation;
        if self.idx >= rbs.num_batches() {
            None
        } else {
            let batch = rbs.batch(self.idx).as_arrow_recordbatch();
            self.idx += 1;

            Some(match &self.projection {
                Some(fields) => batch.project(&fields),
                None => Ok(batch.clone()),
            })
        }
    }
}

impl arrow::record_batch::RecordBatchReader for VecRecordBatchReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.data.schema.clone()
    }
}

// These structs are copied from the duckdb-rs library. If we change the pinned
// version of duckdb-rs (0.6.0), we should manually update these definitions,
// otherwise the transmute call below will result in undefined behavior.
//
// In addition to these declarations, we've also copied some declarations from
// duckdb.cpp (in duckdb-extra.cc) and the whole duckdb.hpp file (committed directly),
// that we should update too.
#[allow(unused)]
mod duckdb_repr {
    use arrow::datatypes::SchemaRef;
    use duckdb::ffi;
    use hashlink::LruCache;
    use std::cell::RefCell;
    use std::sync::Arc;
    pub struct RawStatement {
        ptr: ffi::duckdb_prepared_statement,
        result: Option<ffi::duckdb_arrow>,
        schema: Option<SchemaRef>,
        // Cached SQL (trimmed) that we use as the key when we're in the statement
        // cache. This is None for statements which didn't come from the statement
        // cache.
        //
        // This is probably the same as `self.sql()` in most cases, but we don't
        // care either way -- It's a better cache key as it is anyway since it's the
        // actual source we got from rust.
        //
        // One example of a case where the result of `sqlite_sql` and the value in
        // `statement_cache_key` might differ is if the statement has a `tail`.
        statement_cache_key: Option<Arc<str>>,
    }

    pub struct StatementCache(RefCell<LruCache<Arc<str>, RawStatement>>);

    pub struct InnerConnection {
        pub db: ffi::duckdb_database,
        pub con: ffi::duckdb_connection,
        owned: bool,
    }
    pub struct Connection {
        /// The raw duckdb connection
        pub db: RefCell<InnerConnection>,

        cache: StatementCache,
        path: Option<std::path::PathBuf>,
    }
}

#[test]
fn test_duckdb_init() {
    ExclusiveConnection::new(Connection::open_in_memory().unwrap());
}

#[test]
fn test_duckdb_concurrency() {
    fn run_query(conn: &mut Connection, query: &str) -> Result<Arc<dyn Relation>> {
        let mut stmt = conn.prepare(query)?;
        let query_result = stmt.query_arrow([])?;
        let ret = ArrowRecordBatchRelation::from_duckdb(query_result);
        Ok(ret)
    }
    let _ = std::fs::remove_file("/tmp/test_duckdb_concurrency.duckdb");
    let url = crate::compile::ConnectionString::maybe_parse(
        None,
        "duckdb:///tmp/test_duckdb_concurrency.duckdb",
        &crate::ast::SourceLocation::Unknown,
    )
    .unwrap()
    .unwrap();

    let mut conn = Connection::open(url.get_url().path()).unwrap();
    run_query(&mut conn, "DROP TABLE IF EXISTS t").unwrap();
    run_query(&mut conn, "CREATE TABLE t AS SELECT 1 AS a").unwrap();

    let mut conn1 = Connection::open(url.get_url().path()).unwrap();

    // NOTE: A prior version of this test opened the connection separately here, which would cause
    // conn1 to "not see" x (https://github.com/wangfenjin/duckdb-rs/issues/117).
    let mut conn2 = conn1.try_clone().unwrap();
    run_query(&mut conn2, "CREATE OR REPLACE VIEW x AS SELECT * FROM t").unwrap();

    run_query(&mut conn2, "SELECT * FROM x").unwrap();

    run_query(&mut conn1, "SELECT * FROM t").unwrap();
    run_query(&mut conn1, "SELECT * FROM x").unwrap();
}

#[test]
fn test_replacemnt_scan() {
    fn run_query(conn: &mut Connection, query: &str) -> Result<Arc<dyn Relation>> {
        let mut stmt = conn.prepare(query)?;
        let query_result = stmt.query_arrow([])?;
        let ret = ArrowRecordBatchRelation::from_duckdb(query_result);
        Ok(ret)
    }
    let _ = std::fs::remove_file("/tmp/test_duckdb_replacement.duckdb");
    let url = crate::compile::ConnectionString::maybe_parse(
        None,
        "duckdb:///tmp/test_duckdb_replacement.duckdb",
        &crate::ast::SourceLocation::Unknown,
    )
    .unwrap()
    .unwrap();

    let mut conn1 = ExclusiveConnection::new(Connection::open(url.get_url().path()).unwrap());
    let _ = run_query(&mut conn1.get_state().conn, "SELECT * FROM dne_1");
    let mut conn2 = conn1.try_clone().unwrap();
    conn1.try_clone().unwrap();
    conn1.try_clone().unwrap();

    let conn1 = conn1.get_state();
    let conn2 = conn2.get_state();

    let _ = run_query(&mut conn1.conn, "SELECT * FROM dne_1");
    let _ = run_query(&mut conn2.conn, "SELECT * FROM dne_2");
    let _ = run_query(&mut conn1.conn, "SELECT * FROM dne_1");
}

#[test]
fn test_table_not_exists() {
    let mut conn1 = ExclusiveConnection::new(Connection::open_in_memory().unwrap());
    let conn = &mut conn1.get_state().conn;

    let foo = Into::<sqlast::ObjectName>::into(&Into::<Ident>::into("foo"));

    let mut stmt = conn
        .prepare("SELECT 1 FROM information_schema.tables WHERE table_name = ?")
        .unwrap();
    let result = stmt
        .query_map(&[&foo.to_string()], |row| row.get::<usize, u32>(0))
        .unwrap();
    assert!(result.count() == 0)
}
