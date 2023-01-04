use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::Arc;

use arrow::{
    datatypes::SchemaRef as ArrowSchemaRef, ffi::FFI_ArrowSchema, ffi_stream::FFI_ArrowArrayStream,
    record_batch::RecordBatch, record_batch::RecordBatchReader,
};
use cxx::{CxxString, CxxVector};
use duckdb::{ffi as cffi, Connection};
use sqlparser::ast as sqlast;

use crate::types::{arrow::ArrowRecordBatchRelation, Relation, Value};

use crate::runtime::{
    error::{rt_unimplemented, Result},
    normalize::Normalizer,
    sql::{SQLEngine, SQLParam},
    Context,
};

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
        include!("qvm/include/duckdb-extra.hpp");

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

impl DuckDBNormalizer {
    pub fn new(params: &[String]) -> DuckDBNormalizer {
        DuckDBNormalizer {
            params: params
                .iter()
                .enumerate()
                .map(|(i, s)| (s.to_string(), format!("${}", i + 1)))
                .collect(),
        }
    }
}

impl Normalizer for DuckDBNormalizer {
    fn quote_style(&self) -> Option<char> {
        Some('"')
    }

    fn params(&self) -> &HashMap<String, String> {
        &self.params
    }
}

#[derive(Debug, Clone)]
pub struct ArrowRelation {
    pub relation: Arc<Vec<RecordBatch>>,
    pub schema: ArrowSchemaRef,
}

type RelationMap = HashMap<String, ArrowRelation>;

#[derive(Debug)]
pub struct DuckDBEngine();

impl DuckDBEngine {
    pub fn new() -> DuckDBEngine {
        DuckDBEngine()
    }

    fn eval_in_place(
        &self,
        query: &sqlast::Query,
        params: HashMap<String, SQLParam>,
    ) -> Result<Arc<dyn Relation>> {
        // The code below works by (globally within the connection) installing a replacement
        // scan that accesses the relations referenced in the query parameters. I did some light
        // benchmarking and the cost to create a connection seemed relatively low, but we could
        // potentially pool or concurrently use these connections if necessary.
        let conn = Connection::open_in_memory()?;

        unsafe {
            // This follows suggestion [B] outlined in
            // https://blog.knoldus.com/safe-way-to-access-private-fields-in-rust/
            // to access the fields inside of Connection.
            let conn: &duckdb_repr::Connection = std::mem::transmute(&conn);

            let db_wrapper = conn.db.borrow();
            cppffi::init_arrow_scan(db_wrapper.con as *mut u32);
        }

        let mut scalar_params = Vec::new();
        let mut relations = RelationMap::new();

        for (key, param) in params.iter() {
            match &param.value {
                Value::Relation(r) => {
                    relations.insert(
                        key.clone(),
                        ArrowRelation {
                            relation: r.clone().as_arrow_recordbatch(),
                            schema: Arc::new((&param.type_).try_into()?),
                        },
                    );
                }
                Value::Fn(_) => {
                    return rt_unimplemented!("Function parameters");
                }
                _ => {
                    scalar_params.push(key.clone());
                }
            }
        }

        // This block installs a replacement scan (https://duckdb.org/docs/api/c/replacement_scans.html)
        // that calls back into our code (replacement_scan_callback) when duckdb encounters a table name
        // it does not know about (i.e. any of our __qvmrel_N relations). The replacement for these scans
        // is a function call (arrow_scan_qvm) that scans arrow data. This technique is the same method
        // that duckdb uses internally to automatically query python variables (backed by Arrow, Pandas, etc.).
        unsafe {
            // This follows suggestion [B] outlined in
            // https://blog.knoldus.com/safe-way-to-access-private-fields-in-rust/
            // to access the fields inside of Connection.
            let conn: &duckdb_repr::Connection = std::mem::transmute(&conn);

            let db_wrapper = conn.db.borrow();
            cffi::duckdb_add_replacement_scan(
                db_wrapper.db,
                Some(replacement_scan_callback),
                &mut relations as *mut _ as *mut c_void,
                None,
            );
        }

        scalar_params.sort();

        let normalizer = DuckDBNormalizer::new(&scalar_params);
        let query = normalizer.normalize(&query);
        let query_string = format!("{}", query);

        let duckdb_params: Vec<&dyn duckdb::ToSql> = scalar_params
            .iter()
            .map(|k| &params.get(k).unwrap().value as &dyn duckdb::ToSql)
            .collect();

        let mut stmt = conn.prepare(&query_string)?;

        let query_result = stmt.query_arrow(duckdb_params.as_slice())?;

        // NOTE: We could probably avoid collecting the whole result here and instead make
        // ArrowRecordBatchRelation accept an iterator as input.
        let rbs = ArrowRecordBatchRelation::new(
            query_result.get_schema(),
            Arc::new(query_result.collect::<Vec<RecordBatch>>()),
        );
        Ok(rbs)
    }
}

#[async_trait::async_trait]
impl SQLEngine for DuckDBEngine {
    async fn eval(
        &self,
        ctx: &Context,
        query: &sqlast::Query,
        params: HashMap<String, SQLParam>,
    ) -> Result<Arc<dyn Relation>> {
        // We call block_in_place here because DuckDB may perform computationally expensive work,
        // and it's not hooked into the async coroutines that the runtime uses (and therefore cannot
        // yield work). block_in_place() tells Tokio to expect this thread to spend a while working on
        // this stuff and use other threads for other work.
        ctx.expensive(|| self.eval_in_place(query, params))
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

    let relations = unsafe { &mut *(relation_map as *mut RelationMap) };
    let relation = match relations.get(table_str) {
        Some(relation) => relation,
        None => return,
    };

    let fn_name = CString::new("arrow_scan_qvm").unwrap();
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
        let rbs = self.data.relation.as_ref();
        if self.idx >= rbs.len() {
            None
        } else {
            let batch = &rbs[self.idx];
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
