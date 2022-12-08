use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use duckdb::{params, Connection, Result};

// In your project, we need to keep the arrow version same as the version used in duckdb.
// Refer to https://github.com/wangfenjin/duckdb-rs/issues/92
// You can either:
use duckdb::arrow::record_batch::RecordBatch;
// Or in your Cargo.toml, use * as the version; features can be toggled according to your needs
// arrow = { version = "*", default-features = false, features = ["prettyprint"] }
// Then you can:
// use arrow::record_batch::RecordBatch;

use duckdb::arrow::util::pretty::print_batches;

use duckdb::ffi;
use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr;
use std::time::Instant;

mod duckdbcpp;

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

#[allow(dead_code)]
#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

#[no_mangle]
pub extern "C" fn get_schema(data: *mut u32, schema_ptr: *mut u32) {
    eprintln!("IN GET_SCHEMA");
    let rbs = unsafe { &mut *(data as *mut Vec<RecordBatch>) };
    let first = rbs[0].schema();
    let schema_c = unsafe { arrow::ffi::FFI_ArrowSchema::try_from(first.as_ref()) };
    let schema_c = match schema_c {
        Ok(s) => s,
        Err(e) => {
            eprintln!("FAILED BECAUSE OF: {:?}", e);
            return;
        }
    };
    let schema_ref = unsafe { &mut *(schema_ptr as *mut arrow::ffi::FFI_ArrowSchema) };
    let _old = std::mem::replace(schema_ref, schema_c);
    eprintln!("SCHEMA REF: {:?}", schema_ref);
    eprintln!("FINISHED SCHEMA");
}

#[no_mangle]
pub unsafe extern "C" fn replacement_scan_callback(
    info: ffi::duckdb_replacement_scan_info,
    table_name: *const c_char,
    data: *mut c_void,
) {
    let c_str: &CStr = unsafe { CStr::from_ptr(table_name) };
    let str_slice: &str = match c_str.to_str() {
        Ok(s) => s,
        Err(_e) => return,
    };

    let rbs = unsafe { &mut *(data as *mut Vec<RecordBatch>) };
    eprintln!("PRINTING FROM CALLBACK");
    print_batches(&rbs).unwrap();

    let fn_name = CString::new("arrow_scan_qvm").unwrap();
    ffi::duckdb_replacement_scan_set_function_name(info, fn_name.as_ptr());
    unsafe {
        let get_data_fn = duckdbcpp::bridge::get_create_stream_fn();

        let mut data_ptr =
            duckdbcpp::bridge::duckdb_create_pointer(data as *mut u32) as ffi::duckdb_value;
        let mut get_data_ptr =
            duckdbcpp::bridge::duckdb_create_pointer(get_data_fn) as ffi::duckdb_value;
        let mut get_schema_ptr =
            duckdbcpp::bridge::duckdb_create_pointer(get_schema as *mut u32) as ffi::duckdb_value;
        ffi::duckdb_replacement_scan_add_parameter(info, data_ptr);
        ffi::duckdb_replacement_scan_add_parameter(info, get_data_ptr);
        ffi::duckdb_replacement_scan_add_parameter(info, get_schema_ptr);
        ffi::duckdb_destroy_value(&mut data_ptr);
        ffi::duckdb_destroy_value(&mut get_data_ptr);
        ffi::duckdb_destroy_value(&mut get_schema_ptr);
    }
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    let now = Instant::now();
    conn.execute_batch(
        r"CREATE SEQUENCE seq;
          CREATE TABLE person (
                  id              INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
                  name            TEXT NOT NULL,
                  data            BLOB
                  );
        ",
    )?;
    eprintln!("create table {:?}", now.elapsed());

    eprintln!("FIRST CONNECTION");
    print_batches(
        &conn
            .prepare("SHOW TABLES")?
            .query_arrow([])?
            .collect::<Vec<_>>(),
    )
    .unwrap();

    eprintln!("CLONED CONNECTION");
    print_batches(
        &conn
            .try_clone()
            .unwrap()
            .prepare("SHOW TABLES")?
            .query_arrow([])?
            .collect::<Vec<_>>(),
    )
    .unwrap();

    eprintln!("NEW CONNECTION");
    print_batches(
        &Connection::open_in_memory()?
            .prepare("SHOW TABLES")?
            .query_arrow([])?
            .collect::<Vec<_>>(),
    )
    .unwrap();

    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        data: None,
    };
    let now = Instant::now();
    conn.execute(
        "INSERT INTO person (name, data) VALUES (?, ?)",
        params![me.name, me.data],
    )?;
    eprintln!("insert {:?}", now.elapsed());

    // query table by rows
    let now = Instant::now();
    let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: row.get(1)?,
            data: row.get(2)?,
        })
    })?;

    for person in person_iter {
        eprintln!("Found person {:?}", person.unwrap());
    }
    eprintln!("select {:?}", now.elapsed());

    let now = Instant::now();
    // query table by arrow
    let mut rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    print_batches(&rbs).unwrap();
    eprintln!("arrow stuff {:?}", now.elapsed());

    unsafe {
        // This follows suggestion [B] outlined in
        // https://blog.knoldus.com/safe-way-to-access-private-fields-in-rust/
        // to access the fields inside of Connection.
        let conn: &duckdb_repr::Connection = std::mem::transmute(&conn);

        let db_wrapper = conn.db.borrow();
        duckdbcpp::bridge::init_arrow_scan(db_wrapper.con as *mut u32);

        ffi::duckdb_add_replacement_scan(
            db_wrapper.db,
            Some(replacement_scan_callback),
            &mut rbs as *mut _ as *mut c_void,
            None,
        );
    }

    let mut stmt = conn.prepare("SELECT data, id FROM \"arrow\" WHERE id > 10")?;
    let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    print_batches(&rbs).unwrap();
    eprintln!("replacement scan stuff {:?}", now.elapsed());

    Ok(())
}
