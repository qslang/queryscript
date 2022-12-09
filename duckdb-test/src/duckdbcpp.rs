use arrow::ffi::FFI_ArrowSchema;
use arrow::record_batch::RecordBatchReader;
use cxx::{CxxString, CxxVector};
use duckdb::arrow::record_batch::RecordBatch;

#[cxx::bridge]
pub mod bridge {
    extern "Rust" {
        unsafe fn rust_build_array_stream(
            data: *mut u32,
            fields: &CxxVector<CxxString>,
        ) -> *mut u32;
    }
    unsafe extern "C++" {
        include!("duckdb-test/include/duckdb-extra.hpp");

        type ArrowArrayStreamWrapper;
        type Value;

        unsafe fn get_create_stream_fn() -> *mut u32; // XXX should probably return a const
        unsafe fn duckdb_create_pointer(value: *mut u32) -> *mut Value;
        unsafe fn init_arrow_scan(connection_ptr: *mut u32);
    }
}
struct VecRecordBatchReader {
    data: *mut u32,
    idx: usize,
    projection: Option<Vec<usize>>,
}

impl VecRecordBatchReader {
    pub fn new(data: *mut u32) -> VecRecordBatchReader {
        VecRecordBatchReader {
            data,
            idx: 0,
            projection: None,
        }
    }

    pub fn data_ref(&self) -> &Vec<RecordBatch> {
        unsafe { &*(self.data as *const Vec<RecordBatch>) }
    }

    pub fn set_projection(&mut self, indices: Vec<usize>) {
        self.projection = Some(indices);
    }
}

impl Iterator for VecRecordBatchReader {
    type Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>;

    fn next(
        &mut self,
    ) -> Option<Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>> {
        eprintln!("IN NEXT");
        let rbs = self.data_ref();
        if self.idx >= rbs.len() {
            eprintln!("HAVE NOTHING");
            None
        } else {
            let batch = &rbs[self.idx];

            let ret = match &self.projection {
                Some(fields) => batch.project(&fields),
                None => Ok(batch.clone()),
            };
            self.idx += 1;
            Some(ret)
        }
    }
}

impl arrow::record_batch::RecordBatchReader for VecRecordBatchReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.data_ref()[0].schema()
    }
}

fn rust_build_array_stream(data: *mut u32, fields: &CxxVector<CxxString>) -> *mut u32 {
    let mut batch_reader = VecRecordBatchReader::new(data);

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

    let record_batch_c = Box::new(arrow::ffi_stream::FFI_ArrowArrayStream::new(record_batch));

    let mut foo = arrow::ffi::FFI_ArrowSchema::empty();
    let record_batch_p = Box::into_raw(record_batch_c);
    unsafe {
        (*record_batch_p).get_schema.expect("get_schema_fn")(
            record_batch_p,
            &mut foo as *mut FFI_ArrowSchema,
        )
    };

    record_batch_p as *mut u32
}
