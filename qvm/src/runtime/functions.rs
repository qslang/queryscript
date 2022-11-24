use async_trait::async_trait;
use datafusion::arrow::{
    datatypes::{DataType as DFDataType, Schema as DFSchema},
    record_batch::RecordBatch,
};
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::{csv::CsvFormat, json::JsonFormat};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::file_format::FileScanConfig;
use object_store::path::Path;
use object_store::ObjectMeta;
use std::sync::Arc;

use super::error::{fail, Result};
use crate::types;
use crate::types::{FnValue, Value};

#[derive(Clone, Debug)]
enum Format {
    Json,
    CSV,
}

#[derive(Clone, Debug)]
pub struct LoadFileFn {
    schema: Arc<DFSchema>,
}

impl LoadFileFn {
    pub fn new(type_: &types::Type) -> Result<LoadFileFn> {
        let ret_type = match type_ {
            types::Type::Fn(types::FnType { ret, .. }) => ret,
            _ => return fail!("Type of load is not a function"),
        };

        let mut schema = None;
        if let DFDataType::List(dt) = ret_type.as_ref().try_into()? {
            if let DFDataType::Struct(s) = dt.data_type() {
                schema = Some(Arc::new(DFSchema::new(s.clone())));
            }
        }

        let schema = match schema {
            Some(schema) => schema,
            None => {
                return fail!(
                    "Return type of load ({:?}) is not a list of records",
                    ret_type.as_ref(),
                )
            }
        };

        Ok(LoadFileFn { schema })
    }

    pub async fn load(&self, file: String, format: Option<String>) -> Result<Value> {
        let format_type = match format.map(|s| s.to_lowercase()).as_deref() {
            None => {
                if file.ends_with(".csv") {
                    Format::CSV
                } else {
                    Format::Json
                }
            }
            Some("csv") => Format::CSV,
            Some("json") | _ => Format::Json,
        };
        let format: Box<dyn FileFormat> = match format_type {
            Format::Json => Box::new(JsonFormat::default().with_schema_infer_max_rec(Some(0)))
                as Box<dyn FileFormat>,
            Format::CSV => Box::new(CsvFormat::default().with_schema_infer_max_rec(Some(0)))
                as Box<dyn FileFormat>,
        };

        let location = Path::from_filesystem_path(file.as_str())?;
        let fmeta = std::fs::metadata(file)?;
        let ometa = ObjectMeta {
            location,
            last_modified: fmeta.modified().map(chrono::DateTime::from).unwrap(),
            size: fmeta.len() as usize,
        };

        let plan = format
            .create_physical_plan(
                FileScanConfig {
                    file_schema: self.schema.clone(),
                    file_groups: vec![vec![ometa.into()]],
                    limit: None,
                    object_store_url: ObjectStoreUrl::local_filesystem(),
                    projection: None,
                    statistics: Statistics {
                        num_rows: None,
                        total_byte_size: None,
                        column_statistics: None,
                        is_exact: true,
                    },
                    table_partition_cols: Vec::new(),
                    config_options: ConfigOptions::new().into_shareable(),
                },
                &[],
            )
            .await?;

        let ctx =
            SessionContext::with_config_rt(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let task_ctx = ctx.task_ctx();
        let records = Arc::new(collect(plan, task_ctx).await? as Vec<RecordBatch>);

        Ok(Value::Relation(records))
    }
}

#[async_trait]
impl FnValue for LoadFileFn {
    async fn execute(&self, args: Vec<Value>) -> Result<Value> {
        if args.len() < 1 || args.len() > 2 {
            return fail!("load expects exactly 1-2 arguments");
        }

        let file = match &args[0] {
            Value::Utf8(s) => s.clone(),
            _ => return fail!("load expects its first argument to be a string"),
        };

        let mut format = None;
        if args.len() > 1 {
            match &args[1] {
                Value::Utf8(s) => format = Some(s.clone()),
                _ => return fail!("load expects its second argument to be a string"),
            }
        };

        self.load(file, format).await
    }

    fn fn_type(&self) -> types::FnType {
        types::FnType {
            args: vec![types::Field {
                name: "file".to_string(),
                type_: types::Type::Atom(types::AtomicType::Utf8),
                nullable: false,
            }],
            ret: Box::new(
                (&self.schema.fields)
                    .try_into()
                    .expect("Failed to convert function type back"),
            ),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
