use datafusion::arrow::{
    datatypes::{DataType as DFDataType, Schema as DFSchema},
    record_batch::RecordBatch,
};
use datafusion::common::{Result as DFResult, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::FileFormat;
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
pub struct LoadJsonFn {
    schema: Arc<DFSchema>,
}

impl LoadJsonFn {
    pub fn new(type_: &types::Type) -> Result<LoadJsonFn> {
        let ret_type = match type_ {
            types::Type::Fn(types::FnType { ret, .. }) => ret,
            _ => return fail!("Type of load_json is not a function"),
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
                    "Return type of load_json ({:?}) is not a list of records",
                    ret_type.as_ref(),
                )
            }
        };

        Ok(LoadJsonFn { schema })
    }

    pub fn load_json(&self, file: String) -> Result<Value> {
        let format = JsonFormat::default().with_schema_infer_max_rec(Some(0));

        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let location = Path::from_filesystem_path(file.as_str())?;
        let fmeta = std::fs::metadata(file)?;
        let ometa = ObjectMeta {
            location,
            last_modified: fmeta.modified().map(chrono::DateTime::from).unwrap(),
            size: fmeta.len() as usize,
        };
        // XXX We should make these functions async as datafusion's execution already is
        let records = runtime.block_on(async {
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

            let ctx = SessionContext::with_config_rt(
                SessionConfig::new(),
                Arc::new(RuntimeEnv::default()),
            );
            let task_ctx = ctx.task_ctx();
            return DFResult::Ok(Arc::new(collect(plan, task_ctx).await? as Vec<RecordBatch>));
        })?;

        Ok(Value::Relation(records))
    }
}

impl FnValue for LoadJsonFn {
    fn execute(&self, args: Vec<Value>) -> Result<Value> {
        if args.len() != 1 {
            return fail!("load_json expects exactly one argument");
        }

        let file = match &args[0] {
            Value::Utf8(s) => s.clone(),
            _ => return fail!("load_json expects its first argument to be a string"),
        };

        self.load_json(file)
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
