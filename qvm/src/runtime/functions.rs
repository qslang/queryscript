use async_trait::async_trait;
use datafusion::arrow::{
    datatypes::{DataType as DFDataType, Schema as DFSchema},
    record_batch::RecordBatch,
};
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::{
    csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat,
};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::file_format::FileScanConfig;
use futures::future::{BoxFuture, FutureExt};
use object_store::path::Path;
use object_store::ObjectMeta;
use std::path::{Path as FilePath, PathBuf as FilePathBuf};
use std::sync::Arc;

use crate::compile::schema;
use crate::{
    types,
    types::{FnValue, Value},
};

use super::{
    error::{fail, Result},
    runtime, Context,
};

type TypeRef = schema::Ref<types::Type>;

#[derive(Clone, Debug)]
pub struct QVMFn {
    type_: types::FnType,
    body: schema::TypedExpr<TypeRef>,
}

impl QVMFn {
    pub fn new(type_: TypeRef, body: Arc<schema::Expr<TypeRef>>) -> Result<Value> {
        let type_ = match &*type_.read()? {
            types::Type::Fn(f) => f.clone(),
            _ => return fail!("Function must have function type"),
        };
        let body_type = schema::mkref(type_.ret.as_ref().clone());
        Ok(Value::Fn(Arc::new(QVMFn {
            type_,
            body: schema::TypedExpr {
                type_: body_type,
                expr: body.clone(),
            },
        })))
    }
}

#[async_trait]
impl FnValue for QVMFn {
    fn execute(&self, ctx: &Context, args: Vec<Value>) -> BoxFuture<Result<Value>> {
        let new_ctx = ctx.clone();
        let type_ = self.type_.clone();
        let body = self.body.clone();
        async move {
            let mut new_ctx = new_ctx.clone();
            if args.len() != type_.args.len() {
                return fail!("Wrong number of arguments to function");
            }

            for i in 0..args.len() {
                // Can we avoid cloning the value here
                //
                new_ctx
                    .values
                    .insert(type_.args[i].name.clone(), args[i].clone());
            }

            runtime::eval(&new_ctx, &body).await
        }
        .boxed()
    }

    fn fn_type(&self) -> types::FnType {
        self.type_.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Clone, Debug)]
enum Format {
    Json,
    Csv,
    Parquet,
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

    fn derive_format(file: &FilePath, format: &Option<String>) -> Format {
        let format_lower = format.as_ref().map(|s| s.to_lowercase());
        let format_name = match format_lower.as_deref() {
            None => file.extension().and_then(|s| s.to_str()),
            Some(fmt) => Some(fmt),
        };

        match format_name {
            Some("csv") => Format::Csv,
            Some("parquet") => Format::Parquet,
            Some("json") | _ => Format::Json,
        }
    }

    // XXX We should change this to use arrow directly
    pub async fn load(&self, file: &FilePath, format: Option<String>) -> Result<Value> {
        let format_type = Self::derive_format(file, &format);

        let format: Box<dyn FileFormat> = match format_type {
            Format::Json => Box::new(JsonFormat::default().with_schema_infer_max_rec(Some(0)))
                as Box<dyn FileFormat>,
            Format::Csv => Box::new(CsvFormat::default().with_schema_infer_max_rec(Some(0)))
                as Box<dyn FileFormat>,
            Format::Parquet => Box::new(ParquetFormat::default()) as Box<dyn FileFormat>,
        };
        let location = Path::from_filesystem_path(&file)?;
        let fmeta = std::fs::metadata(&file)?;
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

    fn parse_args(ctx: &Context, args: Vec<Value>) -> Result<(FilePathBuf, Option<String>)> {
        if args.len() != 2 {
            return fail!("load expects exactly 2 arguments");
        }

        let mut path_buf = FilePathBuf::new();
        if let Some(folder) = &ctx.folder {
            path_buf.push(folder.clone());
        }

        match &args[0] {
            Value::Utf8(s) => path_buf.push(s),
            _ => return fail!("load expects its first argument to be a string"),
        };

        let format = match &args[1] {
            Value::Utf8(s) => Some(s.clone()),
            Value::Null => None,
            _ => return fail!("load expects a string or null as the second argument"),
        };

        Ok((path_buf, format))
    }

    pub async fn infer(
        ctx: &Context,
        args: Vec<Value>,
    ) -> crate::runtime::Result<crate::types::Type> {
        let (file_path, format) = Self::parse_args(ctx, args)?;
        let file_path = &*file_path;
        let format_type = Self::derive_format(file_path, &format);

        ctx.expensive(move || {
            let fd = std::fs::File::open(file_path)?;

            Ok(crate::types::Type::List(Box::new(match format_type {
                Format::Csv => {
                    let reader = arrow::csv::ReaderBuilder::new()
                        .infer_schema(Some(100))
                        .has_header(true)
                        .build(fd)?;

                    let schema = reader.schema();
                    schema.as_ref().try_into()?
                }
                Format::Json => {
                    let reader = arrow::json::ReaderBuilder::new()
                        .infer_schema(Some(100))
                        .build(fd)?;

                    let schema = reader.schema();
                    schema.as_ref().try_into()?
                }
                Format::Parquet => {
                    let reader =
                        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(fd)?;
                    let schema = reader.schema();
                    schema.as_ref().try_into()?
                }
            })))
        })
    }
}

#[async_trait]
impl FnValue for LoadFileFn {
    fn execute<'a>(&'a self, ctx: &'a Context, args: Vec<Value>) -> BoxFuture<'a, Result<Value>> {
        let us = self.clone();
        async move {
            let (path_buf, format) = Self::parse_args(ctx, args)?;
            us.load(&*path_buf, format).await
        }
        .boxed()
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
