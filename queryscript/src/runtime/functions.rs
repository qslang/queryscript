use arrow::{
    datatypes::{DataType as ArrowDataType, Schema as ArrowSchema},
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt};
use std::path::{Path as FilePath, PathBuf as FilePathBuf};
use std::sync::Arc;

use crate::compile::schema;
use crate::{
    types,
    types::{arrow::ArrowRecordBatchRelation, FnValue, Value},
};

use super::{
    error::{fail, Result, RuntimeError},
    runtime, Context,
};

type TypeRef = schema::Ref<types::Type>;

#[derive(Clone, Debug)]
pub struct QSFn {
    type_: types::FnType,
    body: schema::TypedExpr<TypeRef>,
}

impl QSFn {
    pub fn new(type_: TypeRef, body: Arc<schema::Expr<TypeRef>>) -> Result<Value> {
        let type_ = match &*type_.read()? {
            types::Type::Fn(f) => f.clone(),
            _ => return fail!("Function must have function type"),
        };
        let body_type = schema::mkref(type_.ret.as_ref().clone());
        Ok(Value::Fn(Arc::new(QSFn {
            type_,
            body: schema::TypedExpr {
                type_: body_type,
                expr: body.clone(),
            },
        })))
    }
}

#[async_trait]
impl FnValue for QSFn {
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
    schema: Arc<ArrowSchema>,
}

impl LoadFileFn {
    pub fn new(type_: &types::Type) -> Result<LoadFileFn> {
        let ret_type = match type_ {
            types::Type::Fn(types::FnType { ret, .. }) => ret,
            _ => return fail!("Type of load is not a function"),
        };

        let mut schema = None;
        if let ArrowDataType::List(dt) = ret_type.as_ref().try_into()? {
            if let ArrowDataType::Struct(s) = dt.data_type() {
                schema = Some(Arc::new(ArrowSchema::new(s.clone())));
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

    pub async fn load(
        &self,
        ctx: &Context,
        file_path: &FilePath,
        format: Option<String>,
    ) -> Result<Value> {
        let format_type = Self::derive_format(file_path, &format);

        let records = ctx.expensive(move || {
            let fd = std::fs::File::open(file_path)?;

            // NOTES:
            // - This reads the entire file into memory, and then operates over it. We could
            //   instead implement the Relation attribute for each Reader (or for Iterator<Item=RecordBatch>).
            // - DataFusion implements an async reader for non-files (i.e. streams that are already async) by reading
            //   newline delimited chunks of the file. We could do something like that to leverage async file reading.
            // - The parquet library actually supports async reading, which we could do in a separate branch
            let records = Arc::new(match format_type {
                Format::Csv => {
                    let reader = arrow::csv::ReaderBuilder::new()
                        .has_header(true)
                        .with_schema(self.schema.clone())
                        .build(fd)?;

                    reader.collect::<Result<Vec<RecordBatch>, ArrowError>>()
                }
                Format::Json => {
                    let reader = arrow::json::ReaderBuilder::new()
                        .with_schema(self.schema.clone())
                        .build(fd)?;

                    reader.collect::<Result<Vec<RecordBatch>, ArrowError>>()
                }
                Format::Parquet => {
                    let reader =
                        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(fd)?
                            .build()?;

                    if reader.schema() != self.schema {
                        return fail!(
                            "Parquet file {:?} has a different schema than the target variable's type",
                            file_path
                        );
                    }

                    reader.collect::<Result<Vec<RecordBatch>, ArrowError>>()
                }
            }? as Vec<RecordBatch>);

            Ok::<Arc<Vec<_>>, RuntimeError>(records)
        })?;

        Ok(Value::Relation(ArrowRecordBatchRelation::new(
            self.schema.clone(),
            records,
        )))
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
            us.load(ctx, &*path_buf, format).await
        }
        .boxed()
    }

    fn fn_type(&self) -> types::FnType {
        types::FnType {
            args: vec![types::Field {
                name: "file".into(),
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

#[derive(Clone, Debug)]
pub struct MaterializeFn {
    type_: types::FnType,
}

impl MaterializeFn {
    pub fn new(type_: &types::Type) -> Result<MaterializeFn> {
        let type_ = match type_ {
            types::Type::Fn(fn_type) => fn_type.clone(),
            _ => return fail!("Type of materialize is not a function"),
        };

        Ok(MaterializeFn { type_ })
    }
}

#[async_trait]
impl FnValue for MaterializeFn {
    fn execute<'a>(&'a self, ctx: &'a Context, args: Vec<Value>) -> BoxFuture<'a, Result<Value>> {
        async move { Ok(args.into_iter().next().unwrap()) }.boxed()
    }

    fn fn_type(&self) -> types::FnType {
        self.type_.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Clone, Debug)]
pub struct IdentityFn {
    type_: types::FnType,
}

impl IdentityFn {
    pub fn new(type_: &types::Type) -> Result<IdentityFn> {
        let type_ = match type_ {
            types::Type::Fn(fn_type) => fn_type.clone(),
            _ => return fail!("Type of identity is not a function"),
        };

        Ok(IdentityFn { type_ })
    }
}

#[async_trait]
impl FnValue for IdentityFn {
    fn execute<'a>(&'a self, _ctx: &'a Context, args: Vec<Value>) -> BoxFuture<'a, Result<Value>> {
        async move { Ok(args.into_iter().next().unwrap()) }.boxed()
    }

    fn fn_type(&self) -> types::FnType {
        self.type_.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
