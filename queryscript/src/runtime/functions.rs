use arrow::{
    datatypes::{DataType as ArrowDataType, Schema as ArrowSchema},
    error::ArrowError,
    record_batch::{RecordBatch as ArrowRecordBatch, RecordBatchReader},
};
use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt};
use sqlparser::ast as sqlast;
use std::{any::Any, collections::HashMap, sync::Arc};
use std::{
    path::{Path as FilePath, PathBuf as FilePathBuf},
    sync::Mutex,
};

use crate::{
    compile::{
        schema::{self, Ident},
        sql::{select_limit_0, select_star_from},
    },
    types::{
        self, arrow::ArrowRecordBatchRelation, value::RecordBatch, Field, FnValue, LazyValue,
        LazyValueClone, Relation, Type, TypesystemError, Value,
    },
};

use super::{
    embedded_engine,
    error::{fail, Result, RuntimeError},
    runtime, Context, LazySQLParam, SQLEngineType,
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
    fn execute(
        &self,
        ctx: &mut Context,
        args: Vec<Value>,
    ) -> BoxFuture<Result<Box<dyn LazyValue>>> {
        let mut new_ctx = ctx.clone();
        let type_ = self.type_.clone();
        let body = self.body.clone();
        async move {
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

            runtime::eval_lazy(&mut new_ctx, &body).await
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
pub enum Format {
    Json,
    Csv,
    Parquet,
}

#[derive(Clone, Debug)]
pub struct LoadFileFn {
    schema: Arc<Vec<Field>>,
}

impl LoadFileFn {
    pub fn new(type_: &types::Type) -> Result<LoadFileFn> {
        let ret_type = match type_ {
            types::Type::Fn(types::FnType { ret, .. }) => ret,
            _ => return fail!("Type of load is not a function"),
        };

        let mut schema = None;
        if let Type::List(dt) = ret_type.as_ref() {
            if let Type::Record(s) = dt.as_ref() {
                schema = Some(Arc::new(s.clone()));
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

    fn parse_args(ctx: &Context, args: Vec<Value>) -> Result<(FilePathBuf, Option<String>)> {
        if args.len() != 2 {
            return fail!("load expects exactly 2 arguments");
        }

        let file_path = match &args[0] {
            Value::Utf8(s) => s,
            _ => return fail!("load expects its first argument to be a string"),
        };

        let is_remote = match url::Url::parse(file_path) {
            Ok(url) => url.scheme() != "file",
            Err(_) => false,
        };

        let mut path_buf = FilePathBuf::new();
        if let (false, Some(folder)) = (is_remote, &ctx.folder) {
            path_buf.push(folder.clone());
        }

        path_buf.push(file_path);

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
        let (path_buf, format) = Self::parse_args(ctx, args)?;
        let format = Self::derive_format(&path_buf, &format);
        let mut lazy_value = LazyFileRelation {
            schema: None,
            file_path: path_buf,
            format,
            schema_only: true,
        };

        let relation = match lazy_value.get().await? {
            Value::Relation(r) => r,
            _ => unreachable!("load should return a relation"),
        };

        Ok(Type::List(Box::new(Type::Record(relation.schema()))))
    }
}

#[async_trait]
impl FnValue for LoadFileFn {
    fn execute<'a>(
        &'a self,
        ctx: &'a mut Context,
        args: Vec<Value>,
    ) -> BoxFuture<'a, Result<Box<dyn LazyValue>>> {
        let schema = self.schema.clone();
        async move {
            let (path_buf, format) = Self::parse_args(ctx, args)?;
            let format = Self::derive_format(&path_buf, &format);
            Ok(Box::new(LazyFileRelation {
                schema: Some(schema),
                file_path: path_buf,
                format,
                schema_only: false,
            }) as Box<dyn LazyValue>)
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
            ret: Box::new(Type::List(Box::new(Type::Record(
                self.schema.as_ref().clone(),
            )))),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct LazyFileRelation {
    pub schema: Option<Arc<Vec<Field>>>,
    pub file_path: FilePathBuf,
    pub format: Format,
    pub schema_only: bool,
}

#[async_trait]
impl LazyValue for LazyFileRelation {
    async fn get(&mut self) -> Result<Value> {
        let table_name = Ident::from("__qs_inference");
        let mut params = HashMap::new();
        params.insert(
            table_name.clone(),
            LazySQLParam::new(
                table_name.clone(),
                self.clone_box(),
                &crate::types::Type::Atom(crate::types::AtomicType::Null),
            ),
        );

        let mut query = select_star_from(&table_name);
        if self.schema_only {
            query = select_limit_0(query);
        }
        let query = sqlast::Statement::Query(Box::new(query));
        let mut engine = embedded_engine(SQLEngineType::DuckDB);

        let mut relation = engine.query(&query, params).await?;

        if let Some(schema) = &self.schema {
            relation = relation.try_cast(schema.as_ref())?;
        }

        Ok(Value::Relation(relation))
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
    fn execute<'a>(
        &'a self,
        _ctx: &'a mut Context,
        args: Vec<Value>,
    ) -> BoxFuture<'a, Result<Box<dyn LazyValue>>> {
        async move { Ok(args.into_iter().next().unwrap().into()) }.boxed()
    }

    fn fn_type(&self) -> types::FnType {
        self.type_.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
