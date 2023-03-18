use snafu::prelude::*;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, LinkedList};
use std::fmt;
use std::fs;
use std::path::Path as FilePath;
use std::sync::Arc;

use crate::compile::builtin_types::{BUILTIN_LOC, GLOBAL_GENERICS, GLOBAL_SCHEMA};
use crate::compile::connection::{ConnectionSchema, ConnectionString};
use crate::compile::error::*;
use crate::compile::inference::*;
use crate::compile::inline::inline_params;
use crate::compile::schema::*;
use crate::compile::scope::SQLScope;
use crate::compile::sql::*;
use crate::compile::unsafe_expr::compile_unsafe_expr;
use crate::{
    ast,
    ast::{Ident, Located, Range, SourceLocation, ToIdents},
};
use crate::{c_try, error::MultiResult, parser, parser::parse_schema};

type CompileResult<T> = MultiResult<T, CompileError>;

// This is a fairly crude hack that aims to "order" how we derive
// external types. As currently implemented, it ensures that we run
// load commands before we run unsafe expressions, so the latter can
// depend on the former.
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
pub enum ExternalTypeRank {
    Load,
    UnsafeExpr,
}

#[derive(Debug)]
pub struct ExternalTypeHandle {
    pub handle: tokio::task::JoinHandle<Result<()>>,
    pub inner_type: CRef<MType>,
    pub tx: tokio::sync::oneshot::Sender<bool>,
    pub order: (ExternalTypeRank, usize),
}

impl ExternalTypeHandle {
    pub fn rank(&self) -> ExternalTypeRank {
        self.order.0
    }
}

// Flip the ordering so that the heap is a min-heap
impl Ord for ExternalTypeHandle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.order.cmp(&self.order)
    }
}
impl PartialOrd for ExternalTypeHandle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.order.cmp(&self.order))
    }
}
impl PartialEq for ExternalTypeHandle {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}
impl Eq for ExternalTypeHandle {}

#[derive(Debug)]
pub struct CompilerData {
    pub config: CompilerConfig,
    pub next_placeholder: usize,
    pub idle: Ref<tokio::sync::watch::Receiver<()>>,
    pub handles: LinkedList<tokio::task::JoinHandle<Result<()>>>,
    pub next_external_type: usize,
    pub external_types: BinaryHeap<ExternalTypeHandle>,
    pub files: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub enum SymbolKind {
    Value,
    Field,
    Argument,
    Type,
    File,
}

pub trait OnSymbol {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn on_symbol(
        &mut self,
        name: Located<Ident>,
        kind: SymbolKind,
        type_: CRef<SType>,
        def: SourceLocation,
        is_public: bool,
    ) -> Result<()>;
}

pub trait OnSchema {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn on_schema(
        &mut self,
        path: Option<&FilePath>,
        ast: Option<&ast::Schema>,
        schema: Option<Ref<Schema>>,
        errors: &Vec<(Option<usize>, CompileError)>,
    ) -> Result<()>;
}

pub struct CompilerConfig {
    pub allow_native: bool,
    pub allow_inlining: bool,
    pub on_symbol: Option<Box<dyn OnSymbol + Send + Sync>>,
    pub on_schema: Option<Box<dyn OnSchema + Send + Sync>>,
    pub import_aliases: BTreeMap<Ident, String>,
}

impl Default for CompilerConfig {
    fn default() -> CompilerConfig {
        CompilerConfig {
            allow_native: false,
            allow_inlining: true,
            on_symbol: None,
            on_schema: None,
            import_aliases: BTreeMap::new(),
        }
    }
}

impl fmt::Debug for CompilerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompilerConfig")
            .field("allow_native", &self.allow_native)
            .field("allow_inlining", &self.allow_inlining)
            .finish_non_exhaustive()
    }
}

pub struct ParsedFile {
    pub file: String,
    pub folder: Option<String>,
    pub ast: ast::Schema,
}

// NOTE: We could disable this in debug mode, but the compiler is not
// very performance sensitive code, so we'll leave it in until proven otherwise
macro_rules! casync {
    ($f: block) => {
        async_backtrace::location!().frame(async move { $f })
    };
}
pub(crate) use casync;

#[derive(Debug, Clone)]
pub struct Compiler {
    runtime: Ref<tokio::runtime::Runtime>,
    data: Ref<CompilerData>,
    builtins: Ref<Schema>,
}

impl Compiler {
    pub fn new() -> Result<Compiler> {
        Compiler::new_with_config(CompilerConfig::default())
    }

    pub fn new_with_config(config: CompilerConfig) -> Result<Compiler> {
        lazy_static::initialize(&GLOBAL_SCHEMA);
        Compiler::new_with_builtins(GLOBAL_SCHEMA.clone(), config)
    }

    pub fn new_with_builtins(schema: Ref<Schema>, config: CompilerConfig) -> Result<Compiler> {
        // Create a watcher that will be signaled whenever the runtime attempts to park.  If
        // anything is awaiting this channel, then the runtime will not actually park, and will
        // instead yield control back to that coroutine.  This is useful because we don't know
        // ahead of time which of our futures will actually resolve, and really just want to
        // execute tasks until none are runnable.  See `drive` for more on this.
        //
        let (idle_tx, idle_rx) = tokio::sync::watch::channel(());
        let on_park = move || {
            idle_tx.send(()).ok();
        };

        // This is only used during bootstrapping (where we know we don't need
        // the GLOBAL_SCHEMA to be initialized).
        //
        let compiler = Compiler {
            runtime: mkref(
                tokio::runtime::Builder::new_current_thread()
                    .thread_name("QueryScript Compiler")
                    .thread_stack_size(3 * 1024 * 1024)
                    .on_thread_park(on_park)
                    .enable_io()
                    .enable_time()
                    .build()?,
            ),
            data: mkref(CompilerData {
                config,
                next_placeholder: 1,
                idle: mkref(idle_rx),
                handles: LinkedList::new(),
                next_external_type: 1,
                external_types: BinaryHeap::new(),
                files: BTreeMap::new(),
            }),
            builtins: schema.clone(),
        };

        Ok(compiler)
    }

    pub fn builtins(&self) -> Ref<Schema> {
        self.builtins.clone()
    }

    pub fn allow_native(&self) -> Result<bool> {
        Ok(self.data.read()?.config.allow_native)
    }

    pub fn allow_inlining(&self) -> Result<bool> {
        Ok(self.data.read()?.config.allow_inlining)
    }

    pub fn on_symbol(
        &self,
        mut on_symbol: Option<Box<dyn OnSymbol + Send + Sync>>,
    ) -> Result<Option<Box<dyn OnSymbol + Send + Sync>>> {
        std::mem::swap(&mut self.data.write()?.config.on_symbol, &mut on_symbol);
        Ok(on_symbol)
    }

    pub fn run_on_symbol<E: Entry>(
        &self,
        name: Located<Ident>,
        kind: SymbolKind,
        type_: CRef<SType>,
        def: SourceLocation,
        decl: Option<Decl<E>>, // XXX We could just take is_public here as input?
    ) -> Result<()> {
        let mut data = self.data.write()?;
        let on_symbol = &mut data.config.on_symbol;
        Ok(match on_symbol {
            Some(f) => f.on_symbol(
                name.clone(),
                kind,
                type_,
                def,
                decl.as_ref().map_or(false, |decl| decl.public),
            )?,
            None => {}
        })
    }

    pub fn on_schema(
        &self,
        mut on_schema: Option<Box<dyn OnSchema + Send + Sync>>,
    ) -> Result<Option<Box<dyn OnSchema + Send + Sync>>> {
        std::mem::swap(&mut self.data.write()?.config.on_schema, &mut on_schema);
        Ok(on_schema)
    }

    pub fn run_on_schema(
        &self,
        file: Option<&FilePath>,
        ast: Option<&ast::Schema>,
        schema: Option<Ref<Schema>>,
        errors: &Vec<(Option<usize>, CompileError)>,
    ) -> Result<()> {
        let mut data = self.data.write()?;
        let on_schema = &mut data.config.on_schema;
        Ok(match on_schema {
            Some(f) => f.on_schema(file, ast, schema, errors)?,
            None => {}
        })
    }

    pub fn compile_string(&self, schema: Ref<Schema>, text: &str) -> CompileResult<()> {
        let mut result = CompileResult::new(());
        let file = c_try!(result, schema.read()).file.clone();
        let (tokens, eof) = c_try!(result, parser::tokenize(file.as_str(), text));
        let mut parser = parser::Parser::new(file.as_str(), tokens, eof);
        let schema_ast = result.absorb(parser.parse_schema());
        result.absorb(self.compile_schema_ast(schema.clone(), &schema_ast));
        result
    }

    pub fn compile_schema_ast(&self, schema: Ref<Schema>, ast: &ast::Schema) -> CompileResult<()> {
        let mut result = CompileResult::new(());
        let runtime = c_try!(result, self.runtime.read());
        runtime.block_on(async move {
            result.replace(compile_schema_ast(self.clone(), schema.clone(), ast));
            result.absorb(self.drive().await);

            c_try!(
                result,
                self.run_on_schema(None, Some(ast), Some(schema.clone()), &result.errors)
            );

            result
        })
    }

    pub fn compile_schema_from_file(
        &self,
        file_path: &FilePath,
    ) -> CompileResult<Option<Ref<Schema>>> {
        let mut result = CompileResult::new(None);
        let runtime = c_try!(result, self.runtime.read());
        runtime.block_on(async {
            let (compile_result, parsed_file) = compile_schema_from_file(self.clone(), file_path);
            result.replace(compile_result);
            result.absorb(self.drive().await);

            c_try!(
                result,
                self.run_on_schema(
                    Some(&file_path),
                    parsed_file.as_ref().map(|f| &f.ast),
                    result.result.as_ref().map(|s| s.clone()),
                    &result.errors
                )
            );

            result
        })
    }

    async fn drive(&self) -> CompileResult<()> {
        let mut result = CompileResult::new(());

        loop {
            // This channel will be signaled by our `on_park` callback when the runtime attempts to
            // park.  By blocking on it, we are ensured that control will be returned to this coroutine
            // only when all other work has been exhausted, meaning we've completed all of the
            // inference we possibly can.
            //
            // NOTE: Be careful not to hold the lock on `self.data` while we await this signal.  It's
            // okay to hold the lock on the signal receiver itself, since only one instance of `drive`
            // may be called concurrently.  To ensure this, use `try_write` instead of `write` and fail
            // quickly if there is contention.
            //
            let idle = c_try!(result, self.data.read()).idle.clone();
            let mut idle = c_try!(result, idle.try_write());

            // Consume any ready changed() signals, so that next time we do not cancel the handles
            // prematurely. If we consume all of the ready signals, and there's no more work left,
            // then the thread will immediately park, so there's no risk of "over checking" the signal.
            while c_try!(result, idle.has_changed()) {
                c_try!(result, idle.changed().await);
            }

            c_try!(result, idle.changed().await);

            // Claim the set external types we've accrued within the compiler to reset things.
            //
            let external_types = {
                let mut ret = Vec::new();
                let mut first_rank = None;

                let mut data = c_try!(result, self.data.write());
                while let Some(et) = data.external_types.peek() {
                    // If the upcoming external type has a higher rank than the first external type,
                    // then bail out for now, so that the compiler has some breathing room to complete
                    // type inference for (safe) expressions that depend on the original kind. A simpler
                    // approach would be to just pull one external type at a time, but in practice we
                    // probably want to schema infer external data sources in parallel.
                    let rank = et.rank();
                    match (first_rank, rank) {
                        (Some(first_rank), rank) if rank > first_rank => break,
                        (None, rank) => first_rank = Some(rank),
                        _ => {}
                    };
                    ret.push(data.external_types.pop().unwrap());
                }
                ret
            };

            let any_external_types = external_types.len() > 0;
            for external_type in external_types {
                // Check whether the type can be converted to a runtime type. If it errors
                // out, then we know that the type is still unresolved.
                if !c_try!(result, external_type.inner_type.is_known())
                    || matches!(
                        c_try!(
                            result,
                            c_try!(
                                result,
                                external_type.inner_type.must().context(RuntimeSnafu {
                                    loc: SourceLocation::Unknown
                                })
                            )
                            .read()
                        )
                        .to_runtime_type(),
                        Err(_)
                    )
                {
                    match external_type.tx.send(true) {
                        Ok(()) => {}
                        Err(_) => {
                            // The oneshot channel sends the value itself back (i.e. the boolean true) in
                            // the event of an error, so we handle it manually to create this error message.
                            result.add_error(
                                None,
                                CompileError::external("Failed to trigger external type callback"),
                            );
                            continue;
                        }
                    };

                    // There are two errors that can be thrown here: one from joining the task and the other
                    // from the task itself.
                    match c_try!(result, external_type.handle.await) {
                        Ok(_) => {}
                        Err(e) => result.add_error(None, e.into()),
                    }
                }
            }

            if any_external_types {
                // If there were any unresolved types, then loop around again (knowing that the thread will
                // park at least once after the requisite work is done or immediately if there's no more work).
                continue;
            }

            // Claim the set of handles we've accrued within the compiler to reset things.
            //
            let mut handles = LinkedList::new();
            std::mem::swap(&mut c_try!(result, self.data.write()).handles, &mut handles);

            // Each handle must either be checked for errors if completed, or aborted if not.
            //
            for handle in handles {
                if handle.is_finished() {
                    match c_try!(result, handle.await) {
                        Ok(_) => {}
                        Err(e) => result.add_error(None, e.into()),
                    }
                } else {
                    handle.abort();
                }
            }

            // Between the above wait for idle and looping through the handles, the on_park() signal
            // may have fired again. If so, then we need to loop around and check the handles again. Since
            // we swap out the handles each time, we'll only check the handles that were added since the
            // the last time we checked idle, which should be zero in the expected case.
            if !c_try!(result, idle.has_changed()) {
                break;
            }
        }

        return result;
    }

    pub fn next_placeholder(&self, kind: &str) -> Result<String> {
        let mut data = self.data.write()?;
        let placeholder = data.next_placeholder;
        data.next_placeholder += 1;
        Ok(format!("{}{}", kind, placeholder))
    }

    pub fn async_cref<T: Constrainable + 'static>(
        &self,
        f: impl std::future::Future<Output = Result<CRef<T>>> + Send + 'static,
    ) -> Result<CRef<T>> {
        let mut data = self.data.write()?;
        let slot = CRef::<T>::new_unknown("async_slot");
        let ret = slot.clone();
        data.handles.push_back(self.runtime.read()?.spawn(casync!({
            let r = f.await?;
            slot.unify(&r)
        })));

        Ok(ret)
    }

    pub fn add_external_type(
        &self,
        f: impl std::future::Future<Output = Result<()>> + Send + 'static,
        inner_type: CRef<MType>,
        order: ExternalTypeRank,
    ) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut data = self.data.write()?;
        let cb = self.runtime.read()?.spawn(async move {
            let should_run = rx.await?;
            if should_run {
                f.await?;
            }
            Ok(())
        });

        let next_external_type = data.next_external_type;
        data.next_external_type += 1;
        let handle = ExternalTypeHandle {
            handle: cb,
            inner_type,
            tx,
            order: (order, next_external_type),
        };
        data.external_types.push(handle);
        Ok(())
    }

    pub fn open_file(&self, file_path: &FilePath) -> Result<(String, Option<String>, ast::Schema)> {
        let parsed_path = FilePath::new(file_path);
        let file = parsed_path.to_str().unwrap().to_string();
        let parent_path = parsed_path.parent();
        let folder = match parent_path {
            Some(p) => p.to_str().map(|f| f.to_string()),
            None => None,
        };

        let mut data = self.data.write()?;
        let entry = data.files.entry(file.clone());
        let contents = match entry {
            std::collections::btree_map::Entry::Vacant(entry) => {
                let contents = fs::read_to_string(&parsed_path)?;
                entry.insert(contents.clone())
            }
            std::collections::btree_map::Entry::Occupied(ref entry) => entry.get().as_str(),
        };
        Ok((
            file,
            folder,
            parse_schema(parsed_path.to_str().unwrap(), contents).as_result()?,
        ))
    }

    pub fn set_file_contents(&self, file: String, contents: String) -> Result<()> {
        self.data.write()?.files.insert(file, contents);
        Ok(())
    }

    pub fn file_contents(&self) -> Result<std::sync::RwLockReadGuard<'_, CompilerData>> {
        Ok(self.data.read()?)
    }

    pub fn dump_task_tree(&self) -> Result<String> {
        let runtime = self.runtime.read()?;
        Ok(runtime.block_on(async { async_backtrace::taskdump_tree(true) }))
    }
}

pub fn lookup_schema(
    compiler: Compiler,
    schema: Ref<Schema>,
    path: &SchemaPath,
) -> Result<Ref<ImportedSchema>> {
    let imported = schema.read()?.imports.get(&path).map(Clone::clone);
    let imported = if let Some(imported) = imported {
        imported
    } else {
        let imported = match &path {
            SchemaPath::Schema(path) => {
                let v = if let Some(root) = &schema.read()?.folder {
                    let mut file_path_buf = FilePath::new(root).to_path_buf();
                    for p in path {
                        file_path_buf.push(FilePath::new(p.get()));
                    }
                    for extension in SCHEMA_EXTENSIONS.iter() {
                        file_path_buf.set_extension(extension);
                        if file_path_buf.as_path().exists() {
                            break;
                        }
                    }
                    let file_path = file_path_buf.as_path();

                    let s = compile_schema_from_file(compiler.clone(), file_path)
                        .0
                        .as_result()?
                        .unwrap();
                    s.clone()
                } else {
                    return Err(CompileError::no_such_entry(path.clone()));
                };

                mkref(ImportedSchema {
                    args: if v.read()?.externs.len() == 0 {
                        None
                    } else {
                        Some(Vec::new())
                    },
                    schema: Importer::Schema(v.clone()),
                })
            }
            SchemaPath::Connection(url) => {
                // TODO: We could support arguments to the connection string as $ variables
                // (eg. postgres://localhost/$db_name) and parse/apply them here
                mkref(ImportedSchema {
                    args: None,
                    schema: Importer::Connection(mkref(ConnectionSchema::new(
                        url.get().clone(),
                        url.location().clone(),
                    ))),
                })
            }
        };

        schema
            .write()?
            .imports
            .insert(path.clone(), imported.clone());

        imported
    };

    if let SchemaPath::Schema(path) = &path {
        if let Some(ident) = path.last() {
            let file = imported.read()?.schema.location()?;
            compiler.run_on_symbol::<SchemaPath>(
                ident.clone(),
                SymbolKind::File,
                CRef::new_unknown("schema"),
                file,
                None,
            )?;
        }
    }

    return Ok(imported);
}

pub fn lookup_path<E: Entry>(
    compiler: Compiler,
    imported_object: Importer,
    path: &ast::Path,
    import_global: bool,
    resolve_last: bool,
) -> Result<(Importer, Option<Decl<E>>, ast::Path)> {
    if path.len() == 0 {
        return Ok((imported_object, None, path.clone()));
    }

    let mut imported_object = imported_object;
    for (i, ident) in path.iter().enumerate() {
        let check_visibility = i > 0;

        if let Some(decl) =
            imported_object.get_and_check::<E>(&compiler, &ident, check_visibility, path)?
        {
            return Ok((
                imported_object,
                Some(decl.get().clone()),
                path[i + 1..].to_vec(),
            ));
        }

        let schema = match &imported_object {
            Importer::Schema(schema) => schema.clone(),
            Importer::Connection(_) => {
                // Cannot proceed any further
                break;
            }
        };

        let new = if let Some(imported) = imported_object.get_and_check::<SchemaPath>(
            &compiler,
            &ident,
            check_visibility,
            path,
        )? {
            lookup_schema(compiler.clone(), schema.clone(), &imported.value)?
                .read()?
                .schema
                .clone()
        } else {
            match &schema.read()?.parent_scope {
                Some(parent) => {
                    return lookup_path::<E>(
                        compiler.clone(),
                        Importer::Schema(parent.clone()),
                        &path[i..].to_vec(),
                        import_global,
                        resolve_last,
                    )
                }
                None => {
                    if import_global {
                        return lookup_path::<E>(
                            compiler.clone(),
                            Importer::Schema(compiler.builtins()),
                            &path[i..].to_vec(),
                            false, /* import_global */
                            resolve_last,
                        );
                    } else {
                        return Ok((imported_object.clone(), None, path[i..].to_vec()));
                    }
                }
            }
        };

        imported_object = new;
    }

    return Ok((imported_object.clone(), None, Vec::new()));
}

pub fn resolve_type(
    compiler: Compiler,
    schema: Ref<Schema>,
    ast: &ast::Type,
) -> Result<CRef<MType>> {
    let loc = SourceLocation::Range(
        schema.read()?.file.clone(),
        Range {
            start: ast.start.clone(),
            end: ast.end.clone(),
        },
    );
    match &ast.body {
        ast::TypeBody::Reference(path) => {
            let (_, decl, r) = lookup_path::<CRef<MType>>(
                compiler.clone(),
                Importer::Schema(schema.clone()),
                &path,
                true, /* import_global */
                true, /* resolve_last */
            )?;
            if r.len() > 0 {
                return Err(CompileError::no_such_entry(r));
            }
            let decl = decl.ok_or_else(|| CompileError::no_such_entry(r))?;
            if let Some(ident) = path.last() {
                compiler.run_on_symbol(
                    ident.clone(),
                    SymbolKind::Type,
                    SType::new_mono(decl.value.clone()),
                    decl.name.location().clone(),
                    Some(decl.clone()),
                )?;
            }

            Ok(decl.value.clone())
        }
        ast::TypeBody::Struct(entries) => {
            let mut fields = Vec::new();
            let mut seen = BTreeSet::new();
            for e in entries {
                match e {
                    ast::StructEntry::NameAndType(nt) => {
                        if seen.contains(&nt.name) {
                            return Err(CompileError::duplicate_entry(vec![nt.name.clone()]));
                        }
                        seen.insert(nt.name.clone());
                        fields.push(MField {
                            name: nt.name.get().clone(),
                            type_: resolve_type(compiler.clone(), schema.clone(), &nt.def)?,
                            nullable: true, /* TODO: implement non-null types */
                        });
                    }
                    ast::StructEntry::Include { .. } => {
                        return Err(CompileError::unimplemented(loc, "Struct inclusions"));
                    }
                }
            }

            Ok(mkcref(MType::Record(Located::new(fields, loc))))
        }
        ast::TypeBody::List(inner) => Ok(mkcref(MType::List(Located::new(
            resolve_type(compiler, schema, inner.as_ref())?,
            loc,
        )))),
        ast::TypeBody::Exclude { .. } => {
            return Err(CompileError::unimplemented(loc, "Struct exclusions"));
        }
        ast::TypeBody::Generic(path, types) => {
            let args = types
                .iter()
                .map(|t| resolve_type(compiler.clone(), schema.clone(), t))
                .collect::<Result<Vec<_>>>()?;

            // Since generic names are hardcoded right now, expect the name to be a single element.
            // Eventually, this should be a decl lookup though.
            let name = if path.len() == 1 {
                path[0].get()
            } else {
                return Err(CompileError::unimplemented(loc, "Multi-part generic names"));
            };

            let generic = match GLOBAL_GENERICS.get(name).map(|g| g.new(&loc, args)) {
                Some(generic) => generic?,
                None => return Err(CompileError::no_such_entry(path.clone())),
            };

            Ok(MType::Generic(Located::new(generic, loc)).resolve_generics()?)
        }
    }
}

pub fn resolve_global_atom(compiler: Compiler, name: &str) -> Result<CRef<MType>> {
    resolve_type(
        compiler.clone(),
        compiler.builtins(),
        &ast::Type {
            body: ast::TypeBody::Reference(vec![Ident::with_location(
                BUILTIN_LOC.clone(),
                name.to_string(),
            )]),
            start: ast::Location { line: 0, column: 0 },
            end: ast::Location { line: 0, column: 0 },
        },
    )
}

pub fn find_field<'a>(fields: &'a Vec<MField>, name: &Ident) -> Option<&'a MField> {
    for f in fields.iter() {
        if &f.name == name {
            return Some(f);
        }
    }
    None
}

impl SType {
    pub fn instantiate(&self) -> Result<CRef<MType>> {
        let variables: BTreeMap<_, _> = self
            .variables
            .iter()
            .map(|n| (n.clone(), MType::new_unknown(n.as_str())))
            .collect();

        return Ok(self.body.substitute(&variables)?);
    }
}

pub fn typecheck_path(type_: CRef<MType>, path: &[Located<Ident>]) -> Result<CRef<MType>> {
    if path.len() == 0 {
        return Ok(type_);
    }

    let name = path[0].get().clone();
    let remainder = path[1..].to_vec();

    type_.then(move |type_: Ref<MType>| match &*type_.read()? {
        MType::Record(fields) => {
            if let Some(field) = find_field(fields.get(), &name) {
                typecheck_path(field.type_.clone(), remainder.as_slice())
            } else {
                return Err(CompileError::wrong_type(
                    &MType::Record(Located::new(
                        vec![MField::new_nullable(
                            name.clone(),
                            MType::new_unknown("field"),
                        )],
                        fields.location().clone(),
                    )),
                    &*type_.read()?,
                ));
            }
        }
        t => {
            return Err(CompileError::wrong_type(
                &MType::Record(Located::new(
                    vec![MField::new_nullable(
                        name.clone(),
                        MType::new_unknown("field"),
                    )],
                    t.location().clone(),
                )),
                &*type_.read()?,
            ))
        }
    })
}

fn compile_expr(compiler: Compiler, schema: Ref<Schema>, expr: &ast::Expr) -> Result<CTypedExpr> {
    let loc = SourceLocation::Range(
        schema.read()?.file.clone(),
        Range {
            start: expr.start.clone(),
            end: expr.end.clone(),
        },
    );

    if expr.is_unsafe {
        compile_unsafe_expr(compiler, schema, &expr.body, &loc)
    } else {
        Ok(match &expr.body {
            ast::ExprBody::SQLQuery(q) => {
                let (_scope, type_, query) =
                    compile_sqlquery(compiler.clone(), schema.clone(), None, &loc, q)?;
                CTypedExpr {
                    type_,
                    expr: compiler.async_cref(async move {
                        let query = cunwrap(query.await?)?;

                        Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                            names: query.names,
                            body: SQLBody::Query(query.body),
                        }))))
                    })?,
                }
            }
            ast::ExprBody::SQLExpr(e) => {
                let scope = SQLScope::new(None);
                compile_sqlexpr(compiler.clone(), schema.clone(), scope, &loc, e)?
            }
        })
    }
}

pub fn rebind_decl<E: Entry>(_schema: SchemaInstance, decl: &Decl<E>) -> Result<E> {
    Ok(decl.value.clone())
}

fn compile_schema_from_file(
    compiler: Compiler,
    file_path: &FilePath,
) -> (CompileResult<Option<Ref<Schema>>>, Option<ParsedFile>) {
    let mut result = CompileResult::new(None);

    let (file, folder, ast) = match compiler.open_file(file_path) {
        Ok((file, folder, contents)) => (file, folder, contents),
        Err(e) => {
            result.add_error(None, e);
            return (result, None);
        }
    };

    result.replace(
        compile_schema(compiler.clone(), file.clone(), folder.clone(), &ast).map(|s| Some(s)),
    );
    (result, Some(ParsedFile { file, folder, ast }))
}

fn compile_schema(
    compiler: Compiler,
    file: String,
    folder: Option<String>,
    ast: &ast::Schema,
) -> CompileResult<Ref<Schema>> {
    let mut result = CompileResult::new(Schema::new(file, folder));
    result.absorb(compile_schema_ast(
        compiler.clone(),
        result.result.clone(),
        ast,
    ));
    result
}

fn compile_schema_ast(
    compiler: Compiler,
    schema: Ref<Schema>,
    ast: &ast::Schema,
) -> CompileResult<()> {
    let mut result = CompileResult::new(());

    result.absorb(declare_schema_entries(
        compiler.clone(),
        schema.clone(),
        ast,
    ));
    result.absorb(compile_schema_entries(
        compiler.clone(),
        schema.clone(),
        ast,
    ));
    result.absorb(optimize_schema(compiler.clone(), schema.clone()));
    match gather_schema_externs(schema) {
        Ok(_) => {}
        Err(e) => result.add_error(None, e),
    };

    result
}

fn declare_schema_entries(
    compiler: Compiler,
    schema: Ref<Schema>,
    ast: &ast::Schema,
) -> CompileResult<()> {
    let mut result = CompileResult::new(());
    for (idx, stmt) in ast.stmts.iter().enumerate() {
        match declare_schema_entry(&compiler, &schema, stmt) {
            Ok(_) => {}
            Err(e) => result.add_error(Some(idx), e),
        }
    }
    result
}

type Declaration<T> = (Located<Ident>, bool, T);

fn import_all_decls<E: Entry>(
    decls: &DeclMap<E>,
    imported_schema: SchemaInstance,
) -> Result<Vec<Declaration<E>>> {
    let mut ret = Vec::new();
    for (_, v) in decls.iter().filter(|(_, v)| v.public) {
        ret.push((
            v.name.clone(),
            false, /* extern_ */
            rebind_decl(imported_schema.clone(), &v)?,
        ));
    }
    Ok(ret)
}

fn import_named_decl<E: Entry>(
    compiler: Compiler,
    imported: Ref<ImportedSchema>,
    imported_schema: SchemaInstance,
    item: &ast::Path,
) -> Result<Declaration<E>> {
    let loc = path_location(item);
    if item.len() != 1 {
        return Err(CompileError::unimplemented(loc, "path imports"));
    }

    let (_, decl, r) = lookup_path::<E>(
        compiler.clone(),
        imported.read()?.schema.clone(),
        &item,
        false, /* import_global */
        false, /* resolve_last */
    )?;
    if r.len() > 0 {
        return Err(CompileError::no_such_entry(r.clone()));
    }
    let decl = decl.ok_or_else(|| CompileError::no_such_entry(r))?;

    if let Some(ident) = item.last() {
        run_on_decl(compiler.clone(), ident.clone(), &decl)?;
    }

    Ok((
        item[0].clone(),
        false, /* extern_ */
        rebind_decl(imported_schema, &decl)?,
    ))
}

fn add_decls<E: Entry>(
    decls: &mut DeclMap<E>,
    entries: Vec<Declaration<E>>,
    loc: &SourceLocation,
    stmt: &ast::Stmt,
) -> Result<()> {
    for (name, extern_, value) in &entries {
        if decls.contains_key(name) {
            return Err(CompileError::duplicate_entry(vec![name.clone()]));
        }

        decls.insert(
            name.get().clone(),
            Located::new(
                Decl {
                    public: true, // For now, any decl is public (whether it's `export` or not)
                    extern_: *extern_,
                    is_arg: false,
                    name: name.clone(),
                    value: value.clone(),
                },
                loc.clone(),
            ),
        );
    }
    Ok(())
}

fn declare_schema_entry(compiler: &Compiler, schema: &Ref<Schema>, stmt: &ast::Stmt) -> Result<()> {
    let loc = SourceLocation::Range(
        schema.read()?.file.clone(),
        Range {
            start: stmt.start.clone(),
            end: stmt.end.clone(),
        },
    );

    let (mut schema_decls, mut type_decls, mut expr_decls) = (Vec::new(), Vec::new(), Vec::new());

    match &stmt.body {
        ast::StmtBody::Noop | ast::StmtBody::Unparsed => {}
        ast::StmtBody::Expr(_) => {}
        ast::StmtBody::Import { path, list, .. } => {
            if path.len() == 0 {
                return Err(CompileError::internal(loc.clone(), "Empty import"));
            }

            let mut path = path.clone();
            if let Some(e) = compiler
                .data
                .read()?
                .config
                .import_aliases
                .get(path[0].get())
            {
                path[0] = Located::new(Ident::from(e.clone()), path[0].location().clone());
            }

            let path = match ConnectionString::maybe_parse(
                schema.read()?.folder.clone(),
                path[0].as_str(),
                &loc,
            )? {
                None => SchemaPath::Schema(path.clone()),
                Some(cs) => SchemaPath::Connection(Located::new(cs, path[0].location().clone())),
            };

            let imported = lookup_schema(compiler.clone(), schema.clone(), &path)?;

            if imported.read()?.args.is_some() {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    "Importing with arguments",
                ));
            }

            // XXX Importing schemas with extern values is currently broken, because we don't
            // actually "inject" any meaningful reference to imported_schema's id into the decl
            // during rebind_decl.  We should figure out how to generate a new set of decls for
            // the imported schema (w/ the imported args)
            //
            // let checked = match args {
            //     None => None,
            //     Some(args) => {
            //         let mut externs = imported.read()?.schema.read()?.externs.clone();
            //         let mut checked = BTreeMap::new();
            //         for arg in args {
            //             let expr = match &arg.expr {
            //                 None => ast::Expr::SQLExpr(sqlast::Expr::CompoundIdentifier(vec![
            //                     sqlast::Ident {
            //                         value: arg.name.clone(),
            //                         quote_style: None,
            //                     },
            //                 ])),
            //                 Some(expr) => expr.clone(),
            //             };

            //             if checked.get(&arg.name).is_some() {
            //                 return Err(CompileError::duplicate_entry(vec![arg.name.clone()]));
            //             }

            //             if let Some(extern_) = externs.get_mut(&arg.name) {
            //                 let compiled = compile_expr(schema.clone(), &expr)?;

            //                 extern_.unify(&compiled.type_)?;
            //                 checked.insert(
            //                     arg.name.clone(),
            //                     TypedNameAndExpr {
            //                         name: arg.name.clone(),
            //                         type_: extern_.clone(),
            //                         expr: compiled.expr,
            //                     },
            //                 );
            //             } else {
            //                 return Err(CompileError::no_such_entry(vec![arg.name.clone()]));
            //             }
            //         }

            //         Some(checked)
            //     }
            // };

            // let id = {
            //     let imported_args = &mut imported.write()?.args;
            //     if let Some(imported_args) = imported_args {
            //         if let Some(checked) = checked {
            //             let id = imported_args.len();
            //             imported_args.push(checked);
            //             Some(id)
            //         } else {
            //             return Err(CompileError::import_error(
            //                 path.clone(),
            //                 "Arguments are not provided to module with extern declarations",
            //             ));
            //         }
            //     } else if args.is_some() {
            //         return Err(CompileError::import_error(
            //               path.clone(),
            //             "Arguments should not be provided to module without extern declarations",
            //         ));
            //     } else {
            //         None
            //     }
            // };

            match list {
                ast::ImportList::None => {
                    let name = match &path {
                        SchemaPath::Schema(path) => path.last().unwrap().clone(),
                        SchemaPath::Connection(cs) => {
                            Located::new(cs.db_name().clone(), cs.location().clone())
                        }
                    };
                    schema_decls.push((name, false /* extern_ */, path));
                }
                ast::ImportList::Star => {
                    let schema = match &imported.read()?.schema {
                        Importer::Schema(s) => s.clone(),
                        Importer::Connection(_) => {
                            return Err(CompileError::unimplemented(
                                loc.clone(),
                                "Importing all from a connection",
                            ));
                        }
                    };
                    let imported_schema = SchemaInstance { schema, id: None };
                    schema_decls.extend(import_all_decls(
                        &imported_schema.schema.read()?.schema_decls,
                        imported_schema.clone(),
                    )?);
                    type_decls.extend(import_all_decls(
                        &imported_schema.schema.read()?.type_decls,
                        imported_schema.clone(),
                    )?);
                    expr_decls.extend(import_all_decls(
                        &imported_schema.schema.read()?.expr_decls,
                        imported_schema.clone(),
                    )?);
                }
                ast::ImportList::Items(items) => {
                    let imported_schema = SchemaInstance {
                        schema: schema.clone(),
                        id: None,
                    };

                    let mut found = false;
                    let mut err = None;

                    for item in items {
                        match import_named_decl::<SchemaPath>(
                            compiler.clone(),
                            imported.clone(),
                            imported_schema.clone(),
                            item,
                        ) {
                            Ok(decl) => {
                                found = true;
                                schema_decls.push(decl);
                            }
                            Err(e) => err = Some(e),
                        };

                        match import_named_decl::<TypeEntry>(
                            compiler.clone(),
                            imported.clone(),
                            imported_schema.clone(),
                            item,
                        ) {
                            Ok(decl) => {
                                found = true;
                                type_decls.push(decl);
                            }
                            Err(e) => err = Some(e),
                        };

                        match import_named_decl::<ExprEntry>(
                            compiler.clone(),
                            imported.clone(),
                            imported_schema.clone(),
                            item,
                        ) {
                            Ok(decl) => {
                                found = true;
                                expr_decls.push(decl);
                            }
                            Err(e) => err = Some(e),
                        };

                        if !found {
                            return Err(err.unwrap());
                        }
                    }
                }
            }
        }
        ast::StmtBody::TypeDef(nt) => type_decls.push((
            nt.name.clone(),
            false, /* extern_ */
            MType::new_unknown(nt.name.get().as_ref()),
        )),
        ast::StmtBody::FnDef(ast::FnDef { name, .. }) => expr_decls.push((
            name.clone(),
            false, /* extern_ */
            STypedExpr::new_unknown(name.get().as_ref()),
        )),
        ast::StmtBody::Let { name, .. } => expr_decls.push((
            name.clone(),
            false, /* extern_ */
            STypedExpr::new_unknown(name.get().as_ref()),
        )),
        ast::StmtBody::Extern { name, .. } => expr_decls.push((
            name.clone(),
            true, /* extern_ */
            STypedExpr::new_unknown(name.get().as_ref()),
        )),
    };

    add_decls(&mut schema.write()?.schema_decls, schema_decls, &loc, stmt)?;
    add_decls(&mut schema.write()?.type_decls, type_decls, &loc, stmt)?;
    add_decls(&mut schema.write()?.expr_decls, expr_decls, &loc, stmt)?;

    Ok(())
}

fn run_on_decl<E: Entry>(compiler: Compiler, ident: Located<Ident>, decl: &Decl<E>) -> Result<()> {
    let info = decl.value.run_on_info();
    match info {
        Some((kind, type_)) => compiler.run_on_symbol(
            ident,
            kind,
            type_,
            decl.name.location().clone(),
            Some(decl.clone()),
        ),
        None => Ok(()),
    }
}

pub fn unify_type_decl(
    compiler: Compiler,
    schema: Ref<Schema>,
    name: &Located<Ident>,
    type_: CRef<MType>,
) -> Result<()> {
    let s = schema.read()?;
    let decl = s.type_decls.get(&name).ok_or_else(|| {
        CompileError::internal(
            name.location().clone(),
            format!(
                "Could not find type declaration {} during reprocessing",
                name
            )
            .as_str(),
        )
    })?;

    decl.value.unify(&type_)?;
    run_on_decl(compiler.clone(), decl.name.clone(), decl.get())?;

    Ok(())
}

pub fn unify_expr_decl(
    compiler: Compiler,
    schema: Ref<Schema>,
    name: &Located<Ident>,
    value: &STypedExpr,
) -> Result<()> {
    let s = schema.read()?;
    let decl = s.expr_decls.get(&name).ok_or_else(|| {
        CompileError::internal(
            name.location().clone(),
            format!(
                "Could not find expr declaration {} during reprocessing",
                name
            )
            .as_str(),
        )
    })?;

    decl.value.unify(&value)?;
    run_on_decl(compiler.clone(), decl.name.clone(), decl.get())?;

    Ok(())
}

fn compile_materialized_expr(
    compiler: Compiler,
    schema: SchemaRef,
    loc: SourceLocation,
    decl_name: Ident,
    expr: CTypedExpr,
    args: ast::MaterializeArgs,
) -> Result<CRef<Expr<CRef<MType>>>> {
    compiler.async_cref({
        let compiler = compiler.clone();
        async move {
            let url = match args.db {
                Some(expr) => {
                    let expr = compile_expr(compiler.clone(), schema.clone(), &expr)?;
                    let url = expr.expr.await?;
                    let url = url.read()?.clone();

                    match url.unwrap_schema_entry().await? {
                        Expr::Connection(url) => Some(url.clone()),
                        _ => {
                            return Err(CompileError::internal(
                                loc.clone(),
                                &format!(
                                    "Expected a connection as the second argument of materialize()"
                                ),
                            ))
                        }
                    }
                }
                None => None,
            };

            let type_ = expr.type_;
            let expr = expr.expr.await?;
            let expr = Arc::new(expr.read()?.clone());

            Ok(mkcref(Expr::Materialize(MaterializeExpr {
                decl_name,
                key: compiler.next_placeholder("materialized")?,
                expr: TypedExpr { expr, type_ },
                url,
                inlined: false,
            })))
        }
    })
}

pub fn compile_schema_entries(
    compiler: Compiler,
    schema: Ref<Schema>,
    ast: &ast::Schema,
) -> CompileResult<()> {
    let mut result = CompileResult::new(());

    for (idx, stmt) in ast.stmts.iter().enumerate() {
        match compile_schema_entry(&compiler, &schema, stmt) {
            Ok(_) => {}
            Err(e) => result.add_error(Some(idx), e),
        }
    }
    result
}

#[derive(Debug)]
pub enum FnContext {
    Decl,
    Call,
}

pub fn compile_fn_body(
    compiler: Compiler,
    schema: Ref<Schema>,
    loc: SourceLocation,
    def: &ast::FnDef,
    precompiled_args: BTreeMap<Ident, TypedNameAndExpr<CRef<MType>>>,
    context: FnContext,
) -> Result<(CTypedExpr, Vec<Located<Ident>>)> {
    let mut def = def.clone();
    let schema = Schema::derive(schema)?;

    let generic_return_type_name = || Located::new("__Return".into(), loc.clone());
    if !def.generics.is_empty() && def.ret.is_none() {
        let ast::Range { start, end } = loc.range().unwrap_or(ast::Range {
            start: ast::Location { line: 0, column: 0 },
            end: ast::Location { line: 0, column: 0 },
        });
        def.generics.push(generic_return_type_name());
        def.ret = Some(ast::Type {
            start,
            end,
            body: ast::TypeBody::Reference(vec![generic_return_type_name()]),
        });
    }

    let has_expr_body = matches!(def.body, ast::FnBody::Expr(_));

    // TODO: We used to also compile_decls that do not have generics (i.e. def.generics.is_empty())
    // but have disabled this because expressions may contain for loops, which could affect the type
    // of the function. There is likely a better solution that captures loops/conditionals as some
    // kind of generically typed value.
    let can_compile_decl = !has_expr_body;

    let compile_body = match context {
        FnContext::Decl => can_compile_decl,
        FnContext::Call => !can_compile_decl,
    };
    let mut unknowns = BTreeMap::new();
    for generic in def.generics.iter() {
        schema.write()?.type_decls.insert(
            generic.get().clone(),
            Located::new(
                Decl {
                    public: false,
                    extern_: true,
                    is_arg: true,
                    name: generic.clone(),
                    value: mkcref(MType::Name(generic.clone())),
                },
                loc.clone(),
            ),
        );
        let unknown = CRef::new_unknown(generic.as_str());
        if !compile_body {
            unknown.then({
                let generic = generic.clone();
                move |_: Ref<MType>| -> Result<CRef<()>> {
                    Err(CompileError::internal(
                        generic.location().clone(),
                        format!("generic {} should not be known", generic).as_str(),
                    ))
                }
            })?;
        } else if matches!(context, FnContext::Call) {
            unknowns.insert(generic.get().clone(), unknown);
        }
    }

    let mut compiled_args = Vec::new();

    let process_arg = |arg: &ast::FnArg| {
        if schema.read()?.expr_decls.get(&arg.name).is_some() {
            return Err(CompileError::duplicate_entry(vec![arg.name.clone()]));
        }

        let (value, type_) = match precompiled_args.get(arg.name.get()) {
            Some(value) => (
                STypedExpr {
                    type_: SType::new_mono(value.type_.clone()),
                    expr: mkcref(value.expr.as_ref().clone()),
                },
                value.type_.clone(),
            ),
            None => {
                let mut type_ = resolve_type(compiler.clone(), schema.clone(), &arg.type_)?;
                if compile_body {
                    type_ = type_.substitute(&unknowns)?;
                }

                let stype = SType::new_mono(type_.clone());
                (
                    STypedExpr {
                        type_: stype.clone(),
                        expr: mkcref(Expr::ContextRef(arg.name.get().clone())),
                    },
                    type_,
                )
            }
        };

        let stype = value.type_.clone();
        schema.write()?.expr_decls.insert(
            arg.name.get().clone(),
            Located::new(
                Decl {
                    public: false,
                    extern_: true,
                    is_arg: true,
                    name: arg.name.clone(),
                    value,
                },
                loc.clone(),
            ),
        );
        compiler.run_on_symbol::<ExprEntry>(
            arg.name.clone(),
            SymbolKind::Argument,
            stype,
            arg.name.location().clone(),
            None,
        )?;
        schema
            .write()?
            .externs
            .insert(arg.name.get().clone(), type_.clone());
        Ok(type_)
    };

    for arg in &def.args {
        let type_ = process_arg(arg)?;
        compiled_args.push(MField::new_nullable(arg.name.get().clone(), type_.clone()));
    }

    let variadic_arg = match &def.variadic_arg {
        Some(arg) => {
            let type_ = process_arg(arg)?;
            Some(MField::new_nullable(arg.name.get().clone(), type_.clone()))
        }
        None => None,
    };

    let mut ret_type = if let Some(ret) = &def.ret {
        resolve_type(compiler.clone(), schema.clone(), ret)?
    } else if def.generics.is_empty() {
        MType::new_unknown("return")
    } else {
        mkcref(MType::Name(generic_return_type_name()))
    };
    if compile_body {
        ret_type = ret_type.substitute(&unknowns)?;
    }

    let expr = if compile_body {
        let (compiled, is_sql) = match &def.body {
            ast::FnBody::Native => {
                if !compiler.allow_native()? {
                    return Err(CompileError::internal(
                        loc.clone(),
                        "Cannot compile native functions",
                    ));
                }

                (
                    CTypedExpr {
                        type_: MType::new_unknown(&format!("__native('{}')", def.name)),
                        expr: mkcref(Expr::NativeFn(def.name.get().clone())),
                    },
                    false,
                )
            }
            ast::FnBody::SQL => (
                CTypedExpr {
                    type_: MType::new_unknown(&format!("__sql('{}')", def.name)),
                    expr: mkcref(Expr::Unknown),
                },
                true,
            ),
            ast::FnBody::Expr(expr) => {
                (compile_expr(compiler.clone(), schema.clone(), expr)?, false)
            }
        };

        ret_type.unify(&compiled.type_)?;
        compiled.expr.then(move |expr: Ref<Expr<CRef<MType>>>| {
            let expr = expr.read()?;
            Ok(mkcref(match &*expr {
                Expr::NativeFn(..) => expr.clone(),
                _ => Expr::Fn(FnExpr {
                    inner_schema: schema.clone(),
                    body: if is_sql {
                        FnBody::SQLBuiltin(def.name.get().clone())
                    } else {
                        FnBody::Expr(Arc::new(expr.clone()))
                    },
                }),
            }))
        })?
    } else {
        mkcref(Expr::UncompiledFn(def.clone()))
    };
    let fn_type = MFnType {
        args: compiled_args,
        ret: ret_type,
        variadic_arg,
    };

    Ok((
        CTypedExpr {
            type_: mkcref(MType::Fn(Located::new(fn_type, loc))),
            expr,
        },
        def.generics.clone(),
    ))
}

fn compile_schema_entry(compiler: &Compiler, schema: &Ref<Schema>, stmt: &ast::Stmt) -> Result<()> {
    let loc = SourceLocation::Range(
        schema.read()?.file.clone(),
        Range {
            start: stmt.start.clone(),
            end: stmt.end.clone(),
        },
    );
    match &stmt.body {
        ast::StmtBody::Noop | ast::StmtBody::Unparsed => {}
        ast::StmtBody::Expr(expr) => {
            let compiled = compile_expr(compiler.clone(), schema.clone(), expr)?;
            schema.write()?.exprs.push(Located::new(compiled, loc));
        }
        ast::StmtBody::Import { .. } => {}
        ast::StmtBody::TypeDef(nt) => {
            let type_ = resolve_type(compiler.clone(), schema.clone(), &nt.def)?;
            unify_type_decl(compiler.clone(), schema.clone(), &nt.name, type_)?;
        }
        ast::StmtBody::FnDef(def) => {
            let (compiled_fn, generics) = compile_fn_body(
                compiler.clone(),
                schema.clone(),
                loc.clone(),
                def,
                BTreeMap::new(),
                FnContext::Decl,
            )?;

            unify_expr_decl(
                compiler.clone(),
                schema.clone(),
                &def.name,
                &STypedExpr {
                    type_: SType::new_poly(
                        compiled_fn.type_,
                        BTreeSet::from_iter(generics.to_idents().into_iter()),
                    ),
                    expr: compiled_fn.expr,
                },
            )?;
        }
        ast::StmtBody::Let {
            name,
            type_,
            body,
            materialize,
        } => {
            let lhs_type = if let Some(t) = type_ {
                resolve_type(compiler.clone(), schema.clone(), &t)?
            } else {
                MType::new_unknown(format!("typeof {}", name).as_str())
            };
            let mut compiled = compile_expr(compiler.clone(), schema.clone(), &body)?;

            compiled.expr = match materialize {
                None => compiled.expr.clone(),
                Some(args) => compile_materialized_expr(
                    compiler.clone(),
                    schema.clone(),
                    loc.clone(),
                    name.get().clone(),
                    compiled.clone(),
                    args.clone(),
                )?,
            };

            lhs_type.unify(&compiled.type_)?;
            unify_expr_decl(
                compiler.clone(),
                schema.clone(),
                name,
                &STypedExpr {
                    type_: SType::new_mono(lhs_type),
                    expr: compiled.expr,
                },
            )?;
        }
        ast::StmtBody::Extern { name, type_ } => {
            unify_expr_decl(
                compiler.clone(),
                schema.clone(),
                name,
                &STypedExpr {
                    type_: SType::new_mono(resolve_type(compiler.clone(), schema.clone(), type_)?),
                    expr: mkcref(Expr::Unknown),
                },
            )?;
        }
    };

    Ok(())
}

fn cref_inline_params(
    compiler: Compiler,
    expr: CRef<Expr<CRef<MType>>>,
) -> Result<CRef<Expr<CRef<MType>>>> {
    compiler.async_cref(casync!({
        // This inline pass will compress together SQL that can be pushed down to an underlying
        // remote database by comparing the URLs of subtrees and pushing down the SQL for any
        // subtree whose URLs are all the same or empty and the parent's is empty.
        let expr = expr.await?;

        // TODO The only reason this needs to be async is because of unwrap_schema_entry().
        // If we remove that, then we should not need to
        let expr = expr.read()?.clone();
        Ok(mkcref(inline_params(&expr).await?))
    }))
}

fn optimize_schema(compiler: Compiler, schema: Ref<Schema>) -> CompileResult<()> {
    let mut result = CompileResult::new(());

    let mut s = c_try!(result, schema.write());
    for expr in s.expr_decls.values_mut() {
        let expr_value = expr.value.expr.clone();
        expr.get_mut().value.expr =
            c_try!(result, cref_inline_params(compiler.clone(), expr_value));
    }

    for expr in s.exprs.iter_mut() {
        let expr_value = expr.expr.clone();
        expr.get_mut().expr = c_try!(result, cref_inline_params(compiler.clone(), expr_value));
    }

    result
}

pub fn gather_schema_externs(schema: Ref<Schema>) -> Result<()> {
    let s = schema.read()?;
    for (name, decl) in &s.expr_decls {
        if decl.extern_ {
            let e = &decl.value;

            schema.write()?.externs.insert(
                name.clone(),
                e.type_.then(|t: Ref<SType>| Ok(t.read()?.instantiate()?))?,
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{atomic::AtomicUsize, Arc, Mutex};

    #[test]
    fn test_park_no_op() {
        // This test verifies that if there's no work to be done, then the thread will be parked and the
        // receive signal will fire, so it's safe to keep checking changed() and not risk blocking indefinitely.
        let (idle_tx, mut idle_rx) = tokio::sync::watch::channel(());
        let on_park = move || {
            idle_tx.send(()).ok();
        };

        let drive_counter = Arc::new(AtomicUsize::new(0));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .thread_name("QueryScript Compiler")
            .thread_stack_size(3 * 1024 * 1024)
            .on_thread_park(on_park)
            .build()
            .unwrap();

        runtime.block_on({
            let drive_counter = drive_counter.clone();
            async move {
                for _ in 0..10 {
                    idle_rx.changed().await.unwrap();
                    drive_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    assert!(!idle_rx.has_changed().unwrap());
                }
            }
        });

        assert_eq!(drive_counter.load(std::sync::atomic::Ordering::SeqCst), 10);
    }

    struct SchemaRecorder {
        pub calls: Arc<Mutex<u32>>,
    }

    impl OnSchema for SchemaRecorder {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
        fn on_schema(
            &mut self,
            _path: Option<&FilePath>,
            _ast: Option<&ast::Schema>,
            _schema: Option<Ref<Schema>>,
            _errors: &Vec<(Option<usize>, CompileError)>,
        ) -> Result<()> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            Ok(())
        }
    }

    #[test]
    fn test_on_schema() {
        // Verify that compile_schema_from_file() and compile_schema_ast() both call on_schema
        let compiler = Compiler::new().unwrap();

        let calls = Arc::new(Mutex::new(0));

        compiler
            .on_schema(Some(Box::new(SchemaRecorder {
                calls: calls.clone(),
            })))
            .unwrap();

        assert_eq!(*calls.lock().unwrap(), 0);
        let result = compiler.compile_schema_from_file(FilePath::new("/dev/thisdoesnotexist"));
        assert!(result.as_result().is_err());
        assert_eq!(*calls.lock().unwrap(), 1);

        let schema = Schema::new("nofolder".to_string(), None);
        let result = compiler.compile_string(schema.clone(), "SELECT 1;");
        assert!(result.as_result().is_ok());
        assert_eq!(*calls.lock().unwrap(), 2);
    }
}
