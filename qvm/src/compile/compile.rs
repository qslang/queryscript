use std::collections::{BTreeMap, BTreeSet, LinkedList};
use std::fs;
use std::path::Path as FilePath;
use std::sync::Arc;

use crate::ast;
use crate::ast::{SourceLocation, ToStrings};
use crate::compile::builtin_types::{BUILTIN_LOC, GLOBAL_SCHEMA};
use crate::compile::coerce::CoerceOp;
use crate::compile::error::*;
use crate::compile::inference::*;
use crate::compile::schema::*;
use crate::compile::sql::*;
use crate::{parser, parser::parse_schema};

#[derive(Debug)]
pub struct CompileResult<V> {
    pub result: V,
    pub errors: Vec<(Option<usize>, CompileError)>,
}

impl<V> CompileResult<V> {
    pub fn new(result: V) -> CompileResult<V> {
        CompileResult {
            result,
            errors: Vec::new(),
        }
    }

    pub fn map<U, F>(self, f: F) -> CompileResult<U>
    where
        F: FnOnce(V) -> U,
    {
        CompileResult {
            result: f(self.result),
            errors: self.errors,
        }
    }

    pub fn add_error(&mut self, idx: Option<usize>, error: CompileError) {
        match error {
            CompileError::Multiple { sources } => {
                for source in sources {
                    self.add_error(idx, source);
                }
            }
            _ => self.errors.push((idx, error)),
        }
    }

    pub fn absorb<U>(&mut self, other: CompileResult<U>) -> U {
        self.errors.extend(other.errors);
        other.result
    }

    pub fn replace(&mut self, other: CompileResult<V>) {
        self.result = other.result;
        self.errors.extend(other.errors);
    }

    pub fn expect(self, debug: &str) -> V {
        if self.errors.len() == 0 {
            self.result
        } else {
            panic!("{:?}: {:?}", debug, self.errors);
        }
    }

    pub fn as_result(mut self) -> Result<V> {
        match self.errors.len() {
            0 => Ok(self.result),
            _ => Err(CompileError::Multiple {
                sources: self.errors.drain(..).map(|e| e.1).collect(),
            }),
        }
    }
}

macro_rules! c_try {
    ($result: expr, $expr: expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                $result.add_error(None, e.into());
                return $result;
            }
        }
    };
}

#[derive(Debug)]
pub struct CompilerData {
    pub next_placeholder: usize,
    pub idle: Ref<tokio::sync::watch::Receiver<()>>,
    pub handles: LinkedList<tokio::task::JoinHandle<Result<()>>>,
    pub files: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CompilerConfig {
    pub allow_native: bool,
    pub allow_inlining: bool,
}

impl Default for CompilerConfig {
    fn default() -> CompilerConfig {
        CompilerConfig {
            allow_native: false,
            allow_inlining: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Compiler {
    runtime: Ref<tokio::runtime::Runtime>,
    data: Ref<CompilerData>,
    builtins: Ref<Schema>,
    config: CompilerConfig,
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
                    .thread_name("qvm-compiler")
                    .thread_stack_size(3 * 1024 * 1024)
                    .on_thread_park(on_park)
                    .build()?,
            ),
            data: mkref(CompilerData {
                next_placeholder: 1,
                idle: mkref(idle_rx),
                handles: LinkedList::new(),
                files: BTreeMap::new(),
            }),
            builtins: schema.clone(),
            config,
        };

        Ok(compiler)
    }

    pub fn builtins(&self) -> Ref<Schema> {
        self.builtins.clone()
    }

    pub fn allow_native(&self) -> bool {
        self.config.allow_native
    }

    pub fn allow_inlining(&self) -> bool {
        self.config.allow_inlining
    }

    pub fn compile_string(&self, schema: Ref<Schema>, text: &str) -> CompileResult<()> {
        let mut result = CompileResult::new(());
        let (tokens, eof) = c_try!(result, parser::tokenize("<string>", text));
        let mut parser = parser::Parser::new("<string>", tokens, eof);
        let schema_ast = c_try!(result, parser.parse_schema());
        result.absorb(self.compile_schema_ast(schema.clone(), &schema_ast));
        result
    }

    pub fn compile_schema_ast(&self, schema: Ref<Schema>, ast: &ast::Schema) -> CompileResult<()> {
        let mut result = CompileResult::new(());
        let runtime = c_try!(result, self.runtime.read());
        runtime.block_on(async move {
            result.replace(compile_schema_ast(self.clone(), schema.clone(), ast));
            result.absorb(self.drive().await);
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
            result.replace(compile_schema_from_file(self.clone(), file_path));
            result.absorb(self.drive().await);
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

            c_try!(result, idle.changed().await);

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
        data.handles
            .push_back(self.runtime.read()?.spawn(async move {
                let r = f.await?;
                slot.unify(&r)
            }));

        Ok(ret)
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
            parse_schema(parsed_path.to_str().unwrap(), contents)?,
        ))
    }

    pub fn set_file_contents(&self, file: String, contents: String) -> Result<()> {
        self.data.write()?.files.insert(file, contents);
        Ok(())
    }

    pub fn file_contents(&self) -> Result<std::sync::RwLockReadGuard<'_, CompilerData>> {
        Ok(self.data.read()?)
    }
}

pub fn lookup_schema(
    compiler: Compiler,
    schema: Ref<Schema>,
    path: &ast::Path,
) -> Result<Ref<ImportedSchema>> {
    if let Some(s) = schema.read()?.imports.get(&path.to_strings()) {
        return Ok(s.clone());
    }

    let (k, v) = if let Some(root) = &schema.read()?.folder {
        let mut file_path_buf = FilePath::new(root).to_path_buf();
        for p in path {
            file_path_buf.push(FilePath::new(&p.value));
        }
        // XXX support .tql
        for extension in SCHEMA_EXTENSIONS.iter() {
            file_path_buf.set_extension(extension);
            if file_path_buf.as_path().exists() {
                break;
            }
        }
        let file_path = file_path_buf.as_path();

        let s = compile_schema_from_file(compiler.clone(), file_path)
            .as_result()?
            .unwrap();
        (path.clone(), s.clone())
    } else {
        return Err(CompileError::no_such_entry(path.clone()));
    };

    let imported = mkref(ImportedSchema {
        args: if v.read()?.externs.len() == 0 {
            None
        } else {
            Some(Vec::new())
        },
        schema: v.clone(),
    });

    schema
        .write()?
        .imports
        .insert(k.to_strings(), imported.clone());

    return Ok(imported);
}

pub fn lookup_path(
    compiler: Compiler,
    schema: Ref<Schema>,
    path: &ast::Path,
    import_global: bool,
    resolve_last: bool,
) -> Result<(Ref<Schema>, Option<Decl>, ast::Path)> {
    if path.len() == 0 {
        return Ok((schema, None, path.clone()));
    }

    let mut schema = schema;
    for (i, ident) in path.iter().enumerate() {
        let new = match schema.read()?.decls.get(&ident.value) {
            Some(decl) => {
                if i > 0 && !decl.public {
                    return Err(CompileError::wrong_kind(path.clone(), "public", decl));
                }

                if i == path.len() - 1 && !resolve_last {
                    return Ok((schema.clone(), Some(decl.get().clone()), vec![]));
                }

                match &decl.value {
                    SchemaEntry::Schema(imported) => {
                        lookup_schema(compiler.clone(), schema.clone(), &imported)?
                            .read()?
                            .schema
                            .clone()
                    }
                    _ => {
                        return Ok((
                            schema.clone(),
                            Some(decl.get().clone()),
                            path[i + 1..].to_vec(),
                        ))
                    }
                }
            }
            None => match &schema.read()?.parent_scope {
                Some(parent) => {
                    return lookup_path(
                        compiler.clone(),
                        parent.clone(),
                        &path[i..].to_vec(),
                        import_global,
                        resolve_last,
                    )
                }
                None => {
                    if import_global {
                        return lookup_path(
                            compiler.clone(),
                            compiler.builtins(),
                            &path[i..].to_vec(),
                            false, /* import_global */
                            resolve_last,
                        );
                    } else {
                        return Ok((schema.clone(), None, path[i..].to_vec()));
                    }
                }
            },
        };

        schema = new;
    }

    return Ok((schema.clone(), None, Vec::new()));
}

pub fn resolve_type(
    compiler: Compiler,
    schema: Ref<Schema>,
    ast: &ast::Type,
) -> Result<CRef<MType>> {
    let loc = SourceLocation::Range(
        schema.read()?.file.clone(),
        ast.start.clone(),
        ast.end.clone(),
    );
    match &ast.body {
        ast::TypeBody::Reference(path) => {
            let (_, decl, r) = lookup_path(
                compiler.clone(),
                schema.clone(),
                &path,
                true, /* import_global */
                true, /* resolve_last */
            )?;
            if r.len() > 0 {
                return Err(CompileError::no_such_entry(r));
            }
            let decl = decl.ok_or_else(|| CompileError::no_such_entry(r))?;
            let t = match decl.value {
                SchemaEntry::Type(t) => t,
                _ => return Err(CompileError::wrong_kind(path.clone(), "type", &decl)),
            };

            Ok(t)
        }
        ast::TypeBody::Struct(entries) => {
            let mut fields = Vec::new();
            let mut seen = BTreeSet::new();
            for e in entries {
                match e {
                    ast::StructEntry::NameAndType(nt) => {
                        if seen.contains(&nt.name.value) {
                            return Err(CompileError::duplicate_entry(vec![nt.name.clone()]));
                        }
                        seen.insert(nt.name.value.clone());
                        fields.push(MField {
                            name: nt.name.clone(),
                            type_: resolve_type(compiler.clone(), schema.clone(), &nt.def)?,
                            nullable: true, /* TODO: implement non-null types */
                        });
                    }
                    ast::StructEntry::Include { .. } => {
                        return Err(CompileError::unimplemented(loc, "Struct inclusions"));
                    }
                }
            }

            Ok(mkcref(MType::Record(MRecordType { loc, fields })))
        }
        ast::TypeBody::List(inner) => Ok(mkcref(MType::List(MListType {
            loc,
            inner: resolve_type(compiler, schema, inner.as_ref())?,
        }))),
        ast::TypeBody::Exclude { .. } => {
            return Err(CompileError::unimplemented(loc, "Struct exclusions"));
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

pub fn find_field<'a>(fields: &'a Vec<MField>, name: &str) -> Option<&'a MField> {
    for f in fields.iter() {
        if f.name.value == name {
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

pub fn typecheck_path(type_: CRef<MType>, path: &[Ident]) -> Result<CRef<MType>> {
    if path.len() == 0 {
        return Ok(type_);
    }

    let name = path[0].clone();
    let remainder = path[1..].to_vec();

    type_.then(move |type_: Ref<MType>| match &*type_.read()? {
        MType::Record(MRecordType { loc, fields }) => {
            if let Some(field) = find_field(&fields, name.value.as_str()) {
                typecheck_path(field.type_.clone(), remainder.as_slice())
            } else {
                return Err(CompileError::wrong_type(
                    &MType::Record(MRecordType {
                        loc: loc.clone(),
                        fields: vec![MField::new_nullable(
                            name.clone(),
                            MType::new_unknown("field"),
                        )],
                    }),
                    &*type_.read()?,
                ));
            }
        }
        t => {
            return Err(CompileError::wrong_type(
                &MType::Record(MRecordType {
                    loc: t.location(),
                    fields: vec![MField::new_nullable(
                        name.clone(),
                        MType::new_unknown("field"),
                    )],
                }),
                &*type_.read()?,
            ))
        }
    })
}

pub fn compile_expr(
    compiler: Compiler,
    schema: Ref<Schema>,
    expr: &ast::Expr,
) -> Result<CTypedExpr> {
    let loc = SourceLocation::Range(
        schema.read()?.file.clone(),
        expr.start.clone(),
        expr.end.clone(),
    );
    match &expr.body {
        ast::ExprBody::SQLQuery(q) => {
            Ok(compile_sqlquery(compiler.clone(), schema.clone(), &loc, q)?)
        }
        ast::ExprBody::SQLExpr(e) => {
            let scope = mkref(SQLScope::new(None));
            Ok(compile_sqlexpr(
                compiler.clone(),
                schema.clone(),
                scope,
                &loc,
                e,
            )?)
        }
    }
}

pub fn rebind_decl(_schema: SchemaInstance, decl: &Decl) -> Result<SchemaEntry> {
    match &decl.value {
        SchemaEntry::Schema(s) => Ok(SchemaEntry::Schema(s.clone())),
        SchemaEntry::Type(t) => Ok(SchemaEntry::Type(t.clone())),
        SchemaEntry::Expr(e) => Ok(SchemaEntry::Expr(e.clone())),
    }
}

fn compile_schema_from_file(
    compiler: Compiler,
    file_path: &FilePath,
) -> CompileResult<Option<Ref<Schema>>> {
    let mut result = CompileResult::new(None);
    let (file, folder, ast) = match compiler.open_file(file_path) {
        Ok((file, folder, contents)) => (file, folder, contents),
        Err(e) => {
            result.add_error(None, e);
            return result;
        }
    };

    result.replace(compile_schema(compiler.clone(), file, folder, &ast).map(|s| Some(s)));
    result
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
    match gather_schema_externs(schema) {
        Ok(_) => {}
        Err(e) => result.add_error(None, e),
    };

    result
}

pub fn declare_schema_entries(
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

pub fn declare_schema_entry(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    stmt: &ast::Stmt,
) -> Result<()> {
    let loc = SourceLocation::Range(
        schema.read()?.file.clone(),
        stmt.start.clone(),
        stmt.end.clone(),
    );
    let entries: Vec<(Ident, bool, SchemaEntry)> = match &stmt.body {
        ast::StmtBody::Noop => Vec::new(),
        ast::StmtBody::Expr(_) => Vec::new(),
        ast::StmtBody::Import { path, list, .. } => {
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

            let mut imports = Vec::new();
            match list {
                ast::ImportList::None => {
                    imports.push((
                        path.last().unwrap().clone(),
                        false, /* extern_ */
                        SchemaEntry::Schema(path.clone()),
                    ));
                }
                ast::ImportList::Star => {
                    for (_, v) in imported
                        .read()?
                        .schema
                        .read()?
                        .decls
                        .iter()
                        .filter(|(_, v)| v.public)
                    {
                        let imported_schema = SchemaInstance {
                            schema: imported.read()?.schema.clone(),
                            id: None,
                        };
                        imports.push((
                            v.name.clone(),
                            false, /* extern_ */
                            rebind_decl(imported_schema, &v)?,
                        ));
                    }
                }
                ast::ImportList::Items(items) => {
                    for item in items {
                        let loc = path_location(item);
                        if item.len() != 1 {
                            return Err(CompileError::unimplemented(loc, "path imports"));
                        }

                        let (_, decl, r) = lookup_path(
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

                        let imported_schema = SchemaInstance {
                            schema: schema.clone(),
                            id: None,
                        };

                        imports.push((
                            item[0].clone(),
                            false, /* extern_ */
                            rebind_decl(imported_schema, &decl)?,
                        ));
                    }
                }
            }

            imports
        }
        ast::StmtBody::TypeDef(nt) => vec![(
            nt.name.clone(),
            false, /* extern_ */
            SchemaEntry::Type(MType::new_unknown(nt.name.value.as_str())),
        )],
        ast::StmtBody::FnDef { name, .. } => {
            vec![(
                name.clone(),
                false, /* extern_ */
                SchemaEntry::Expr(STypedExpr::new_unknown(name.value.as_str())),
            )]
        }
        ast::StmtBody::Let { name, .. } => {
            vec![(
                name.clone(),
                false, /* extern_ */
                SchemaEntry::Expr(STypedExpr::new_unknown(name.value.as_str())),
            )]
        }
        ast::StmtBody::Extern { name, .. } => vec![(
            name.clone(),
            true, /* extern_ */
            SchemaEntry::Expr(STypedExpr::new_unknown(name.value.as_str())),
        )],
    };

    for (name, extern_, value) in &entries {
        if schema.read()?.decls.contains_key(&name.value) {
            return Err(CompileError::duplicate_entry(vec![name.clone()]));
        }

        schema.write()?.decls.insert(
            name.value.clone(),
            Located::new(
                Decl {
                    public: stmt.export,
                    extern_: *extern_,
                    name: name.clone(),
                    value: value.clone(),
                },
                loc.clone(),
            ),
        );
    }
    Ok(())
}

pub fn unify_type_decl(schema: Ref<Schema>, name: &Ident, type_: CRef<MType>) -> Result<()> {
    let s = schema.read()?;
    let decl = s.decls.get(&name.value).ok_or_else(|| {
        CompileError::internal(
            name.loc.clone(),
            format!(
                "Could not find type declaration {} during reprocessing",
                name.value
            )
            .as_str(),
        )
    })?;
    match &decl.value {
        SchemaEntry::Type(t) => t.unify(&type_)?,
        _ => {
            return Err(CompileError::internal(
                name.loc.clone(),
                format!(
                    "Expected {} to be a type declaration during reprocessing",
                    name.value
                )
                .as_str(),
            ))
        }
    }

    Ok(())
}

pub fn unify_expr_decl(schema: Ref<Schema>, name: &Ident, value: &STypedExpr) -> Result<()> {
    let s = schema.read()?;
    let decl = s.decls.get(&name.value).ok_or_else(|| {
        CompileError::internal(
            name.loc.clone(),
            format!(
                "Could not find type declaration {} during reprocessing",
                name.value
            )
            .as_str(),
        )
    })?;
    match &decl.value {
        SchemaEntry::Expr(e) => e.unify(&value)?,
        _ => {
            return Err(CompileError::internal(
                name.loc.clone(),
                format!(
                    "Expected {} to be a type declaration during reprocessing",
                    name.value
                )
                .as_str(),
            ))
        }
    }

    Ok(())
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

pub fn compile_schema_entry(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    stmt: &ast::Stmt,
) -> Result<()> {
    let loc = SourceLocation::Range(
        schema.read()?.file.clone(),
        stmt.start.clone(),
        stmt.end.clone(),
    );
    match &stmt.body {
        ast::StmtBody::Noop => {}
        ast::StmtBody::Expr(expr) => {
            let compiled = compile_expr(compiler.clone(), schema.clone(), expr)?;
            schema.write()?.exprs.push(Located::new(compiled, loc));
        }
        ast::StmtBody::Import { .. } => {}
        ast::StmtBody::TypeDef(nt) => {
            let type_ = resolve_type(compiler.clone(), schema.clone(), &nt.def)?;
            unify_type_decl(schema.clone(), &nt.name, type_)?;
        }
        ast::StmtBody::FnDef {
            name,
            generics,
            args,
            ret,
            body,
        } => {
            let inner_schema =
                Schema::new(schema.read()?.file.clone(), schema.read()?.folder.clone());
            inner_schema.write()?.parent_scope = Some(schema.clone());

            for generic in generics {
                inner_schema.write()?.decls.insert(
                    generic.value.clone(),
                    Located::new(
                        Decl {
                            public: true,
                            extern_: true,
                            name: generic.clone(),
                            value: SchemaEntry::Type(mkcref(MType::Name(generic.clone()))),
                        },
                        loc.clone(),
                    ),
                );
            }

            let mut compiled_args = Vec::new();
            for arg in args {
                if inner_schema.read()?.decls.get(&arg.name.value).is_some() {
                    return Err(CompileError::duplicate_entry(vec![name.clone()]));
                }
                let type_ = resolve_type(compiler.clone(), inner_schema.clone(), &arg.type_)?;
                inner_schema.write()?.decls.insert(
                    arg.name.value.clone(),
                    Located::new(
                        Decl {
                            public: true,
                            extern_: true,
                            name: arg.name.clone(),
                            value: SchemaEntry::Expr(STypedExpr {
                                type_: SType::new_mono(type_.clone()),
                                expr: mkcref(Expr::ContextRef(arg.name.value.clone())),
                            }),
                        },
                        loc.clone(),
                    ),
                );
                inner_schema
                    .write()?
                    .externs
                    .insert(arg.name.value.clone(), type_.clone());
                compiled_args.push(MField::new_nullable(arg.name.clone(), type_.clone()));
            }

            let (compiled, is_sql) = match body {
                ast::FnBody::Native => {
                    if !compiler.allow_native() {
                        return Err(CompileError::internal(
                            loc.clone(),
                            "Cannot compile native functions",
                        ));
                    }

                    (
                        CTypedExpr {
                            type_: MType::new_unknown(&format!("__native('{}')", name.value)),
                            expr: mkcref(Expr::NativeFn(name.value.to_string())),
                        },
                        false,
                    )
                }
                ast::FnBody::SQL => (
                    CTypedExpr {
                        type_: MType::new_unknown(&format!("__sql('{}')", name.value)),
                        expr: mkcref(Expr::Unknown),
                    },
                    true,
                ),
                ast::FnBody::Expr(expr) => (
                    compile_expr(compiler.clone(), inner_schema.clone(), expr)?,
                    false,
                ),
            };

            if let Some(ret) = ret {
                resolve_type(compiler.clone(), inner_schema.clone(), ret)?.unify(&compiled.type_)?
            }

            let fn_type = SType::new_poly(
                mkcref(MType::Fn(MFnType {
                    loc: loc.clone(),
                    args: compiled_args,
                    ret: compiled.type_.clone(),
                })),
                BTreeSet::from_iter(generics.to_strings().into_iter()),
            );

            unify_expr_decl(
                schema.clone(),
                name,
                &STypedExpr {
                    type_: fn_type,
                    expr: compiled.expr.then(move |expr: Ref<Expr<CRef<MType>>>| {
                        let expr = expr.read()?;
                        Ok(mkcref(match &*expr {
                            Expr::NativeFn(..) => expr.clone(),
                            _ => Expr::Fn(FnExpr {
                                inner_schema: inner_schema.clone(),
                                body: if is_sql {
                                    FnBody::SQLBuiltin
                                } else {
                                    FnBody::Expr(Arc::new(expr.clone()))
                                },
                            }),
                        }))
                    })?,
                },
            )?;
        }
        ast::StmtBody::Let { name, type_, body } => {
            let lhs_type = if let Some(t) = type_ {
                resolve_type(compiler.clone(), schema.clone(), &t)?
            } else {
                MType::new_unknown(format!("typeof {}", name.value).as_str())
            };
            let compiled = compile_expr(compiler.clone(), schema.clone(), &body)?;
            lhs_type.unify(&compiled.type_)?;
            unify_expr_decl(
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

pub fn gather_schema_externs(schema: Ref<Schema>) -> Result<()> {
    let s = schema.read()?;
    for (name, decl) in &s.decls {
        if decl.extern_ {
            match &decl.value {
                SchemaEntry::Expr(e) => {
                    schema.write()?.externs.insert(
                        name.clone(),
                        e.type_.then(|t: Ref<SType>| Ok(t.read()?.instantiate()?))?,
                    );
                }
                _ => {
                    return Err(CompileError::unimplemented(
                        SourceLocation::Unknown,
                        "type externs",
                    ))
                }
            }
        }
    }

    Ok(())
}

pub fn coerce<T: Constrainable + 'static>(
    compiler: Compiler,
    op: CoerceOp,
    left: CRef<T>,
    right: CRef<T>,
) -> Result<CRef<T>> {
    compiler.async_cref(async move {
        let left = left.await?;
        let right = right.await?;

        Constrainable::coerce(&op, &left, &right)
    })
}
